# app/masterlist.py
"""
Masterlist builder + Parent grouping (exact normalized-name grouping)

Behavior:
- Build active masterlist using mftool (parallel, cached)
- Parent grouping: strip plan/option tokens (Direct/Regular/Growth/IDCW etc.)
  and group by exact normalized base name (no fuzzy merging)
- Choose representative per parent (prefer Direct+Growth, Direct, Regular+Growth, highest AUM)
- Persist masterlist, per-code caches, and parent masterlist JSON.
"""

import json
import os
import time
import traceback
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import pandas as pd
from mftool import Mftool

# -------- CONFIG & PATHS --------
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "..", "data")
MASTER_CACHE_FILE = os.path.join(CACHE_DIR, "masterlist.json")
DETAILS_CACHE_FILE = os.path.join(CACHE_DIR, "details_cache.json")
QUOTE_CACHE_FILE = os.path.join(CACHE_DIR, "quote_cache.json")
PARENT_MASTER_FILE = os.path.join(CACHE_DIR, "parent_masterlist.json")

CACHE_TTL_SECONDS = 60 * 60 * 24  # 24 hours
NAV_FRESH_DAYS = 30               # NAV freshness window (days)

# Performance tuning
MAX_WORKERS = 20
CHECKPOINT_EVERY = 200

# In-memory caches
_masterlist_cache: Optional[Dict[str, str]] = None
_masterlist_meta = {"ts": 0.0}

UTC = timezone.utc


# ------------------ Utilities ------------------
def _ensure_data_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json(path: str):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _safe_save_json(path: str, obj):
    """Write a shallow copy to avoid concurrent modification errors."""
    try:
        _ensure_data_dir()
        try:
            safe = dict(obj)
        except Exception:
            safe = obj
        with open(path, "w", encoding="utf-8") as f:
            json.dump(safe, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[masterlist] failed to save {path}: {e}")


def _normalize(name: str) -> str:
    return " ".join(str(name).lower().strip().split()) if name else ""


def _parse_date(val):
    """
    Parse date string with pandas and return timezone-aware datetime (UTC) or None.
    """
    if not val:
        return None
    try:
        dt = pd.to_datetime(str(val), errors="coerce")
        if pd.isna(dt):
            return None
        py = dt.to_pydatetime()
        if py.tzinfo is None:
            return py.replace(tzinfo=UTC)
        return py.astimezone(UTC)
    except Exception:
        return None


def _is_valid_nav(nav):
    if not nav:
        return False
    try:
        float(str(nav).replace(",", ""))
        return True
    except Exception:
        return False


def _is_nav_recent(last_upd):
    dt = _parse_date(last_upd)
    if not dt:
        return False
    return (datetime.now(UTC) - dt) <= timedelta(days=NAV_FRESH_DAYS)


def _is_closed_scheme(scheme_type: str) -> bool:
    if not scheme_type:
        return False
    s = str(scheme_type).lower()
    closed_keywords = [
        "closed ended", "close ended", "close-end", "closed-end", "closed", "maturity"
    ]
    return any(k in s for k in closed_keywords)


def _is_open_or_interval(scheme_type: str) -> bool:
    if not scheme_type:
        return False
    st = str(scheme_type).lower()
    good_keywords = [
        "open",
        "open ended",
        "open-ended",
        "etf",
        "exchange traded",
        "index",
        "liquid",
        "equity",
        "debt",
        "hybrid",
        "interval",
        "fund of funds",
    ]
    return any(k in st for k in good_keywords)


# ---------- per-code checker (thread worker) ----------
def _check_code_active(mf: Mftool, code: str, details_cache: Dict, quote_cache: Dict) -> Tuple[str, dict, dict, bool]:
    """
    Check activity for a single scheme code.
    Returns (code, details, quote, is_active)
    Updates provided caches (details_cache, quote_cache).
    """
    code = str(code)
    details = details_cache.get(code)
    quote = quote_cache.get(code)

    # fetch details if missing
    if details is None:
        try:
            details = mf.get_scheme_details(code) or {}
        except Exception:
            details = {}
        details_cache[code] = details

    scheme_type = details.get("scheme_type") or details.get("type") or details.get("status") or ""

    # closed-ended: reject
    if _is_closed_scheme(scheme_type):
        return code, details, quote or {}, False

    # fetch quote if missing
    if quote is None:
        try:
            quote = mf.get_scheme_quote(code) or {}
        except Exception:
            quote = {}
        quote_cache[code] = quote

    nav = quote.get("nav") or quote.get("nav_val") or quote.get("nav_value") or details.get("nav")
    last_upd = quote.get("last_updated") or details.get("last_updated") or details.get("nav_date")

    # require numeric nav
    if not _is_valid_nav(nav):
        return code, details, quote, False

    # require fresh nav
    if not _is_nav_recent(last_upd):
        return code, details, quote, False

    # require open-ish
    if scheme_type:
        if not _is_open_or_interval(scheme_type):
            return code, details, quote, False

    return code, details, quote, True


# ------------------ Masterlist build (parallel + caches) ------------------
def build_master_list_cache(force: bool = False, max_workers: int = MAX_WORKERS) -> Dict[str, str]:
    """
    Build masterlist mapping normalized_name -> code for active (investable) schemes.
    Uses parallel per-code checking and persistent per-code caches.
    """
    global _masterlist_cache, _masterlist_meta

    # in-memory cache
    if _masterlist_cache is not None and not force:
        return _masterlist_cache

    # try disk masterlist
    if os.path.exists(MASTER_CACHE_FILE) and not force:
        try:
            with open(MASTER_CACHE_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            ts = payload.get("meta", {}).get("ts", 0)
            if (time.time() - ts) <= CACHE_TTL_SECONDS:
                _masterlist_cache = payload.get("master", {})
                print(f"[masterlist] loaded masterlist from disk ({len(_masterlist_cache)} entries)")
                return _masterlist_cache
        except Exception:
            pass

    # load per-code caches
    details_cache = _load_json(DETAILS_CACHE_FILE)
    quote_cache = _load_json(QUOTE_CACHE_FILE)

    mf = Mftool()
    try:
        codes_map = mf.get_scheme_codes() or {}
    except Exception as e:
        print("[masterlist] ERROR fetching scheme codes:", e)
        traceback.print_exc()
        if _masterlist_cache:
            return _masterlist_cache
        return {}

    total = len(codes_map)
    print(f"[masterlist] total schemes from mftool: {total}")

    master: Dict[str, str] = {}
    skipped_samples = []
    processed = 0
    active_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_code_active, mf, code, details_cache, quote_cache): (code, name) for code, name in codes_map.items()}

        for fut in as_completed(futures):
            code, name = futures[fut]
            try:
                code_ret, details, quote, is_active = fut.result()
            except Exception:
                code_ret, details, quote, is_active = code, {}, {}, False

            processed += 1

            if is_active:
                master[_normalize(name)] = str(code)
                active_count += 1
            else:
                if len(skipped_samples) < 10:
                    skipped_samples.append((code, name))

            if processed % CHECKPOINT_EVERY == 0:
                print(f"[masterlist] processed {processed}/{total}, active={active_count}; checkpointing caches...")
                _safe_save_json(DETAILS_CACHE_FILE, details_cache)
                _safe_save_json(QUOTE_CACHE_FILE, quote_cache)

    print(f"[masterlist] done processing. active_count={active_count}")
    if skipped_samples:
        print("[masterlist] sample skipped (inactive):")
        for code, name in skipped_samples:
            print(" -", code, "-", name)

    # save masterlist & caches
    payload = {"meta": {"ts": time.time()}, "master": master}
    try:
        _ensure_data_dir()
        with open(MASTER_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[masterlist] failed to save masterlist:", e)

    _safe_save_json(DETAILS_CACHE_FILE, details_cache)
    _safe_save_json(QUOTE_CACHE_FILE, quote_cache)

    _masterlist_cache = master
    _masterlist_meta["ts"] = time.time()
    return master


def refresh_masterlist():
    return build_master_list_cache(force=True)


# ============== Parent grouping (exact normalized-name grouping) ==============

# ------------- Normalization utilities (strip plan/option tokens) -------------
def _strip_plan_option_tokens(name: str) -> str:
    """
    Heuristics to remove plan/option tokens from scheme names while preserving
    the parent product identity (e.g., 'Micro Cap' vs 'Long Term Tax Advantage').
    Removes bracketed qualifiers, IDCW/payout phrases, plan/option keywords, etc.
    """
    if not name:
        return ""
    s = str(name)

    # Normalize some unicode bullets/separators into hyphen
    s = __import__("re").sub(r"[\/|•\u2022]+", "-", s)

    # Remove large, verbose IDCW/payout phrases (common long-form)
    s = __import__("re").sub(
        r"\b(payout of income distribution cum capital withdrawal|payout of income distribution|income distribution cum capital withdrawal|payout of income distribution cum capital withdrawal \(idcw\))\b",
        " ",
        s,
        flags=__import__("re").IGNORECASE,
    )

    # Remove bracketed qualifiers e.g. (Direct Plan), (IDCW), [Direct]
    s = __import__("re").sub(r"[\(\[\{][^\)\]\}]*[\)\]\}]", " ", s)

    # Remove explicit 'Plan'/'Option' connected tokens and short descriptors
    tokens_pattern = r"\b(?:direct plan|regular plan|direct|regular|plan|option|growth|idcw|dividend|div|dividend reinvestment|reinvestment|monthly|quarterly|annual|institutional|inst|super institutional|sub-plan|sub plan|retail|monthly idcw|fortnightly idcw|weekly idcw|payout|bonus)\b"
    s = __import__("re").sub(tokens_pattern, " ", s, flags=__import__("re").IGNORECASE)

    # clean hyphens / whitespace / punctuation
    s = __import__("re").sub(r"[-]{2,}", "-", s)
    s = __import__("re").sub(r"\s*-\s*", " - ", s)
    s = __import__("re").sub(r"\s{2,}", " ", s)
    s = __import__("re").sub(r"(^[\s\-\:]+|[\s\-\:]+$)", "", s)
    s = __import__("re").sub(r"[,;:]+", " ", s)

    return s.strip()


def normalize_parent_name(name: str) -> str:
    """
    Normalize the stripped parent name for grouping (lowercase, collapse spaces).
    """
    base = _strip_plan_option_tokens(name)
    base = __import__("re").sub(r"[\.\'\"\/\(\)\[\]\:]+", " ", base)
    base = " ".join(base.split())
    return base.lower().strip()


# ------------- Group plan-level variants into parent products (exact match on normalized name) -------------
def group_variants_exact(code_to_name: Dict[str, str]) -> Dict[str, List[Tuple[str, str]]]:
    """
    Group plan-level codes by exact normalized parent name.
    Input: code_to_name {code: original_name}
    Output: parent_norm -> [(code, original_name), ...]
    """
    groups = defaultdict(list)
    for code, name in code_to_name.items():
        parent_norm = normalize_parent_name(name)
        if not parent_norm:
            parent_norm = str(name).lower().strip()
        groups[parent_norm].append((str(code), name))
    return dict(groups)


# ------------- Representative selection -------------
def choose_representative(entries: List[Tuple[str, str]],
                          mf: Optional[Mftool] = None,
                          quote_cache: Optional[Dict[str, dict]] = None) -> Tuple[Optional[str], Optional[str], str, float]:
    """
    Given list of (code, original_name) variants for a parent product, choose a representative.
    Returns: (rep_code, rep_name, reason, score)
     - reason: textual reason e.g. 'direct_growth', 'highest_aum'
     - score: numeric score to indicate quality (higher better)
    Strategy:
     1) prefer Direct + Growth variants
     2) else prefer Direct (any)
     3) else prefer Regular + Growth
     4) else pick highest AUM (cached first, then live)
     5) fallback to first
    """
    if not entries:
        return None, None, "empty", 0.0

    def has_token(name: str, token: str) -> bool:
        return bool(__import__("re").search(rf"\b{__import__('re').escape(token)}\b", name, flags=__import__("re").IGNORECASE))

    # 1) direct + growth
    for code, name in entries:
        if has_token(name, "direct") and (has_token(name, "growth") or (not has_token(name, "idcw") and not has_token(name, "dividend"))):
            return code, name, "direct_growth", 100.0

    # 2) direct any
    for code, name in entries:
        if has_token(name, "direct"):
            return code, name, "direct", 80.0

    # 3) regular + growth
    for code, name in entries:
        if has_token(name, "regular") and has_token(name, "growth"):
            return code, name, "regular_growth", 60.0

    # 4) pick highest AUM if available (cached then live)
    best = None
    best_aum = -1.0
    if quote_cache:
        for code, name in entries:
            qc = quote_cache.get(str(code)) if isinstance(quote_cache, dict) else None
            if qc and isinstance(qc, dict):
                aum = qc.get("aum") or qc.get("AUM") or qc.get("asset_under_management") or qc.get("assets_under_management")
                try:
                    aum_val = float(str(aum).replace(",", "")) if aum not in (None, "") else 0.0
                except Exception:
                    aum_val = 0.0
                if aum_val > best_aum:
                    best_aum = aum_val
                    best = (code, name)
        if best:
            return best[0], best[1], "highest_aum_cached", float(best_aum)

    if mf is None:
        try:
            mf = Mftool()
        except Exception:
            mf = None

    if mf is not None:
        for code, name in entries:
            try:
                q = mf.get_scheme_quote(str(code)) or {}
            except Exception:
                q = {}
            aum = q.get("aum") or q.get("AUM") or q.get("asset_under_management") or q.get("assets_under_management")
            try:
                aum_val = float(str(aum).replace(",", "")) if aum not in (None, "") else 0.0
            except Exception:
                aum_val = 0.0
            if aum_val > best_aum:
                best_aum = aum_val
                best = (code, name)
        if best:
            return best[0], best[1], "highest_aum_live", float(best_aum)

    # fallback: first entry
    code0, name0 = entries[0]
    return code0, name0, "first_fallback", 10.0


# ------------- Build parent masterlist (exact grouping) -------------
def build_parent_masterlist_from_codes_exact(code_to_name: Dict[str, str],
                                             mf: Optional[Mftool] = None,
                                             quote_cache_path: Optional[str] = QUOTE_CACHE_FILE) -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, Tuple[Optional[str], Optional[str], str, float]]]:
    """
    Given plan-level code_to_name, group by exact normalized parent name and select representative.
    Persists parent masterlist to PARENT_MASTER_FILE.
    Returns:
      - parent_groups: parent_norm -> list[(code, original_name)]
      - parent_reps: parent_norm -> (rep_code, rep_name, reason, score)
    """
    parent_groups = group_variants_exact(code_to_name)
    print(f"[parent_mapper] parent groups (exact normalized): {len(parent_groups)}")

    # prepare quote cache
    quote_cache = {}
    if quote_cache_path and os.path.exists(quote_cache_path):
        try:
            quote_cache = _load_json(quote_cache_path)
        except Exception:
            quote_cache = {}

    if mf is None:
        try:
            mf = Mftool()
        except Exception:
            mf = None

    parent_reps = {}
    for parent, entries in parent_groups.items():
        rep_code, rep_name, reason, score = choose_representative(entries, mf=mf, quote_cache=quote_cache)
        parent_reps[parent] = (rep_code, rep_name, reason, score)

    # Persist parent masterlist
    try:
        _ensure_data_dir()
        payload = {"meta": {"ts": time.time()}, "parent_groups": parent_groups, "parent_reps": parent_reps}
        with open(PARENT_MASTER_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[parent_mapper] saved parent masterlist to {PARENT_MASTER_FILE}")
    except Exception as e:
        print("[parent_mapper] failed to save parent masterlist:", e)

    return parent_groups, parent_reps


# ============== Helper to build end-to-end ==============
def build_all_exact(force_master: bool = False, max_workers: int = MAX_WORKERS):
    """
    Build masterlist and then parent masterlist (exact normalized grouping).
    Returns (masterlist_map, code_to_name_map, parent_groups, parent_reps)
    """
    master = build_master_list_cache(force=force_master, max_workers=max_workers)

    mf = Mftool()
    try:
        codes_map = mf.get_scheme_codes() or {}
    except Exception:
        codes_map = {}

    # filter codes_map to only codes present in masterlist values
    active_codes = set(master.values())
    code_to_name = {code: name for code, name in codes_map.items() if str(code) in active_codes}

    print(f"[build_all_exact] active masterlist entries: {len(master)}, code_to_name entries: {len(code_to_name)}")

    parent_groups, parent_reps = build_parent_masterlist_from_codes_exact(code_to_name, mf=mf, quote_cache_path=QUOTE_CACHE_FILE)
    return master, code_to_name, parent_groups, parent_reps


# CLI
if __name__ == "__main__":
    print("Rebuilding masterlist (active schemes) and parent masterlist (exact grouping)...")
    t0 = time.time()
    master, code_to_name, parent_groups, parent_reps = build_all_exact(force_master=True, max_workers=MAX_WORKERS)
    t1 = time.time()
    print(f"Done. Active masterlist entries: {len(master)}")
    print(f"Code->Name map entries used: {len(code_to_name)}")
    print(f"Parent products (exact normalized grouping): {len(parent_groups)}")
    print(f"Time elapsed: {t1 - t0:.1f}s")
