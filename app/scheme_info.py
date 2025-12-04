"""
app.scheme_info

Provides helper functions to query parent masterlist, load parent metrics,
load SID-extracted JSON docs and return a UI-ready overview containing:

- summary: top-level fields (from SID if available)
- representative_child: chosen child rep (code + name)
- children: list of child dicts {scheme_code, scheme_name}
- metrics: raw metrics entry from metrics_parent_reps.json (if any)
- metrics_numeric: normalized numeric metrics ready for UI (cagr, sharpe, ...),
                   merged with SID-extracted fields (expense_ratio_percent, benchmark, etc.)
- sid: loaded SID JSON (if available for rep)

Expected files under project root `data/`:
    data/parent_masterlist.json
    data/metrics_parent_reps.json
    data/scheme_docs/<scheme_code>.json
"""
import os
import json
import re
from typing import Any, Dict, List, Optional
from difflib import get_close_matches

# Paths (assumes project layout where `app/` is a subfolder of repo root)
BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARENT_MASTERLIST_PATH = os.path.join(DATA_DIR, "parent_masterlist.json")
METRICS_PARENT_REPS_PATH = os.path.join(DATA_DIR, "metrics_parent_reps.json")
SCHEME_DOCS_DIR = os.path.join(DATA_DIR, "scheme_docs")


# ---------------------------
# Utility: load JSON safely
# ---------------------------
def _load_json(path: str) -> Optional[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


# ---------------------------
# Simple name normalization
# ---------------------------
def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


# ---------------------------
# Parent keys retrieval
# ---------------------------
def get_parent_keys() -> List[str]:
    pm = _load_json(PARENT_MASTERLIST_PATH)
    if not pm:
        return []
    # If structure { meta:..., parent_groups: { ... } }
    if isinstance(pm, dict) and "parent_groups" in pm and isinstance(pm["parent_groups"], dict):
        keys = list(pm["parent_groups"].keys())
    else:
        keys = [k for k in pm.keys() if k != "meta"]
    return keys


# ---------------------------
# Search parent keys (substring + fuzzy)
# ---------------------------
def search_parent_keys(query: str, limit: int = 10) -> List[str]:
    q = _norm(query)
    parents = get_parent_keys()
    if not parents:
        return []
    normalized_map = {p: _norm(p) for p in parents}

    # 1) substring contains
    contains = [p for p, np in normalized_map.items() if q in np]
    if contains:
        return contains[:limit]

    # 2) difflib close matches on normalized names
    names = list(normalized_map.values())
    close = get_close_matches(q, names, n=limit, cutoff=0.6)
    rev = {v: k for k, v in normalized_map.items()}
    results = [rev[c] for c in close if c in rev]
    return results[:limit]


# ---------------------------
# Merging helpers
# ---------------------------
def _coerce_num(val: Any) -> Optional[float]:
    """Try to coerce different shapes to a float or None."""
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, dict):
            if "value" in val and val["value"] not in (None, ""):
                return float(val["value"])
            for k in ("amount", "percent", "value"):
                if k in val and val[k] not in (None, ""):
                    return float(val[k])
            return None
        s = str(val).strip()
        if s == "":
            return None
        s2 = s.replace("%", "").replace(",", "").strip()
        m = re.search(r"[-+]?\d+(\.\d+)?", s2)
        if m:
            return float(m.group(0))
        return None
    except Exception:
        return None


def _pick_preferred_value(metrics_raw: Optional[dict], sid: Optional[dict], summary: Optional[dict], *keys):
    """
    Priority search for keys: metrics_raw -> sid -> summary
    """
    cand_metrics = None
    if metrics_raw and isinstance(metrics_raw, dict):
        if "metrics" in metrics_raw and isinstance(metrics_raw["metrics"], dict):
            cand_metrics = metrics_raw["metrics"]
        else:
            cand_metrics = metrics_raw

    for key in keys:
        # metrics
        if cand_metrics and key in cand_metrics and cand_metrics.get(key) not in (None, ""):
            return cand_metrics.get(key)
        # sid
        if sid and isinstance(sid, dict):
            if key in sid and sid.get(key) not in (None, ""):
                return sid.get(key)
            alt = {
                "fund_manager": ["fund_manager", "fund_manager_name", "fund_managers"],
                "expense_ratio_percent": ["expense_ratio_percent", "expense_ratio", "ter"],
                "declared_benchmark": ["declared_benchmark", "benchmark"],
                "plans_and_options": ["plans_and_options", "plans_options", "plans"],
                "fund_objective_summary": ["fund_objective_summary", "objective_summary", "investment_objective"],
                "exit_load": ["exit_load", "load_structure"],
            }
            if key in alt:
                for ak in alt[key]:
                    if ak in sid and sid.get(ak) not in (None, ""):
                        return sid.get(ak)
        # summary
        if summary and key in summary and summary.get(key) not in (None, ""):
            return summary.get(key)
    return None


def merge_sid_summary_into_metrics(metrics_numeric: dict, metrics_raw: dict, sid: dict, summary: dict):
    """
    Merge SID / summary values into metrics_numeric (in-place) and ensure summary contains SID fields.
    """
    numeric_keys = {"expense_ratio_percent": _coerce_num}

    sid_fields = [
        "declared_benchmark",
        "expense_ratio_percent",
        "exit_load",
        "fund_manager",
        "category",
        "scheme_type",
        "plans_and_options",
        "fund_objective_summary",
        "scheme_name",
        "display_name",
        "aum_reported_text",
    ]

    # 1) numeric merges
    for k, coerce_fn in numeric_keys.items():
        existing_val = None
        if metrics_raw:
            if isinstance(metrics_raw, dict) and "metrics" in metrics_raw and isinstance(metrics_raw["metrics"], dict):
                existing_val = metrics_raw["metrics"].get(k)
            elif isinstance(metrics_raw, dict):
                existing_val = metrics_raw.get(k)
        if existing_val not in (None, ""):
            metrics_numeric[k] = coerce_fn(existing_val)
            continue
        picked = _pick_preferred_value(metrics_raw, sid, summary, k)
        metrics_numeric[k] = coerce_fn(picked)

    # 2) non-numeric merges into summary and metrics_numeric
    for k in sid_fields:
        picked = _pick_preferred_value(metrics_raw, sid, summary, k)
        if picked is not None:
            if summary is not None:
                if summary.get(k) in (None, ""):
                    summary[k] = picked
            if k == "expense_ratio_percent":
                metrics_numeric[k] = _coerce_num(picked)
            else:
                metrics_numeric[k] = picked

    # 3) ensure display_name exists
    disp = None
    if summary:
        disp = summary.get("display_name") or summary.get("scheme_name") or summary.get("scheme_name_display")
    if not disp and sid and isinstance(sid, dict):
        disp = sid.get("scheme_name")
    if disp:
        summary["display_name"] = disp
        metrics_numeric["display_name"] = disp

    return metrics_numeric, summary


# ---------------------------
# Main: get_parent_overview
# ---------------------------
def get_parent_overview(parent_key: str) -> Dict[str, Any]:
    """
    Build a comprehensive overview for a parent_key.

    Returns dict with keys:
      - parent_key
      - summary
      - representative_child (rep_code, rep_name)
      - children
      - metrics (raw)
      - metrics_numeric (normalized)
      - sid (loaded sid json)
    """
    parent_key_norm = _norm(parent_key)
    pm = _load_json(PARENT_MASTERLIST_PATH) or {}
    if isinstance(pm, dict) and "parent_groups" in pm and isinstance(pm["parent_groups"], dict):
        parent_groups = pm["parent_groups"]
    else:
        parent_groups = {k: v for k, v in pm.items() if k != "meta"} if isinstance(pm, dict) else {}

    chosen_key = None
    for k in parent_groups.keys():
        if _norm(k) == parent_key_norm:
            chosen_key = k
            break
    if not chosen_key:
        candidates = search_parent_keys(parent_key, limit=1)
        chosen_key = candidates[0] if candidates else None

    if not chosen_key:
        return {}

    children_raw = parent_groups.get(chosen_key, [])
    children = []
    for entry in children_raw:
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            code = str(entry[0])
            name = entry[1]
            children.append({"scheme_code": code, "scheme_name": name})
        elif isinstance(entry, dict):
            code = str(entry.get("scheme_code", "")) or str(entry.get("code", ""))
            name = entry.get("scheme_name") or entry.get("name") or ""
            children.append({"scheme_code": code, "scheme_name": name})

    metrics_all = _load_json(METRICS_PARENT_REPS_PATH) or {}
    metrics_entry = metrics_all.get(chosen_key)
    rep_code = None
    rep_name = None
    if metrics_entry and isinstance(metrics_entry, dict):
        rep_code = str(metrics_entry.get("rep_code")) if metrics_entry.get("rep_code") else None
        rep_name = metrics_entry.get("rep_name")
    if not rep_code:
        if children:
            rep_code = children[0]["scheme_code"]
            rep_name = children[0]["scheme_name"]

    sid = None
    if rep_code:
        sid_path = os.path.join(SCHEME_DOCS_DIR, f"{rep_code}.json")
        sid = _load_json(sid_path) or None

    summary = {
        "display_name": rep_name,
        "category": None,
        "scheme_type": None,
        "declared_benchmark": None,
        "fund_manager": None,
        "expense_ratio_percent": None,
        "exit_load": None,
    }
    if sid and isinstance(sid, dict):
        for k in ("scheme_name", "category", "scheme_type", "declared_benchmark", "fund_manager", "expense_ratio_percent", "exit_load", "plans_and_options", "fund_objective_summary", "aum_reported_text"):
            if k in sid and sid.get(k) not in (None, ""):
                summary[k if k != "scheme_name" else "display_name"] = sid.get(k)

    if metrics_entry and isinstance(metrics_entry, dict):
        m = metrics_entry.get("metrics") if "metrics" in metrics_entry else metrics_entry
        if isinstance(m, dict):
            if summary.get("expense_ratio_percent") in (None, "") and "expense_ratio_percent" in m:
                summary["expense_ratio_percent"] = m.get("expense_ratio_percent")
            if summary.get("fund_manager") in (None, "") and "fund_manager" in m:
                summary["fund_manager"] = m.get("fund_manager")

    metrics_numeric = {}
    numeric_keys = [
        "scheme_code",
        "data_points",
        "first_date",
        "last_date",
        "cagr",
        "rolling_1y",
        "rolling_3y",
        "rolling_5y",
        "volatility_annual",
        "sharpe",
        "sortino",
        "max_drawdown",
        "beta",
        "tracking_error",
        "expense_ratio_percent",
    ]
    if metrics_entry and isinstance(metrics_entry, dict):
        m = metrics_entry.get("metrics") if "metrics" in metrics_entry else metrics_entry
        if isinstance(m, dict):
            for k in numeric_keys:
                metrics_numeric[k] = m.get(k) if k in m else None
    else:
        for k in numeric_keys:
            metrics_numeric[k] = None

    if not metrics_numeric.get("scheme_code"):
        metrics_numeric["scheme_code"] = rep_code

    metrics_numeric, summary = merge_sid_summary_into_metrics(metrics_numeric, metrics_entry, sid or {}, summary)

    info = {
        "parent_key": chosen_key,
        "summary": summary,
        "representative_child": {"rep_code": rep_code, "rep_name": rep_name},
        "children": children,
        "metrics": metrics_entry or {},
        "metrics_numeric": metrics_numeric,
        "sid": sid or {},
    }
    return info
