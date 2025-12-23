# app/masterlist.py

import os
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from mftool import Mftool

# ==========================================================
# Paths & config
# ==========================================================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

MASTER_CACHE_FILE = os.path.join(DATA_DIR, "masterlist.json")
DETAILS_CACHE_FILE = os.path.join(DATA_DIR, "details_cache.json")
QUOTE_CACHE_FILE = os.path.join(DATA_DIR, "quote_cache.json")
SYSTEM_STATUS_FILE = os.path.join(DATA_DIR, "system_status.json")

MAX_WORKERS = 12
CHECKPOINT_EVERY = 200
CACHE_TTL_SECONDS = 24 * 3600  # 1 day

os.makedirs(DATA_DIR, exist_ok=True)

_masterlist_cache = None

# ==========================================================
# Utilities
# ==========================================================
def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_masterlist_status(status: str, message: str):
    """
    status: live | cached | failed
    """
    payload = {}
    if os.path.exists(SYSTEM_STATUS_FILE):
        try:
            with open(SYSTEM_STATUS_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            payload = {}

    payload["masterlist"] = {
        "status": status,
        "last_updated": utc_now(),
        "message": message,
    }

    with open(SYSTEM_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _load_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _safe_write_json(path: str, data: Dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _normalize(name: str) -> str:
    return " ".join(name.lower().split())


# ==========================================================
# AMFI connection with retry
# ==========================================================
def create_mftool_with_retry(retries: int = 3, delay: int = 5) -> Mftool:
    last_err = None
    for i in range(retries):
        try:
            print(f"[masterlist] Connecting to AMFI (attempt {i+1}/{retries})...")
            return Mftool()
        except Exception as e:
            last_err = e
            print("[masterlist] AMFI connection failed:", e)
            if i < retries - 1:
                print(f"[masterlist] Retrying in {delay} seconds...")
                time.sleep(delay)
    raise last_err


# ==========================================================
# Scheme activity check
# ==========================================================
def _check_code_active(mf, code, details_cache, quote_cache):
    code = str(code)

    if code not in details_cache:
        try:
            details_cache[code] = mf.get_scheme_details(code) or {}
        except Exception:
            details_cache[code] = {}

    if code not in quote_cache:
        try:
            quote_cache[code] = mf.get_scheme_quote(code) or {}
        except Exception:
            quote_cache[code] = {}

    quote = quote_cache.get(code, {})
    is_active = bool(quote.get("nav"))

    return code, is_active


# ==========================================================
# Masterlist builder
# ==========================================================
def build_master_list_cache(force: bool = False) -> Dict[str, str]:
    global _masterlist_cache

    if _masterlist_cache and not force:
        return _masterlist_cache

    # Load cached masterlist
    if os.path.exists(MASTER_CACHE_FILE) and not force:
        try:
            with open(MASTER_CACHE_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            ts = payload.get("meta", {}).get("timestamp")
            if ts:
                _masterlist_cache = payload["master"]
                print(f"[masterlist] loaded cached masterlist from disk ({len(_masterlist_cache)} entries)")
                write_masterlist_status("cached", "Loaded cached masterlist (TTL valid)")
                return _masterlist_cache
        except Exception:
            pass

    # Load per-code caches
    details_cache = _load_json(DETAILS_CACHE_FILE)
    quote_cache = _load_json(QUOTE_CACHE_FILE)

    # Try live fetch
    try:
        mf = create_mftool_with_retry()
        codes_map = mf.get_scheme_codes() or {}
        print(f"[masterlist] total schemes from mftool: {len(codes_map)}")
        source = "live"
    except Exception:
        if os.path.exists(MASTER_CACHE_FILE):
            with open(MASTER_CACHE_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            _masterlist_cache = payload["master"]
            print("[masterlist] Using cached masterlist due to AMFI failure.")
            write_masterlist_status("cached", "AMFI unavailable; used cached masterlist")
            return _masterlist_cache

        write_masterlist_status("failed", "AMFI unreachable and no cache available")
        raise RuntimeError("AMFI unreachable and no cached masterlist found")

    # Build fresh
    master = {}
    processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_check_code_active, mf, code, details_cache, quote_cache): (code, name)
            for code, name in codes_map.items()
        }

        for fut in as_completed(futures):
            code, name = futures[fut]
            try:
                _, is_active = fut.result()
            except Exception:
                is_active = False

            processed += 1
            if is_active:
                master[_normalize(name)] = str(code)

            if processed % CHECKPOINT_EVERY == 0:
                print(f"[masterlist] processed {processed}/{len(codes_map)}")

    # Persist
    payload = {
        "meta": {"timestamp": utc_now()},
        "master": master,
    }

    _safe_write_json(MASTER_CACHE_FILE, payload)
    _safe_write_json(DETAILS_CACHE_FILE, details_cache)
    _safe_write_json(QUOTE_CACHE_FILE, quote_cache)

    _masterlist_cache = master
    write_masterlist_status("live", "Masterlist rebuilt from live AMFI data")

    return master


# ==========================================================
# CLI
# ==========================================================
if __name__ == "__main__":
    print("Rebuilding masterlist (active schemes)...")
    t0 = time.time()
    write_masterlist_status(
    "running",
    "Masterlist rebuild started"
    )

    try:
        master = build_master_list_cache(force=True)
    except Exception as e:
        print("[masterlist] FAILED:", e)
        exit(1)

    t1 = time.time()
    print(f"Done. Active masterlist entries: {len(master)}")
    print(f"Time elapsed: {t1 - t0:.1f}s")
