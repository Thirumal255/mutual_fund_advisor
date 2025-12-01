"""
scheme_info.py
------------------------------------
Parent-level view for Indian mutual funds.

It joins:

- parent_masterlist.json  (wrapped format)
    {
      "meta": {...},
      "parent_groups": {
        "navi large & midcap fund": [
          ["141414", "Navi Large & Midcap Fund - Direct Annual IDCW Payout"],
          ["141411", "Navi Large & Midcap Fund - Regular Annual IDCW payout"],
          ["135677", "Navi Large & Midcap Fund- Direct Plan- Growth Option"],
          ["135678", "Navi Large & Midcap Fund- Regular Plan- Growth Option"]
        ],
        ...
      },
      "parent_reps": { ... }   # optional
    }

- metrics_parent_reps.json  (parent-level metrics)
    {
      "parent_key": {
        "rep_code": "...",
        "rep_name": "...",
        "rep_reason": "...",
        "rep_reason_score": 100.0,
        "metrics": {
          "scheme_code": "...",
          "cagr": ...,
          "rolling_3y": ...,
          "sharpe": ...,
          "sortino": ...,
          "max_drawdown": ...,
          "expense_ratio_percent": null,
          "exit_load_percent": null,
          ...
        }
      },
      ...
    }

- scheme_docs/<scheme_code>.json (from doc_extractor)
    {
      "scheme_code": "...",
      "scheme_name": "...",
      "category": "...",
      "scheme_type": "...",
      "fund_objective_summary": "...",
      "declared_benchmark": "...",
      "plans_and_options": [...],
      "exit_load": {...},
      "asset_allocation_summary": "...",
      "fund_manager": "...",
      "expense_ratio_percent": 2.25
    }

Exports:

- get_parent_keys()
- search_parent_keys(query)
- get_parent_overview(parent_key)
"""

import os
import json
from typing import Dict, Any, List, Optional, Tuple

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

PARENT_MASTERLIST_PATH = os.path.join(DATA_DIR, "parent_masterlist.json")
# New: metrics_parent_reps file
METRICS_PARENT_REPS_PATH = os.path.join(DATA_DIR, "metrics_parent_reps.json")
# Optional future file (not used currently, but kept for flexibility)
PARENT_METRICS_PATH = os.path.join(DATA_DIR, "parent_metrics.json")

SCHEME_DOCS_DIR = os.path.join(DATA_DIR, "scheme_docs")


def log(msg: str) -> None:
    print(f"[scheme_info] {msg}")


# ---------------------------------------------------------
# Lazy loaders with simple in-memory cache
# ---------------------------------------------------------

_parent_masterlist_groups: Optional[Dict[str, List[List[str]]]] = None
_parent_reps_cache: Optional[Dict[str, Dict[str, Any]]] = None
_parent_metrics_cache: Optional[Dict[str, Dict[str, Any]]] = None


def _load_raw_parent_masterlist() -> Dict[str, Any]:
    if not os.path.exists(PARENT_MASTERLIST_PATH):
        raise FileNotFoundError(f"parent_masterlist.json not found at {PARENT_MASTERLIST_PATH}")
    with open(PARENT_MASTERLIST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_parent_masterlist_groups() -> Dict[str, List[List[str]]]:
    """
    Load the parent grouping mapping:

        parent_key -> [[scheme_code, scheme_name], ...]

    Supports two formats:

    1) Wrapped format (current):
        {
          "meta": {...},
          "parent_groups": { ... as above ... },
          "parent_reps": { ... }
        }

    2) Old flat format:
        {
          "parent_key_1": [[code, name], ...],
          "parent_key_2": [[code, name], ...],
          ...
        }
    """
    global _parent_masterlist_groups

    if _parent_masterlist_groups is not None:
        return _parent_masterlist_groups

    raw = _load_raw_parent_masterlist()

    # Wrapped format: use "parent_groups"
    if isinstance(raw, dict) and "parent_groups" in raw and isinstance(raw["parent_groups"], dict):
        groups = raw["parent_groups"]
        cleaned: Dict[str, List[List[str]]] = {}
        for k, v in groups.items():
            if isinstance(v, list):
                cleaned[str(k)] = v
        _parent_masterlist_groups = cleaned
        log(f"Loaded parent_masterlist (wrapped): {len(cleaned)} parents.")
        return cleaned

    # Flat format fallback
    if isinstance(raw, dict):
        cleaned = {}
        for k, v in raw.items():
            if isinstance(v, list):
                cleaned[str(k)] = v
        _parent_masterlist_groups = cleaned
        log(f"Loaded parent_masterlist (flat): {len(cleaned)} parents.")
        return cleaned

    _parent_masterlist_groups = {}
    log("Loaded parent_masterlist: 0 parents (unrecognized format).")
    return _parent_masterlist_groups


def load_parent_reps() -> Dict[str, Dict[str, Any]]:
    """
    Load parent_reps from parent_masterlist.json (if present).

    Structure:
        parent_key -> {
          "rep_code": "...",
          "rep_name": "...",
          ...
        }
    """
    global _parent_reps_cache
    if _parent_reps_cache is not None:
        return _parent_reps_cache

    raw = _load_raw_parent_masterlist()
    reps: Dict[str, Dict[str, Any]] = {}

    if isinstance(raw, dict) and "parent_reps" in raw and isinstance(raw["parent_reps"], dict):
        for k, v in raw["parent_reps"].items():
            if isinstance(v, dict):
                reps[str(k)] = v

    _parent_reps_cache = reps
    log(f"Loaded parent_reps: {len(reps)} parents.")
    return reps


def load_parent_metrics() -> Dict[str, Dict[str, Any]]:
    """
    Load parent-level metrics.

    Priority:
      1) parent_metrics.json (if you ever create one)
      2) metrics_parent_reps.json  <-- current main source
      3) parent_reps (minimal info fallback)

    For metrics_parent_reps.json, structure is:

        parent_key -> {
          "rep_code": "...",
          "rep_name": "...",
          "rep_reason": "...",
          "rep_reason_score": 100.0,
          "metrics": {
            "scheme_code": "...",
            "cagr": ...,
            ...
          }
        }
    """
    global _parent_metrics_cache
    if _parent_metrics_cache is not None:
        return _parent_metrics_cache

    # (1) If a dedicated parent_metrics.json exists, prefer that (not current case)
    if os.path.exists(PARENT_METRICS_PATH):
        with open(PARENT_METRICS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _parent_metrics_cache = data
        else:
            tmp: Dict[str, Dict[str, Any]] = {}
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict) and "parent_key" in entry:
                        tmp[str(entry["parent_key"])] = entry
            _parent_metrics_cache = tmp
        log(f"Loaded parent_metrics from {PARENT_METRICS_PATH}: {len(_parent_metrics_cache)} parents.")
        return _parent_metrics_cache

    # (2) metrics_parent_reps.json (your current file)
    if os.path.exists(METRICS_PARENT_REPS_PATH):
        with open(METRICS_PARENT_REPS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _parent_metrics_cache = data
            log(f"Loaded parent_metrics from {METRICS_PARENT_REPS_PATH}: {len(_parent_metrics_cache)} parents.")
            return _parent_metrics_cache
        else:
            log(f"metrics_parent_reps.json exists but is not a dict; ignoring.")

    # (3) Fallback: use parent_reps (very minimal info)
    reps = load_parent_reps()
    _parent_metrics_cache = reps
    log(f"No dedicated metrics file found. Using parent_reps as metrics: {len(reps)} parents.")
    return _parent_metrics_cache


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def get_parent_keys() -> List[str]:
    """Return all parent keys (as in parent masterlist)."""
    return list(load_parent_masterlist_groups().keys())


def search_parent_keys(query: str, limit: int = 10) -> List[str]:
    """
    Simple case-insensitive substring search over parent keys.

    Example:
        search_parent_keys("navi large")
        -> ["navi large & midcap fund"]
    """
    q = query.strip().lower()
    if not q:
        return []

    keys = load_parent_masterlist_groups().keys()
    matches = [k for k in keys if q in k.lower()]
    matches.sort()
    return matches[:limit]


def _children_for_parent(parent_key: str) -> List[Dict[str, str]]:
    """
    Return child schemes for a given parent_key as:

        [{"scheme_code": "...", "scheme_name": "..."}, ...]
    """
    master = load_parent_masterlist_groups()
    raw = master.get(parent_key)
    if not raw:
        return []

    children: List[Dict[str, str]] = []
    for item in raw:
        if isinstance(item, list) and len(item) >= 2:
            code, name = item[0], item[1]
            children.append({
                "scheme_code": str(code),
                "scheme_name": str(name),
            })
    return children


def _rep_info_from_metrics(parent_key: str, children: List[Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Determine the representative child (scheme_code, scheme_name)
    for this parent.

    Priority:
      1. Use metrics[parent_key]["rep_code"] / ["rep_name"] if present.
      2. Fallback to first child in the masterlist (if any).
    """
    metrics_entry = load_parent_metrics().get(parent_key)

    rep_code: Optional[str] = None
    rep_name: Optional[str] = None

    if isinstance(metrics_entry, dict):
        if metrics_entry.get("rep_code"):
            rep_code = str(metrics_entry["rep_code"])
        elif metrics_entry.get("scheme_code"):
            rep_code = str(metrics_entry["scheme_code"])
        if metrics_entry.get("rep_name"):
            rep_name = str(metrics_entry["rep_name"])

    if rep_code is None and children:
        rep_code = children[0]["scheme_code"]
        rep_name = children[0]["scheme_name"]

    # If we have a rep_code but no rep_name, try to pull from children list
    if rep_code and not rep_name:
        for ch in children:
            if ch["scheme_code"] == rep_code:
                rep_name = ch["scheme_name"]
                break

    return rep_code, rep_name


def _load_sid_doc(scheme_code: str) -> Optional[Dict[str, Any]]:
    """
    Load SID extraction JSON for the given scheme_code, if present.

    Path: data/scheme_docs/<scheme_code>.json
    """
    path = os.path.join(SCHEME_DOCS_DIR, f"{scheme_code}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to read SID doc for {scheme_code}: {e}")
        return None


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------

def get_parent_overview(parent_key: str) -> Optional[Dict[str, Any]]:
    """
    Return a comprehensive view for a given parent key.

    Structure:

      {
        "parent_key": str,
        "children": [
          {"scheme_code": "...", "scheme_name": "..."},
          ...
        ],
        "rep_code": "...",
        "rep_name": "...",
        "metrics": {...} | {},
        "sid": {...} | None,
        "summary": {...}
      }
    """
    parent_key = str(parent_key)
    children = _children_for_parent(parent_key)
    if not children:
        log(f"No children found for parent '{parent_key}'.")
        return None

    metrics_all = load_parent_metrics()
    metrics_entry = metrics_all.get(parent_key, {}) if isinstance(metrics_all, dict) else {}
    # metrics_numeric is where CAGR, Sharpe, etc. live
    metrics_numeric = metrics_entry.get("metrics", metrics_entry) if isinstance(metrics_entry, dict) else {}

    # 1) Decide representative code/name from metrics or first child
    rep_code, rep_name = _rep_info_from_metrics(parent_key, children)

    # 2) Try to load SID doc for that rep_code
    sid_doc: Optional[Dict[str, Any]] = None
    sid_code_for_doc: Optional[str] = None

    if rep_code:
        sid_doc = _load_sid_doc(rep_code)
        sid_code_for_doc = rep_code

    # 3) Fallback: if no SID for rep_code, try other children
    if sid_doc is None:
        for ch in children:
            code = ch["scheme_code"]
            doc = _load_sid_doc(code)
            if doc:
                sid_doc = doc
                sid_code_for_doc = code
                # Only promote this as rep if we don't really have a metrics-based rep
                if not rep_code or not metrics_entry:
                    rep_code = code
                    rep_name = ch["scheme_name"]
                log(f"Using {code} as representative for SID info of parent '{parent_key}'.")
                break

    # Build a condensed summary, preferring SID info
    display_name = rep_name or children[0]["scheme_name"]

    category = sid_doc.get("category") if sid_doc else None
    scheme_type = sid_doc.get("scheme_type") if sid_doc else None
    declared_benchmark = sid_doc.get("declared_benchmark") if sid_doc else None
    fund_manager = sid_doc.get("fund_manager") if sid_doc else None

    expense_ratio = None
    if sid_doc and sid_doc.get("expense_ratio_percent") is not None:
        expense_ratio = sid_doc["expense_ratio_percent"]
    elif isinstance(metrics_numeric, dict) and metrics_numeric.get("expense_ratio_percent") is not None:
        expense_ratio = metrics_numeric["expense_ratio_percent"]

    exit_load = sid_doc.get("exit_load") if sid_doc else None

    summary = {
        "display_name": display_name,
        "category": category,
        "scheme_type": scheme_type,
        "declared_benchmark": declared_benchmark,
        "fund_manager": fund_manager,
        "expense_ratio_percent": expense_ratio,
        "exit_load": exit_load,
    }

    return {
        "parent_key": parent_key,
        "children": children,
        "rep_code": rep_code,
        "rep_name": rep_name,
        "metrics": metrics_entry,
        "metrics_numeric": metrics_numeric,
        "sid": sid_doc,
        "summary": summary,
    }


if __name__ == "__main__":
    # Small debug: allow quick CLI usage
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.scheme_info <parent_search_string>")
        raise SystemExit(1)

    query = " ".join(sys.argv[1:])
    matches = search_parent_keys(query, limit=5)
    if not matches:
        print(f"No parents found matching '{query}'.")
        raise SystemExit(0)

    print("Matches:")
    for idx, k in enumerate(matches, start=1):
        print(f"  {idx}. {k}")

    chosen = matches[0]
    print(f"\nUsing first match: {chosen}\n")

    info = get_parent_overview(chosen)
    if not info:
        print("No info for parent.")
    else:
        from pprint import pprint
        pprint(info["summary"])
        print("\nChildren:")
        for ch in info["children"]:
            print("  -", ch["scheme_code"], "-", ch["scheme_name"])
