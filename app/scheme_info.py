# app/scheme_info.py
"""
Robust simplified scheme_info for UI payload generation.

- SID-derived fields (category, scheme_type, declared_benchmark, fund_objective_summary,
  plans_and_options, exit_load, fund_manager, expense_ratio_percent, asset_allocation_summary)
  are taken from the parent's representative SID (robustly searching common keys/sections).
- Per-scheme numeric/time-series metrics are taken from data/metrics_by_code.json when available,
  otherwise fall back to parent-level representative metrics (metrics_parent_reps.json).
- Writes simplified flattened UI payload to data/metrics_ui.json when run.

Run:
    python -m app.scheme_info
"""
import os
import json
import re
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARENT_MASTERLIST_PATH = os.path.join(DATA_DIR, "parent_masterlist.json")
METRICS_PARENT_REPS_PATH = os.path.join(DATA_DIR, "metrics_parent_reps.json")
METRICS_BY_CODE_PATH = os.path.join(DATA_DIR, "metrics_by_code.json")
SCHEME_DOCS_DIR = os.path.join(DATA_DIR, "scheme_docs")
OUTPUT_UI_PATH = os.path.join(DATA_DIR, "metrics_ui.json")


# ---------------------------
# Utilities
# ---------------------------
def _load_json(path: str) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _safe_get(d: Optional[Dict], *keys, default=None):
    if not isinstance(d, dict):
        return default
    cur = d
    for k in keys:
        if cur is None or not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def _coerce_num(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s == "":
        return None
    s2 = s.replace("%", "").replace(",", "").strip()
    m = re.search(r"[-+]?\d+(\.\d+)?", s2)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    return None


# ---------------------------
# Readers for source files
# ---------------------------
def _read_parent_masterlist() -> Dict[str, Any]:
    pm = _load_json(PARENT_MASTERLIST_PATH)
    if not isinstance(pm, dict):
        return {}
    if "parent_groups" in pm and isinstance(pm["parent_groups"], dict):
        return pm["parent_groups"]
    return {k: v for k, v in pm.items() if k != "meta"}


def _read_parent_metrics() -> Dict[str, Any]:
    m = _load_json(METRICS_PARENT_REPS_PATH)
    if not isinstance(m, dict):
        return {}
    return m


_metrics_by_code_cache: Optional[Dict[str, Any]] = None


def _load_metrics_by_code() -> Dict[str, Any]:
    global _metrics_by_code_cache
    if _metrics_by_code_cache is None:
        _metrics_by_code_cache = _load_json(METRICS_BY_CODE_PATH) or {}
    return _metrics_by_code_cache


def _load_sid_for_code(code: Optional[str]) -> Optional[Dict[str, Any]]:
    if not code:
        return None
    path = os.path.join(SCHEME_DOCS_DIR, f"{str(code).strip()}.json")
    return _load_json(path)


# ---------------------------
# Robust SID field finder
# ---------------------------
def _find_in_sid(sid: Dict[str, Any], candidates: List[str]) -> Optional[Any]:
    """
    Search SID JSON for any of the keys/names in candidates.
    Looks at:
      - top-level keys
      - nested under 'summary', 'highlights', 'part_1', 'part i', common variants
      - nested under keys suggesting section blocks
    Returns first non-empty match (not strictly None/empty string).
    """
    if not isinstance(sid, dict):
        return None

    def _value_ok(v: Any) -> bool:
        return v not in (None, "", [], {})

    # Direct top-level search
    for k in candidates:
        v = sid.get(k)
        if _value_ok(v):
            return v

    # Common nested containers to check
    containers = []
    for key in ("summary", "highlights", "part_1", "part i", "part1", "part_i", "part i. highlights/summary"):
        if key in sid and isinstance(sid[key], dict):
            containers.append(sid[key])

    # also search all top-level dict values that look like sub-sections
    for vtop in sid.values():
        if isinstance(vtop, dict):
            containers.append(vtop)

    # now check containers for candidate keys
    for cont in containers:
        for k in candidates:
            v = cont.get(k)
            if _value_ok(v):
                return v

    # check textual fields that might contain key-value maps as text (rare)
    for k, v in sid.items():
        if isinstance(v, str):
            low = v.lower()
            for cand in candidates:
                if cand.replace("_", " ") in low or cand.replace("_", " ").split()[0] in low:
                    # not reliable to parse — skip
                    pass

    # Finally, check some known alternative keys embedded in the SID (examples)
    alt_key_map = {
        "declared_benchmark": ["declared_benchmark", "benchmark", "benchmarks"],
        "fund_manager": ["fund_manager", "fund_managers", "fund_manager_name"],
        "expense_ratio_percent": ["expense_ratio_percent", "expense_ratio", "ter"],
        "category": ["category", "scheme_category", "scheme_type_category"],
        "scheme_type": ["scheme_type", "type_of_scheme"],
        "plans_and_options": ["plans_and_options", "plans_options", "plans"],
        "asset_allocation_summary": ["asset_allocation_summary", "asset_allocation", "asset_allocation_pattern"],
        "fund_objective_summary": ["fund_objective_summary", "objective_summary", "investment_objective"],
        "exit_load": ["exit_load", "load_structure", "exit_load_percent"],
    }
    # reverse lookup: if any candidate provided matches key in alt_key_map, search those lists
    for cand in candidates:
        if cand in alt_key_map:
            for alt in alt_key_map[cand]:
                # direct top-level
                v = sid.get(alt)
                if _value_ok(v):
                    return v
                # check containers
                for cont in containers:
                    v = cont.get(alt)
                    if _value_ok(v):
                        return v
    return None


# ---------------------------
# Extract SID-derived fields (parent-only) - improved
# ---------------------------
def _extract_parent_sid_fields(sid: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Return a dict with the SID-derived fields, using robust search across the SID JSON.
    """
    # defaults
    out = {
        "category": None,
        "scheme_type": None,
        "declared_benchmark": None,
        "fund_objective_summary": None,
        "plans_and_options": None,
        "asset_allocation_summary": None,
        "fund_manager": None,
        "expense_ratio_percent": None,
        "exit_load": None,
    }
    if not sid or not isinstance(sid, dict):
        return out

    # mapping of desired field -> candidate key names in SID
    key_candidates = {
        "category": ["category", "scheme_category", "scheme_category_name"],
        "scheme_type": ["scheme_type", "type", "scheme_type_name"],
        "declared_benchmark": ["declared_benchmark", "benchmark", "benchmarks"],
        "fund_objective_summary": ["fund_objective_summary", "objective_summary", "investment_objective", "fund_objective"],
        "plans_and_options": ["plans_and_options", "plans_options", "plans", "plans_and_options_text"],
        "asset_allocation_summary": ["asset_allocation_summary", "asset_allocation", "how_will_the_scheme_allocate_its_assets", "asset_allocation_pattern"],
        "fund_manager": ["fund_manager", "fund_managers", "fund_manager_name", "who_manages_the_scheme"],
        "expense_ratio_percent": ["expense_ratio_percent", "expense_ratio", "ter", "annual_scheme_recurring_expenses"],
        "exit_load": ["exit_load", "load_structure", "exit_load_percent", "load"]
    }

    for field, candidates in key_candidates.items():
        val = _find_in_sid(sid, candidates)
        # special coercion for numeric expense_ratio_percent
        if field == "expense_ratio_percent":
            out[field] = _coerce_num(val)
        else:
            out[field] = val if val is not None else None

    return out


# ---------------------------
# Extract numeric metric fields from a metrics entry (per-code or parent rep)
# ---------------------------
def _extract_metrics_fields(metrics_entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not metrics_entry:
        return {}

    if isinstance(metrics_entry, dict) and "metrics" in metrics_entry and isinstance(metrics_entry["metrics"], dict):
        m = metrics_entry["metrics"]
    elif isinstance(metrics_entry, dict):
        m = metrics_entry
    else:
        return {}

    out: Dict[str, Any] = {}
    out["data_points"] = m.get("data_points")
    out["first_date"] = m.get("first_date")
    out["last_date"] = m.get("last_date")
    out["cagr"] = _coerce_num(m.get("cagr"))
    out["rolling_1y"] = _coerce_num(m.get("rolling_1y"))
    out["rolling_3y"] = _coerce_num(m.get("rolling_3y"))
    out["rolling_5y"] = _coerce_num(m.get("rolling_5y"))
    out["volatility_annual"] = _coerce_num(m.get("volatility_annual"))
    out["sharpe"] = _coerce_num(m.get("sharpe"))
    out["sortino"] = _coerce_num(m.get("sortino"))
    out["max_drawdown"] = _coerce_num(m.get("max_drawdown"))
    out["beta"] = _coerce_num(m.get("beta"))
    out["tracking_error"] = _coerce_num(m.get("tracking_error"))
    out["aum"] = _coerce_num(m.get("aum") or _safe_get(m, "scheme_details_raw", "aum"))
    out["_scheme_details_raw"] = m.get("scheme_details_raw") if isinstance(m.get("scheme_details_raw"), dict) else None
    out["_scheme_quote_raw"] = m.get("scheme_quote_raw") if isinstance(m.get("scheme_quote_raw"), dict) else None
    return out


# ---------------------------
# Build flattened child entry
# ---------------------------
def _build_child_simple_entry(child_code: str, child_name: str, sid_fields: Dict[str, Any], metrics_fields: Dict[str, Any]) -> Dict[str, Any]:
    sd = metrics_fields.get("_scheme_details_raw") or {}
    sq = metrics_fields.get("_scheme_quote_raw") or {}

    scheme_start_date = None
    scheme_initial_nav = None
    if sd:
        ssd = sd.get("scheme_start_date")
        if isinstance(ssd, dict):
            scheme_start_date = ssd.get("date")
            try:
                scheme_initial_nav = float(str(ssd.get("nav")).replace(",", "")) if ssd.get("nav") not in (None, "") else None
            except Exception:
                scheme_initial_nav = _coerce_num(ssd.get("nav"))
        else:
            scheme_start_date = sd.get("scheme_start_date") or None

    scheme_latest_date = sq.get("last_updated") if sq else None
    scheme_current_nav = None
    if sq and "nav" in sq:
        try:
            scheme_current_nav = float(str(sq.get("nav")).replace(",", "")) if sq.get("nav") not in (None, "") else None
        except Exception:
            scheme_current_nav = _coerce_num(sq.get("nav"))

    entry = {
        "scheme_name": child_name,
        "scheme_code": child_code,
        # SID-derived (from parent)
        "category": sid_fields.get("category"),
        "scheme_type": sid_fields.get("scheme_type"),
        "declared_benchmark": sid_fields.get("declared_benchmark"),
        "fund_objective_summary": sid_fields.get("fund_objective_summary"),
        "plans_and_options": sid_fields.get("plans_and_options"),
        "asset_allocation_summary": sid_fields.get("asset_allocation_summary"),
        "fund_manager": sid_fields.get("fund_manager"),
        "expense_ratio_percent": sid_fields.get("expense_ratio_percent"),
        "exit_load": sid_fields.get("exit_load"),
        # per-scheme metric fields
        "data_points": metrics_fields.get("data_points"),
        "first_date": metrics_fields.get("first_date"),
        "last_date": metrics_fields.get("last_date"),
        "cagr": metrics_fields.get("cagr"),
        "rolling_1y": metrics_fields.get("rolling_1y"),
        "rolling_3y": metrics_fields.get("rolling_3y"),
        "rolling_5y": metrics_fields.get("rolling_5y"),
        "volatility_annual": metrics_fields.get("volatility_annual"),
        "sharpe": metrics_fields.get("sharpe"),
        "sortino": metrics_fields.get("sortino"),
        "max_drawdown": metrics_fields.get("max_drawdown"),
        "aum": metrics_fields.get("aum"),
        "scheme_start_date": scheme_start_date,
        "scheme_initial_nav": scheme_initial_nav,
        "scheme_latest_date": scheme_latest_date,
        "scheme_current_nav": scheme_current_nav,
        "beta": metrics_fields.get("beta"),
        "tracking_error": metrics_fields.get("tracking_error"),
    }
    return entry


# ---------------------------
# Generator: build simplified UI payload
# ---------------------------
def generate_ui_payload() -> Dict[str, Any]:
    parents = _read_parent_masterlist()
    metrics_parent_reps = _read_parent_metrics()
    metrics_by_code = _load_metrics_by_code()

    out: Dict[str, Any] = {}

    for parent_key, parent_val in parents.items():
        # Determine children list across possible shapes
        raw_children: List[Any] = []
        if isinstance(parent_val, list):
            raw_children = parent_val
        elif isinstance(parent_val, dict):
            if "children" in parent_val and isinstance(parent_val["children"], list):
                raw_children = parent_val["children"]
            else:
                for k in ("schemes", "child_schemes", "members", "list"):
                    if k in parent_val and isinstance(parent_val[k], list):
                        raw_children = parent_val[k]
                        break
        else:
            raw_children = []

        # Representative code/name
        rep_code = None
        rep_name = None
        metrics_entry_parent = metrics_parent_reps.get(parent_key) if isinstance(metrics_parent_reps, dict) else None
        if isinstance(metrics_entry_parent, dict):
            rep_code = str(metrics_entry_parent.get("rep_code")) if metrics_entry_parent.get("rep_code") else None
            rep_name = metrics_entry_parent.get("rep_name") or None

        if not rep_code and isinstance(parent_val, dict):
            rep_code = str(parent_val.get("rep_code") or _safe_get(parent_val, "rep", "rep_code") or "") or None
            rep_name = rep_name or parent_val.get("rep_name") or _safe_get(parent_val, "rep", "rep_name")

        if not rep_code and raw_children:
            first = raw_children[0]
            if isinstance(first, dict):
                rep_code = str(first.get("scheme_code") or first.get("code") or "") or None
                rep_name = rep_name or first.get("scheme_name") or first.get("name")
            elif isinstance(first, (list, tuple)) and len(first) >= 1:
                rep_code = str(first[0]) or None
                rep_name = rep_name or (first[1] if len(first) >= 2 else None)
            else:
                rep_code = str(first) if first else rep_code

        # Load parent's SID (from representative code) and extract robust SID-derived fields
        rep_sid = _load_sid_for_code(rep_code) if rep_code else None
        sid_fields = _extract_parent_sid_fields(rep_sid)

        # parent-level metrics entry used as fallback
        parent_metrics_entry = metrics_entry_parent

        # Build children entries
        children_entries: List[Dict[str, Any]] = []
        for raw in raw_children:
            if isinstance(raw, dict):
                code = str(raw.get("scheme_code") or raw.get("code") or raw.get("schemeCode") or "").strip()
                name = raw.get("scheme_name") or raw.get("name") or raw.get("schemeName") or ""
            elif isinstance(raw, (list, tuple)) and len(raw) >= 1:
                code = str(raw[0]).strip()
                name = raw[1] if len(raw) >= 2 else ""
            else:
                code = str(raw).strip()
                name = ""

            if not name:
                name = rep_name or ""

            # Prefer per-scheme metrics; fallback to parent-level rep metrics
            metrics_entry_for_child = None
            if code:
                metrics_entry_for_child = metrics_by_code.get(code) if isinstance(metrics_by_code, dict) else None
            if not metrics_entry_for_child:
                metrics_entry_for_child = parent_metrics_entry

            metrics_fields = _extract_metrics_fields(metrics_entry_for_child)
            entry = _build_child_simple_entry(code, name, sid_fields, metrics_fields)
            children_entries.append(entry)

        out[parent_key] = {
            "parent_key": parent_key,
            "children": children_entries
        }

    return out


def dump_ui_file() -> str:
    payload = generate_ui_payload()
    os.makedirs(os.path.dirname(OUTPUT_UI_PATH), exist_ok=True)
    with open(OUTPUT_UI_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"[scheme_info] Wrote simplified UI payload to {OUTPUT_UI_PATH}")
    return OUTPUT_UI_PATH


if __name__ == "__main__":
    dump_ui_file()
