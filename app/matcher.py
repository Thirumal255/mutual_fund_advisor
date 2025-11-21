# app/matcher.py
"""
Fuzzy matcher for scheme names -> AMFI codes using RapidFuzz and the masterlist.
Does NOT import any private helpers from masterlist.py.

Provides:
- match_name_to_code(name, threshold=75, limit=5) -> List[(scheme_name, code, score)]
- best_match(name, threshold=75) -> (scheme_name, code, score) or (None, None, 0.0)
"""

from typing import List, Tuple, Optional, Dict
from rapidfuzz import fuzz, process
from mftool import Mftool
from .masterlist import build_master_list_cache

# Local normalizer (do not rely on master's private helpers)
def _normalize_query(q: str) -> str:
    if not q:
        return ""
    return " ".join(str(q).lower().strip().split())

def _build_choices_and_map() -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    """
    Returns:
      - choices: list of normalized names (strings) used for fuzzy matching
      - norm_to_code: mapping normalized_name -> code (from masterlist)
      - code_to_orig_name: mapping code -> original name (from mftool.get_scheme_codes())
    """
    master = build_master_list_cache()
    choices = list(master.keys())  # these are normalized names
    norm_to_code = dict(master)    # normalized_name -> code

    # get original names from mftool (code -> original name)
    code_to_orig = {}
    try:
        mf = Mftool()
        codes_map = mf.get_scheme_codes() or {}
        # codes_map: {code: original_name}
        for code, orig in codes_map.items():
            code_to_orig[str(code)] = orig
    except Exception:
        # If mftool fails, we will fallback to using normalized names as display names
        code_to_orig = {}

    return choices, norm_to_code, code_to_orig

def match_name_to_code(name: str, threshold: int = 75, limit: int = 5) -> List[Tuple[str, str, float]]:
    """
    Return up to `limit` matches as tuples: (scheme_name_display, scheme_code, score)
    score is 0..100 (higher = better).
    Uses RapidFuzz.process.extract with token_set_ratio by default.
    """
    if not name or not isinstance(name, str):
        return []

    query = _normalize_query(name)
    choices, norm_to_code, code_to_orig = _build_choices_and_map()
    if not choices:
        return []

    # Rapidfuzz: returns tuples (choice, score, idx)
    results = process.extract(query, choices, scorer=fuzz.token_set_ratio, limit=limit)

    out: List[Tuple[str, str, float]] = []
    for choice, score, _index in results:
        if score < threshold:
            continue
        code = norm_to_code.get(choice)
        if not code:
            continue
        # map code -> original name for display where possible
        orig_name = code_to_orig.get(str(code)) or choice
        out.append((orig_name, str(code), float(score)))
    return out

def best_match(name: str, threshold: int = 75) -> Tuple[Optional[str], Optional[str], float]:
    matches = match_name_to_code(name, threshold=threshold, limit=1)
    if not matches:
        return (None, None, 0.0)
    return matches[0]
