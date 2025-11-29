# tests/test_masterlist_full_manual.py
"""
Comprehensive manual test:
- raw schemes fetched from AMFI
- filtered (active) masterlist
- search checks: exact and fuzzy
- parent grouping (exact normalized grouping)
Run:
  python -m tests.test_masterlist_full_manual
"""

import sys
import os
import json
from pprint import pprint

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from mftool import Mftool
from app.masterlist import build_master_list_cache, build_all_exact, PARENT_MASTER_FILE
from app.matcher import match_name_to_code, best_match

# Example codes to sanity-check grouping (your Sundaram example)
SUNDARAM_EXAMPLE_CODES = [
    "139710", "139712", "142154", "142152", "139711", "139709"
]

# Example search queries for checks (mix of short/brand/long names)
SEARCH_QUERIES = [
    "ICICI Prudential Bluechip Fund",
    "SBI Small Cap Fund",
    "HDFC Top 100",
    "Axis Banking & PSU Debt Fund",
    "Aditya Birla Sun Life Banking & PSU Debt Fund"
]


def normalize_for_lookup(s: str) -> str:
    """Use same normalization used in masterlist (lower + collapse spaces)."""
    return " ".join(str(s).lower().strip().split())


def exact_search(query: str, master: dict):
    """Exact search: normalized query key lookup in master (normalized_name -> code)."""
    key = normalize_for_lookup(query)
    code = master.get(key)
    return key, code


def fuzzy_search(query: str, threshold: int = 70, limit: int = 5):
    """Fuzzy search using matcher.match_name_to_code and best_match."""
    matches = match_name_to_code(query, threshold=threshold, limit=limit)
    best = best_match(query, threshold=threshold)
    return matches, best


def find_parent_for_code(code: str, parent_groups: dict) -> str:
    for parent_key, entries in parent_groups.items():
        for c, name in entries:
            if str(c) == str(code):
                return parent_key
    return ""


def main():
    print("\n=== Masterlist full manual test ===\n")

    # 1) Raw schemes fetched from AMFI (plan-level)
    print("1) Fetching raw schemes from AMFI (mftool.get_scheme_codes)...")
    mf = Mftool()
    try:
        raw_codes = mf.get_scheme_codes() or {}
    except Exception as e:
        print("ERROR fetching scheme codes:", e)
        raw_codes = {}
    print("  - Raw plan-level scheme count (AMFI):", len(raw_codes))

    # show a tiny sample
    sample_items = list(raw_codes.items())[:6]
    print("  - Sample raw codes (code -> name):")
    for c, n in sample_items:
        print("    ", c, "->", n)

    # 2) Build filtered active masterlist (normalized_name -> code)
    print("\n2) Building filtered active masterlist (this may use caches)...")
    master = build_master_list_cache(force=False)
    print("  - Filtered active masterlist entries (normalized_name -> code):", len(master))
    # sample
    sample_master = list(master.items())[:8]
    print("  - Sample active (normalized_name -> code):")
    for nm, c in sample_master:
        print("    ", nm, "->", c)

    # 3) Search checks (exact + fuzzy)
    print("\n3) Search checks (exact and fuzzy):")
    for q in SEARCH_QUERIES:
        print(f"\n Query: {q}")
        key, code = exact_search(q, master)
        print("  - Exact normalized key:", key)
        if code:
            print("  - Exact match found in masterlist -> code:", code)
        else:
            print("  - Exact match NOT found in masterlist")

        matches, best = fuzzy_search(q, threshold=60, limit=5)
        print("  - Fuzzy matches (top results):")
        pprint(matches)
        print("  - Best match:")
        pprint(best)

    # 4) Build parent groupings and reps (exact normalized grouping)
    print("\n4) Building parent groups (exact normalized grouping) via build_all_exact()...")
    # build_all_exact returns master, code_to_name, parent_groups, parent_reps
    master2, code_to_name, parent_groups, parent_reps = build_all_exact(force_master=False)
    print("  - Active masterlist entries (from build_all_exact):", len(master2))
    print("  - code_to_name entries (filtered active codes):", len(code_to_name))
    print("  - parent groups (exact normalized):", len(parent_groups))
    print("  - parent representatives:", len(parent_reps))

    # show a few sample parents
    print("\n  - Sample parent groups (first 6):")
    cnt = 0
    for parent_key, entries in parent_groups.items():
        rep = parent_reps.get(parent_key)
        print("\n   Parent key:", parent_key)
        print("    Representative:", rep)
        print("    Variants (up to 6):")
        for c, nm in entries[:6]:
            print("      -", c, "-", nm)
        cnt += 1
        if cnt >= 6:
            break

    # 5) Focused check for Sundaram example codes
    print("\n5) Focused grouping check for Sundaram example codes:")
    for code in SUNDARAM_EXAMPLE_CODES:
        parent_key = find_parent_for_code(code, parent_groups)
        if not parent_key:
            print(f"  - Code {code}: NOT FOUND in parent_groups (may be filtered out).")
            continue
        entries = parent_groups[parent_key]
        rep = parent_reps.get(parent_key)
        print(f"\n  - Code {code} -> Parent key: {parent_key}")
        print("    Representative:", rep)
        print("    All variants for this parent (count={}):".format(len(entries)))
        for c, nm in entries:
            marker = "<- queried" if str(c) == str(code) else ""
            print("     -", c, "-", nm, marker)

    # 6) Persisted parent file check
    if os.path.exists(PARENT_MASTER_FILE):
        try:
            payload = json.load(open(PARENT_MASTER_FILE, "r", encoding="utf-8"))
            print("\nPersisted parent_masterlist found at:", PARENT_MASTER_FILE)
            print(" Persisted parent_groups count:", len(payload.get("parent_groups", {})))
        except Exception as e:
            print(" Could not read persisted parent_masterlist:", e)
    else:
        print("\nNo persisted parent_masterlist file found at:", PARENT_MASTER_FILE)

    print("\n=== Test complete ===\n")


if __name__ == "__main__":
    main()
