"""
Manual test: Full parent-level scheme info dump

Usage:
  # Run preset queries
  python -m tests.test_scheme_info_full_manual

  # Or run a custom query
  python -m tests.test_scheme_info_full_manual "navi large & midcap"
"""

import json
import sys
from typing import List
from app.scheme_info import get_parent_overview, search_parent_keys, get_parent_keys

# default queries used when no CLI args provided
DEFAULT_QUERIES = [
    "navi large & midcap",
    "sbi gold fund",
    "parag parikh flexi cap",
    "quantum nifty 50 etf",
    "hdfc corporate bond fund",
    "lic mf multi cap",
]


def pretty_print(title: str) -> None:
    print("\n" + "=" * 30)
    print(title)
    print("=" * 30)


def dump_json(obj, title: str = None, indent: int = 2, max_chars: int = 2000) -> None:
    if title:
        pretty_print(title)
    txt = json.dumps(obj, indent=indent, ensure_ascii=False)
    if len(txt) <= max_chars:
        print(txt)
    else:
        print(txt[:max_chars])
        print("... (truncated output, total %d chars) ..." % len(txt))


def show_parent_info(parent_key: str) -> None:
    pretty_print(f"Parent key: {parent_key}")
    info = get_parent_overview(parent_key)
    if not info:
        print("No info returned for parent_key ->", parent_key)
        return

    # Summary
    pretty_print("SUMMARY (top-level)")
    print(json.dumps(info.get("summary", {}), indent=2, ensure_ascii=False))

    # Representative child
    pretty_print("REPRESENTATIVE CHILD")
    rep = info.get("representative_child", {})
    print(f"rep_code: {rep.get('rep_code')}")
    print(f"rep_name: {rep.get('rep_name')}")

    # Children (all)
    pretty_print(f"CHILDREN (count={len(info.get('children', []))})")
    for ch in info.get("children", []):
        print(f" - {ch.get('scheme_code')}  |  {ch.get('scheme_name')}")

    # Raw metrics entry
    pretty_print("RAW METRICS ENTRY")
    dump_json(info.get("metrics", {}), max_chars=4000)

    # Numeric metrics
    pretty_print("METRICS_NUMERIC (normalized)")
    print(json.dumps(info.get("metrics_numeric", {}), indent=2, ensure_ascii=False))

    # SID doc keys & excerpt
    sid = info.get("sid", {})
    if sid:
        pretty_print("SID DOC - keys")
        print(", ".join(sorted(list(sid.keys()))))
        pretty_print("SID DOC - excerpt")
        excerpt = {
            "scheme_code": sid.get("scheme_code"),
            "scheme_name": sid.get("scheme_name"),
            "category": sid.get("category"),
            "scheme_type": sid.get("scheme_type"),
            "declared_benchmark": sid.get("declared_benchmark"),
            "fund_manager": sid.get("fund_manager"),
            "expense_ratio_percent": sid.get("expense_ratio_percent"),
            "exit_load": sid.get("exit_load"),
            "fund_objective_summary": sid.get("fund_objective_summary"),
        }
        dump_json(excerpt, max_chars=4000)
    else:
        pretty_print("SID DOC")
        print("No SID extracted JSON found for this parent's representative child or children.")

    pretty_print("EXTRA")
    print("parent_key:", info.get("parent_key"))
    rep = info.get("representative_child", {})
    print("rep (code,name):", rep.get("rep_code"), rep.get("rep_name"))
    print("Total children:", len(info.get("children", [])))
    print()


def main(argv: List[str]) -> None:
    # Accept queries via CLI args, otherwise use DEFAULT_QUERIES
    if len(argv) > 1:
        queries = [" ".join(argv[1:])]
    else:
        queries = DEFAULT_QUERIES

    all_parents = get_parent_keys()
    print(f"\n[schema test] parent_masterlist contains {len(all_parents)} parents (sample 10):")
    print("  ", all_parents[:10])

    for q in queries:
        pretty_print(f"Query: {q}")
        matches = search_parent_keys(q, limit=10)
        print("Matches:", matches)
        if not matches:
            print("No parents found for query:", q)
            continue
        chosen = matches[0]
        print("Using first match:", chosen)
        show_parent_info(chosen)

    print("\nDone.\n")


if __name__ == "__main__":
    main(sys.argv)
