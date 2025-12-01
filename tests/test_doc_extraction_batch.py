"""
tests/test_doc_extraction_batch.py

Batch test for SID extraction over multiple schemes.

Usage (from project root):

  python -m tests.test_doc_extraction_batch --limit 5
"""

import os
import sys
import json
from typing import Dict, Any

# Ensure project root is on sys.path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.doc_extractor import parse_scheme_pdf  # type: ignore
from app import doc_index  # type: ignore


def load_sid_index() -> Dict[str, Any]:
    if hasattr(doc_index, "load_sid_index"):
        return doc_index.load_sid_index()  # type: ignore

    data_dir = os.path.join(ROOT, "data")
    index_path = os.path.join(data_dir, "sid_index.json")
    if not os.path.exists(index_path):
        raise RuntimeError("Could not find SID index (no load_sid_index and no data/sid_index.json)")

    with open(index_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_pdf_path(entry: Any) -> str:
    if isinstance(entry, str):
        return entry

    if isinstance(entry, dict):
        for key in ("sid_path", "pdf_path", "path"):
            if key in entry and entry[key]:
                return entry[key]

    raise ValueError(f"Cannot resolve pdf path from index entry: {entry!r}")


def main():
    limit = None
    args = sys.argv[1:]
    if "--limit" in args:
        i = args.index("--limit")
        if i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                print("Invalid --limit value, ignoring --limit.")
        args = args[:i] + args[i + 2:]

    sid_index = load_sid_index()
    codes = list(sid_index.keys())
    total = len(codes)
    print(f"Total SIDs in index: {total}")

    if limit is not None:
        codes = codes[:limit]
        print(f"Processing first {limit} schemes...\n")
    else:
        print(f"Processing all {total} schemes...\n")

    count_obj = 0
    count_alloc = 0
    count_fm = 0
    count_exp = 0
    count_exit = 0

    for i, code in enumerate(codes, start=1):
        entry = sid_index[code]
        pdf_path = resolve_pdf_path(entry)

        print(f"\n=== Scheme {code} | SID: {pdf_path} ===")
        res = parse_scheme_pdf(pdf_path, code)

        print("  scheme_name:", res.get("scheme_name"))
        print("  category:", res.get("category"))
        print("  scheme_type:", res.get("scheme_type"))
        print("  benchmark:", res.get("declared_benchmark"))

        obj = res.get("fund_objective_summary")
        if obj:
            count_obj += 1
            print("  objective (snippet):", obj[:140], "...")
        else:
            print("  objective: None")

        alloc = res.get("asset_allocation_summary")
        if alloc:
            count_alloc += 1
            print("  asset_allocation_summary (snippet):", alloc[:140], "...")
        else:
            print("  asset_allocation_summary: None")

        fm = res.get("fund_manager")
        if fm:
            count_fm += 1
            print("  fund_manager:", fm)
        else:
            print("  fund_manager: None")

        exp_val = res.get("expense_ratio_percent")
        if isinstance(exp_val, (int, float)):
            count_exp += 1
            print("  expense_ratio_percent:", exp_val)
        else:
            print("  expense_ratio_percent: None")

        exit_load = res.get("exit_load")
        if exit_load:
            count_exit += 1
        print("  exit_load:", exit_load)

        plans = res.get("plans_and_options")
        if plans:
            print("  plans_and_options:", plans)

    print("\n=== Batch summary ===")
    processed = len(codes)
    print(f"Schemes processed: {processed}")
    print(f"Schemes with objective: {count_obj}")
    print(f"Schemes with asset_allocation_summary: {count_alloc}")
    print(f"Schemes with fund_manager: {count_fm}")
    print(f"Schemes with expense_ratio_percent: {count_exp}")
    print(f"Schemes with exit_load: {count_exit}")


if __name__ == "__main__":
    main()
