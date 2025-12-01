"""
tests/test_scheme_info_manual.py

Manual test for app.scheme_info.

Run from project root:

  python -m tests.test_scheme_info_manual

You can customize the PARENT_QUERIES list with typical scheme names.
"""

import os
import sys
from pprint import pprint

# Ensure project root on path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.scheme_info import (  # type: ignore
    search_parent_keys,
    get_parent_overview,
)


# Try some parents we know exist (adjust as needed)
PARENT_QUERIES = [
    "navi large & midcap",      # should match parent for 135677
    "sbi gold fund",            # 119788
    "parag parikh flexi cap",   # 122639
    "quantum nifty 50 etf",     # 108479
    "hdfc corporate bond fund", # 118987
    "lic mf multi cap",         # 150659
]


def test_single_parent(query: str):
    print("\n==============================")
    print(f"Query: {query}")
    matches = search_parent_keys(query, limit=3)
    print("Matches:", matches)
    if not matches:
        print("  -> No parent found.")
        return

    parent_key = matches[0]
    print(f"Using parent_key: {parent_key}")

    info = get_parent_overview(parent_key)
    if not info:
        print("  -> No info returned.")
        return

    print("\nSummary:")
    pprint(info["summary"])

    print("\nRepresentative child:")
    print("  rep_code:", info.get("rep_code"))
    print("  rep_name:", info.get("rep_name"))

    print("\nChildren (up to 5):")
    for ch in info["children"][:5]:
        print("  -", ch["scheme_code"], "-", ch["scheme_name"])

    print("\nMetrics (keys):", list(info["metrics"].keys()))


def main():
    for q in PARENT_QUERIES:
        test_single_parent(q)

    print("\nAll tests done.\n")


if __name__ == "__main__":
    main()
