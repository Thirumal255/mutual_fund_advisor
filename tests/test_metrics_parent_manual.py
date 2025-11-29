# tests/test_metrics_parent_manual.py
"""
Manual test for data/metrics_parent_reps.json

Usage:
  python -m tests.test_metrics_parent_manual

What it does:
  - Loads data/metrics_parent_reps.json
  - Prints counts and a few sample entries
"""

import os
import sys
import json
from pprint import pprint

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

DATA_DIR = os.path.join(ROOT, "data")
METRICS_PARENT_FILE = os.path.join(DATA_DIR, "metrics_parent_reps.json")


def main():
    if not os.path.exists(METRICS_PARENT_FILE):
        print("metrics_parent_reps.json not found. Run:")
        print("  python scripts/build_parent_metrics.py")
        return

    with open(METRICS_PARENT_FILE, "r", encoding="utf-8") as f:
        parent_metrics = json.load(f)

    total_parents = len(parent_metrics)
    print(f"Total parents with metrics: {total_parents}")

    # Basic coverage check
    cagr_ok = 0
    exp_ok = 0
    for pk, info in parent_metrics.items():
        m = info.get("metrics", {})
        if m.get("cagr") is not None:
            cagr_ok += 1
        if m.get("expense_ratio_percent") is not None:
            exp_ok += 1

    print(f"Parents with non-null CAGR: {cagr_ok}")
    print(f"Parents with non-null expense_ratio_percent: {exp_ok}")

    # Show a few sample parents
    print("\nSample parents (up to 5):")
    count = 0
    for pk, info in parent_metrics.items():
        print("Parent key:", pk)
        print("  rep_code:", info.get("rep_code"))
        print("  rep_name:", info.get("rep_name"))
        m = info.get("metrics", {})
        print("  data_points:", m.get("data_points"),
              " cagr:", m.get("cagr"),
              " rolling_3y:", m.get("rolling_3y"),
              " sharpe:", m.get("sharpe"),
              " expense:", m.get("expense_ratio_percent"),
              " exit_load:", m.get("exit_load_percent"))
        print("----")
        count += 1
        if count >= 5:
            break

    print("\nTest complete.")


if __name__ == "__main__":
    main()
