# app/build_parent_metrics.py
"""
Build metrics for all parent schemes using their representative child code.

Usage (from project root):
  python scripts/build_parent_metrics.py

What it does:
  - Loads parent_reps from data/parent_masterlist.json (via PARENT_MASTER_FILE)
  - Extracts representative scheme codes (rep_code) per parent
  - Computes metrics for each unique rep_code (using compute_metrics_batch)
  - Saves per-parent metrics to data/metrics_parent_reps.json:

    {
      "parent_key": {
        "rep_code": "120701",
        "rep_name": "XYZ Flexicap Fund - Direct Plan - Growth",
        "rep_reason": "direct_growth",
        "rep_reason_score": 100.0,
        "metrics": { ... compute_metrics_for_code output ... }
      },
      ...
    }
"""

import os
import sys
import json
import time
from pprint import pprint

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.masterlist import build_all_exact, PARENT_MASTER_FILE
from app.metrics import compute_metrics_batch

DATA_DIR = os.path.join(ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

METRICS_PARENT_FILE = os.path.join(DATA_DIR, "metrics_parent_reps.json")


def load_parent_reps():
    """Load parent_reps from persisted parent_masterlist, or rebuild if missing."""
    if os.path.exists(PARENT_MASTER_FILE):
        try:
            print(f"[build_parent_metrics] Loading parent_masterlist from {PARENT_MASTER_FILE}")
            payload = json.load(open(PARENT_MASTER_FILE, "r", encoding="utf-8"))
            parent_reps = payload.get("parent_reps", {})
            if isinstance(parent_reps, dict) and parent_reps:
                return parent_reps
        except Exception as e:
            print("[build_parent_metrics] Failed to load PARENT_MASTER_FILE:", e)

    print("[build_parent_metrics] parent_masterlist missing or empty — rebuilding via build_all_exact()...")
    _, code_to_name, parent_groups, parent_reps = build_all_exact(force_master=False)
    return parent_reps


def build_parent_metrics(limit: int = None,
                         max_workers: int = 8,
                         risk_free_rate: float = 0.06) -> dict:
    """
    Core function:
      - loads parent_reps
      - computes metrics for representative codes
      - returns parent_metrics dict
    """
    parent_reps = load_parent_reps()
    parent_keys = list(parent_reps.keys())
    print(f"[build_parent_metrics] Total parent schemes: {len(parent_keys)}")

    if limit is not None:
        parent_keys = parent_keys[:limit]
        print(f"[build_parent_metrics] Limiting to first {len(parent_keys)} parents for this run...")

    # Collect unique representative codes
    rep_codes = []
    rep_info = {}  # code -> (parent_keys list)
    for pk in parent_keys:
        rep = parent_reps.get(pk)
        if not rep or not rep[0]:
            continue
        code = str(rep[0])
        if code not in rep_codes:
            rep_codes.append(code)
            rep_info[code] = []
        rep_info[code].append(pk)

    print(f"[build_parent_metrics] Unique representative codes: {len(rep_codes)}")

    # Compute metrics in batch
    print("[build_parent_metrics] Computing metrics for representative codes...")
    t0 = time.time()
    metrics_by_code = compute_metrics_batch(
        rep_codes,
        max_workers=max_workers,
        risk_free_rate=risk_free_rate
    )
    t1 = time.time()
    print(f"[build_parent_metrics] Metrics computed in {t1 - t0:.1f}s")

    # Build parent-level structure
    parent_metrics = {}
    missing_count = 0
    for pk in parent_keys:
        rep = parent_reps.get(pk)
        if not rep or not rep[0]:
            continue
        rep_code = str(rep[0])
        rep_name = rep[1] if len(rep) > 1 else None
        rep_reason = rep[2] if len(rep) > 2 else None
        rep_reason_score = rep[3] if len(rep) > 3 else None

        m = metrics_by_code.get(rep_code)
        if not m:
            missing_count += 1
            continue

        parent_metrics[pk] = {
            "rep_code": rep_code,
            "rep_name": rep_name,
            "rep_reason": rep_reason,
            "rep_reason_score": rep_reason_score,
            "metrics": m,
        }

    print(f"[build_parent_metrics] Parent metrics built for {len(parent_metrics)} parents "
          f"(missing metrics for {missing_count}).")

    return parent_metrics


def save_parent_metrics(parent_metrics: dict, path: str = METRICS_PARENT_FILE):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(parent_metrics, f, indent=2, ensure_ascii=False)
        print(f"[build_parent_metrics] Saved parent metrics to: {path}")
    except Exception as e:
        print("[build_parent_metrics] Failed to save metrics file:", e)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Build metrics for parent schemes using representative child codes.")
    p.add_argument("--limit", type=int, default=None,
                   help="Optional: limit number of parent schemes processed (for quick testing).")
    p.add_argument("--workers", type=int, default=8, help="Number of worker threads for metric computation.")
    p.add_argument("--rf", type=float, default=0.06, help="Risk-free annual rate for Sharpe/Sortino.")
    args = p.parse_args()

    parent_metrics = build_parent_metrics(limit=args.limit,
                                          max_workers=args.workers,
                                          risk_free_rate=args.rf)
    save_parent_metrics(parent_metrics, METRICS_PARENT_FILE)

    # Print a tiny summary
    print("\nSample parents (up to 3):")
    count = 0
    for pk, info in parent_metrics.items():
        print("Parent key:", pk)
        print("  rep_code:", info.get("rep_code"))
        print("  rep_name:", info.get("rep_name"))
        m = info.get("metrics", {})
        print("  cagr:", m.get("cagr"), "  vol:", m.get("volatility_annual"),
              "  sharpe:", m.get("sharpe"), "  expense:", m.get("expense_ratio_percent"))
        print("----")
        count += 1
        if count >= 3:
            break
    print("\nDone.")


if __name__ == "__main__":
    main()
