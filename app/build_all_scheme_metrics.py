#!/usr/bin/env python3
"""
scripts/build_all_scheme_metrics.py

Compute metrics for every scheme code present in data/parent_masterlist.json
(or any scheme list you provide). Saves per-code metrics to data/metrics_by_code.json.

Usage:
  python scripts/build_all_scheme_metrics.py --workers 8 --rf 0.04 --limit 0
"""
import os
import json
import argparse
from typing import List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# Adjust import path if needed; this expects app.metrics.compute_metrics_batch to exist
try:
    # When running as module (recommended)
    from app.metrics import compute_metrics_batch
except Exception:
    # fallback: try importing metrics directly
    from metrics import compute_metrics_batch  # type: ignore

BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARENT_MASTERLIST_PATH = os.path.join(DATA_DIR, "parent_masterlist.json")
OUT_PATH = os.path.join(DATA_DIR, "metrics_by_code.json")

def collect_all_scheme_codes(parent_master_path: str) -> List[str]:
    try:
        with open(parent_master_path, "r", encoding="utf-8") as f:
            pm = json.load(f)
    except Exception:
        return []

    codes: Set[str] = set()

    # possible shapes
    if isinstance(pm, dict) and "parent_groups" in pm and isinstance(pm["parent_groups"], dict):
        parent_groups = pm["parent_groups"]
    elif isinstance(pm, dict):
        parent_groups = {k: v for k, v in pm.items() if k != "meta"}
    else:
        parent_groups = {}

    for parent_key, val in parent_groups.items():
        # val may be list or dict
        if isinstance(val, list):
            entries = val
        elif isinstance(val, dict) and "children" in val and isinstance(val["children"], list):
            entries = val["children"]
        else:
            # attempt common alternative keys
            entries = []
            if isinstance(val, dict):
                for k in ("children", "schemes", "child_schemes", "members", "list"):
                    if k in val and isinstance(val[k], list):
                        entries = val[k]
                        break

        for e in entries:
            if isinstance(e, dict):
                code = str(e.get("scheme_code") or e.get("code") or e.get("schemeCode") or "").strip()
                if code:
                    codes.add(code)
            elif isinstance(e, (list, tuple)) and len(e) >= 1:
                codes.add(str(e[0]).strip())
            else:
                codes.add(str(e).strip())

        # also add rep_code if present in parent metadata
        if isinstance(val, dict):
            rep = val.get("rep") or val.get("rep_code") or (val.get("rep_info") or {}).get("rep_code")
            if rep:
                codes.add(str(rep).strip())

    # Remove empty
    codes = {c for c in codes if c and c.isdigit()}
    return sorted(list(codes))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-master", default=PARENT_MASTERLIST_PATH)
    parser.add_argument("--out", default=OUT_PATH)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rf", type=float, default=0.04, help="risk free rate")
    parser.add_argument("--limit", type=int, default=0, help="process only first N codes (0 = all)")
    args = parser.parse_args()

    codes = collect_all_scheme_codes(args.parent_master)
    if args.limit and args.limit > 0:
        codes = codes[: args.limit]

    print(f"[build_all_scheme_metrics] Will compute metrics for {len(codes)} codes (workers={args.workers})")
    # compute_metrics_batch should return dict: code -> metrics_dict
    metrics_by_code = compute_metrics_batch(codes, max_workers=args.workers, risk_free_rate=args.rf)

    # Save
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(metrics_by_code or {}, f, indent=2, ensure_ascii=False)

    print(f"[build_all_scheme_metrics] Saved metrics_by_code to {args.out}")

if __name__ == "__main__":
    main()
