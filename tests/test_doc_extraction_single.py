"""
tests/test_doc_extraction_single.py

Manual test for a single SID PDF.

Usage (from project root):

  python -m tests.test_doc_extraction_single data/sid_pdfs/135677_SID.pdf 135677
"""

import os
import sys
import json

# Ensure project root is on sys.path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.doc_extractor import parse_scheme_pdf, SCHEME_DOCS_DIR  # type: ignore


def main():
    if len(sys.argv) < 3:
        print("Usage: python -m tests.test_doc_extraction_single <pdf_path> <scheme_code>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    scheme_code = sys.argv[2]

    print("PDF path:", pdf_path)
    print("Scheme code:", scheme_code)

    res = parse_scheme_pdf(pdf_path, scheme_code)

    print("\n=== Extraction summary keys ===")
    print(list(res.keys()))

    print("\n=== Key fields ===")
    print("scheme_name:", res.get("scheme_name"))
    print("category:", res.get("category"))
    print("scheme_type:", res.get("scheme_type"))
    print("declared_benchmark:", res.get("declared_benchmark"))
    print("fund_objective_summary:", res.get("fund_objective_summary"))
    print("asset_allocation_summary:", res.get("asset_allocation_summary"))
    print("fund_manager:", res.get("fund_manager"))
    print("plans_and_options:", res.get("plans_and_options"))
    print("expense_ratio_percent:", res.get("expense_ratio_percent"))
    print("exit_load:", res.get("exit_load"))

    out_path = os.path.join(SCHEME_DOCS_DIR, f"{scheme_code}.json")
    print("\nOutput JSON (if saved):", out_path)
    print("File exists ✓" if os.path.exists(out_path) else "File NOT found ✗")


if __name__ == "__main__":
    main()
