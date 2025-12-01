# app/doc_index.py
"""
Helper to load scheme_code -> SID PDF mapping.

We assume a single SID per representative scheme code (rep_code).
"""

import os
import json
from typing import Dict

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
SID_INDEX_FILE = os.path.join(DATA_DIR, "sid_index.json")


def load_sid_index() -> Dict[str, str]:
    """
    Load mapping of scheme_code -> SID PDF path from sid_index.json.

    File format:
      {
        "120700": "data/sid_pdfs/120700_SID.pdf",
        "119550": "data/sid_pdfs/119550_SID.pdf"
      }
    """
    if not os.path.exists(SID_INDEX_FILE):
        raise FileNotFoundError(
            f"SID index file not found: {SID_INDEX_FILE}. "
            "Create it with scheme_code -> SID PDF path mapping."
        )

    with open(SID_INDEX_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("sid_index.json must contain a JSON object (dict).")

    out: Dict[str, str] = {}
    for code, path in data.items():
        code_str = str(code).strip()
        out[code_str] = path
    return out
