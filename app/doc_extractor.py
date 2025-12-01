"""
doc_extractor.py
------------------------------------------
Section-based SID extractor (Phase 3)

Structure assumed:

PART 1:
  "HIGHLIGHTS / SUMMARY OF THE SCHEME" (or similar)
    - Name of the Scheme
    - Category of the Scheme
    - Type of the Scheme
    - Investment Objective
    - Benchmark
    - Plans and Options
    - Load Structure (exit load)

PART 2:
  - "Asset Allocation" / "HOW WILL THE SCHEME ALLOCATE ITS ASSETS"
  - "WHO MANAGES THE SCHEME" / "Fund Manager"

PART 3:
  - "ANNUAL SCHEME RECURRING EXPENSES" (expense ratio)
"""

import os
import json
from typing import List, Dict, Any, Optional

from .doc_loader import extract_paragraphs

# -------------------------
# Paths / constants
# -------------------------

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
SCHEME_DOCS_DIR = os.path.join(DATA_DIR, "scheme_docs")
os.makedirs(SCHEME_DOCS_DIR, exist_ok=True)


def log(msg: str) -> None:
    print(f"[doc_extractor] {msg}")


# -------------------------
# API key & client
# -------------------------

def load_api_key() -> Optional[str]:
    key = os.environ.get("OPENAI_API_KEY")
    if key:
        return key.strip()

    for candidate in (
        os.path.join(DATA_DIR, ".openai_api_key"),
        os.path.join(BASE_DIR, ".openai_api_key"),
    ):
        if os.path.exists(candidate):
            try:
                with open(candidate, "r", encoding="utf-8") as f:
                    return f.read().strip()
            except Exception:
                pass
    return None


OPENAI_API_KEY = load_api_key()

_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI  # type: ignore
        _client = OpenAI(api_key=OPENAI_API_KEY)
        log("OpenAI client initialized (gpt-4o-mini).")
    except Exception as e:
        log(f"Failed to init OpenAI client: {e}")
else:
    log("⚠ No OPENAI_API_KEY found. LLM extraction disabled (regex-only / empty).")


# -------------------------
# Utility: safe JSON parse
# -------------------------

def safe_json(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        # try to extract first {...}
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            snippet = text[start:end + 1]
            try:
                return json.loads(snippet)
            except Exception:
                return None
    return None


# -------------------------
# Low-level LLM caller
# -------------------------

def call_llm(block: str, prompt: str, model: str = "gpt-4o-mini") -> Optional[Dict[str, Any]]:
    if not _client:
        return None
    try:
        resp = _client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": block},
            ],
            max_completion_tokens=300,
        )
        raw = resp.choices[0].message.content
        return safe_json(raw)
    except Exception as e:
        log(f"LLM error: {e}")
        return None


# -------------------------
# Section block finder
# -------------------------

def find_section_block(
    paragraphs: List[Dict[str, str]],
    title_keywords: List[str],
    max_paras_after: int = 15,
) -> Optional[str]:
    """
    Find a paragraph that looks like a real section heading.

    Strategy:
      - Scan all paragraphs.
      - Record the first match (first paragraph whose text contains any keyword).
      - If we find a SECOND match later:
          -> use the SECOND match as the heading (to skip index/TOC style references).
      - If there is only one match in the whole document:
          -> fall back to using the first match.

    Then return that paragraph + up to max_paras_after following paragraphs.
    """
    first_match_idx: Optional[int] = None
    heading_idx: Optional[int] = None

    for idx, item in enumerate(paragraphs):
        raw = item.get("text", "")
        t = raw.lower()

        if any(kw.lower() in t for kw in title_keywords):
            if first_match_idx is None:
                first_match_idx = idx
                continue
            else:
                heading_idx = idx
                break

    if heading_idx is None:
        heading_idx = first_match_idx

    if heading_idx is None:
        return None

    block_paras: List[str] = []
    for j in range(heading_idx, min(heading_idx + 1 + max_paras_after, len(paragraphs))):
        block_paras.append(paragraphs[j].get("text", ""))

    return "\n".join(block_paras)


# ============================================================
# PROMPTS
# ============================================================

PROMPT_HIGHLIGHTS = """
You are reading the 'HIGHLIGHTS / SUMMARY OF THE SCHEME' section from an Indian mutual fund SID.

This section usually has a table or key-value pairs with rows like:
- Name of the Scheme
- Category of the Scheme
- Type of the Scheme
- Investment Objective
- Benchmark
- Plans and Options
- Load Structure

Extract and return exactly this JSON:

{
  "scheme_name": string | null,
  "category": string | null,
  "scheme_type": string | null,
  "fund_objective_summary": string | null,
  "declared_benchmark": string | null,
  "plans_and_options": string[] | null,
  "exit_load": {
    "type": "nil" | "single" | "rules" | "unknown" | null,
    "rules": [
      {
        "tenure_days": number | null,
        "load_percent": number
      }
    ] | null
  }
}
"""

PROMPT_ASSET_ALLOCATION = """
You are reading the 'Asset Allocation' or 'How will the scheme allocate its assets' section of an Indian mutual fund SID.

Summarize the asset allocation pattern in 1–3 concise sentences, describing major asset classes and their typical percentage ranges.

Return exactly:

{
  "asset_allocation_summary": string | null
}
"""

PROMPT_FUND_MANAGER = """
You are reading the 'Who manages the scheme' or 'Fund Manager' section of an Indian mutual fund SID.

Extract the fund manager names for this scheme, as a single comma-separated string.

Return exactly:

{
  "fund_manager": string | null
}
"""

PROMPT_EXPENSES = """
You are reading the 'ANNUAL SCHEME RECURRING EXPENSES' section of an Indian mutual fund SID.

Extract the total expense ratio (TER) or maximum estimated recurring expenses as a percentage of daily net assets for the scheme (not AMC-wide ranges).

Return exactly:

{
  "expense_ratio_percent": number | null
}
"""


# ============================================================
# SECTION EXTRACTORS (with debug logging)
# ============================================================

def extract_highlights(paragraphs: List[Dict[str, str]], model: str) -> Dict[str, Any]:
    title_keywords = [
        "highlights / summary of the scheme",
        "highlights/summary of the scheme",
        "highlights / summary",
        "summary of the scheme",
        "highlights of the scheme",
        "part i. highlights/summary of the scheme",
        "part i - highlights/summary of the scheme",
    ]
    block = find_section_block(paragraphs, title_keywords, max_paras_after=20)
    if not block:
        log("Highlights section NOT found.")
        return {}

    log("Highlights section found. First 200 chars:")
    log(block[:200].replace("\n", " "))

    res = call_llm(block, PROMPT_HIGHLIGHTS, model=model)
    if not res:
        log("Highlights LLM returned no/invalid JSON.")
    else:
        log(f"Highlights LLM keys: {list(res.keys())}")
    return res or {}


def extract_asset_allocation(paragraphs: List[Dict[str, str]], model: str) -> Dict[str, Any]:
    title_keywords = [
        "how will the scheme allocate its assets",
        "asset allocation",
        "asset allocation pattern",
        "asset allocation (% of total assets)",
    ]
    block = find_section_block(paragraphs, title_keywords, max_paras_after=20)
    if not block:
        log("Asset Allocation section NOT found.")
        return {}

    log("Asset Allocation section found. First 200 chars:")
    log(block[:200].replace("\n", " "))

    res = call_llm(block, PROMPT_ASSET_ALLOCATION, model=model)
    if not res:
        log("Asset Allocation LLM returned no/invalid JSON.")
    else:
        log(f"Asset Allocation LLM keys: {list(res.keys())}")
    return res or {}


def extract_fund_manager(paragraphs: List[Dict[str, str]], model: str) -> Dict[str, Any]:
    title_keywords = [
        "who manages the scheme",
        "who manages this scheme",
        "fund manager",
        "fund manager(s)",
        "fund management",
        "information about the fund manager",
    ]
    block = find_section_block(paragraphs, title_keywords, max_paras_after=15)
    if not block:
        log("Fund Manager section NOT found.")
        return {}

    log("Fund Manager section found. First 200 chars:")
    log(block[:200].replace("\n", " "))

    res = call_llm(block, PROMPT_FUND_MANAGER, model=model)
    if not res:
        log("Fund Manager LLM returned no/invalid JSON.")
    else:
        log(f"Fund Manager LLM keys: {list(res.keys())}")
    return res or {}


def extract_expense_ratio(paragraphs: List[Dict[str, str]], model: str) -> Dict[str, Any]:
    title_keywords = [
        "annual scheme recurring expenses",
        "scheme recurring expenses",
        "recurring expenses",
    ]
    block = find_section_block(paragraphs, title_keywords, max_paras_after=20)
    if not block:
        log("Expense section NOT found.")
        return {}

    log("Expense section found. First 200 chars:")
    log(block[:200].replace("\n", " "))

    res = call_llm(block, PROMPT_EXPENSES, model=model)
    if not res:
        log("Expense LLM returned no/invalid JSON.")
    else:
        log(f"Expense LLM keys: {list(res.keys())}")
    return res or {}


# ============================================================
# MAIN ENTRY
# ============================================================

def parse_scheme_pdf(pdf_path: str, scheme_code: str, model: str = "gpt-4o-mini") -> Dict[str, Any]:
    pdf_path = pdf_path.replace("\\", "/")
    scheme_code = str(scheme_code).strip()

    log(f"Parsing SID → {pdf_path}")
    paragraphs = extract_paragraphs(pdf_path)
    log(f"Paragraphs extracted: {len(paragraphs)}")

    result: Dict[str, Any] = {
        "scheme_code": scheme_code,
        "scheme_name": None,
        "category": None,
        "scheme_type": None,
        "fund_objective_summary": None,
        "declared_benchmark": None,
        "plans_and_options": None,
        "exit_load": None,
        "asset_allocation_summary": None,
        "fund_manager": None,
        "expense_ratio_percent": None,
    }

    if not _client:
        log("No LLM client; returning empty/default result.")
    else:
        # Highlights / Summary
        highlights = extract_highlights(paragraphs, model)
        if isinstance(highlights, dict):
            for k, v in highlights.items():
                if v is not None and result.get(k) in (None, "", [], {}):
                    result[k] = v

        # Asset Allocation
        alloc = extract_asset_allocation(paragraphs, model)
        if isinstance(alloc, dict) and alloc.get("asset_allocation_summary"):
            result["asset_allocation_summary"] = alloc["asset_allocation_summary"]

        # Fund Manager
        fm = extract_fund_manager(paragraphs, model)
        if isinstance(fm, dict) and fm.get("fund_manager"):
            result["fund_manager"] = fm["fund_manager"]

        # Expense Ratio
        exp = extract_expense_ratio(paragraphs, model)
        if isinstance(exp, dict) and exp.get("expense_ratio_percent") is not None:
            result["expense_ratio_percent"] = exp["expense_ratio_percent"]

    out_path = os.path.join(SCHEME_DOCS_DIR, f"{scheme_code}.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        log(f"Saved {out_path}")
    except Exception as e:
        log(f"Failed to save {out_path}: {e}")

    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python -m app.doc_extractor <pdf_path> <scheme_code>")
        raise SystemExit(1)
    pdf = sys.argv[1]
    code = sys.argv[2]
    res = parse_scheme_pdf(pdf, code)
    print(json.dumps(res, indent=2, ensure_ascii=False))
