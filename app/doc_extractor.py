# app/doc_extractor.py
"""
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
import sys
import json
import argparse
import time
import importlib
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------
# Robust import of extract_paragraphs from doc_loader
# Try (in order):
#  1) relative import (works when run as module)
#  2) absolute import app.doc_loader (works when package installed / PYTHONPATH has repo root)
#  3) direct import by adding the app/ dir to sys.path and importing doc_loader as a plain module
# -------------------------
try:
    # Preferred when running as module: python -m app.doc_extractor
    from .doc_loader import extract_paragraphs  # type: ignore
except Exception:
    try:
        # Try absolute package import (works if repo root is on PYTHONPATH)
        from app.doc_loader import extract_paragraphs  # type: ignore
    except Exception:
        # Final fallback: import doc_loader from same directory by adding app/ to sys.path
        # Compute the directory containing this file (app/)
        _app_dir = os.path.dirname(__file__)
        if _app_dir not in sys.path:
            sys.path.insert(0, _app_dir)
        try:
            _mod = importlib.import_module("doc_loader")
            extract_paragraphs = getattr(_mod, "extract_paragraphs")
        except Exception as e:
            # give a clear error with details
            raise ImportError(
                "Failed to import extract_paragraphs from doc_loader via relative, absolute, and fallback imports. "
                f"Details: {e}"
            ) from e

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


# -------------------------
# Helper: mtime checks and batch processing (timestamp-aware)
# -------------------------

def _pdf_or_json_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


SID_INDEX_PATH_DEFAULT = os.path.join(DATA_DIR, "sid_index.json")


def process_sid_index(
    index_path: str = SID_INDEX_PATH_DEFAULT,
    force: bool = False,
    limit: Optional[int] = None,
    workers: int = 1,
    model: str = "gpt-4o-mini",
):
    """
    Load data/sid_index.json and run parse_scheme_pdf for each mapping.

    Extraction decision logic:
      - If JSON output does not exist => extract.
      - If JSON exists, compare mtimes:
          if pdf_mtime > json_mtime => extract (pdf is newer)
          else => skip
      - --force overrides and extracts regardless.
    """
    if not os.path.exists(index_path):
        log(f"SID index not found: {index_path}")
        return {"processed": 0, "skipped": 0, "failed": 0}

    with open(index_path, "r", encoding="utf-8") as f:
        idx = json.load(f)

    items = list(idx.items())
    total = len(items)
    if limit and limit > 0:
        items = items[:limit]
        log(f"Limiting to first {len(items)} entries (limit={limit})")

    processed = 0
    skipped = 0
    failed = 0
    start = time.time()

    def _needs_extraction(code: str, pdf_path: str) -> bool:
        """
        Return True if we should extract:
          - force == True
          - JSON missing
          - PDF mtime > JSON mtime
        """
        if force:
            return True
        out_path = os.path.join(SCHEME_DOCS_DIR, f"{str(code).strip()}.json")
        if not os.path.exists(out_path):
            return True
        pdf_m = _pdf_or_json_mtime(pdf_path)
        json_m = _pdf_or_json_mtime(out_path)
        # If we can't stat pdf, be conservative (attempt extraction)
        if pdf_m is None:
            return True
        # If json mtime missing treat as extract
        if json_m is None:
            return True
        # Extract only if pdf modified after json
        return pdf_m > json_m

    # Sequential processing
    if workers and int(workers) <= 1:
        log(f"Processing {len(items)} entries sequentially.")
        for code, pdf in items:
            try:
                if not _needs_extraction(code, pdf):
                    skipped += 1
                else:
                    parse_scheme_pdf(pdf, code, model=model)
                    processed += 1
            except Exception as e:
                log(f"Failed {code} ({pdf}): {e}")
                failed += 1
            elapsed = time.time() - start
            # minimal progress line
            log(f"Progress: {processed}/{len(items)} processed, {skipped} skipped, {failed} failed. Elapsed: {int(elapsed)}s")
    else:
        # Parallel processing with thread pool
        log(f"Processing {len(items)} entries with {workers} workers (parallel).")
        # Submit only tasks that need extraction; count skipped separately
        to_submit = []
        for code, pdf in items:
            if _needs_extraction(code, pdf):
                to_submit.append((code, pdf))
            else:
                skipped += 1

        if not to_submit:
            total_elapsed = time.time() - start
            log(f"Nothing to do. Total entries: {total}. Processed: {processed}. Skipped: {skipped}. Failed: {failed}. Total time: {int(total_elapsed)}s")
            return {"processed": processed, "skipped": skipped, "failed": failed, "elapsed_s": int(total_elapsed)}

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(parse_scheme_pdf, pdf, code, model): (code, pdf) for (code, pdf) in to_submit}
            for fut in as_completed(futures):
                code, pdf = futures[fut]
                try:
                    fut.result()
                    processed += 1
                except Exception as e:
                    log(f"Failed {code} ({pdf}): {e}")
                    failed += 1
                elapsed = time.time() - start
                # minimal progress line
                log(f"Progress: {processed}/{len(items)} processed, {skipped} skipped, {failed} failed. Elapsed: {int(elapsed)}s")

    total_elapsed = time.time() - start
    log(f"Done. Total entries: {total}. Processed: {processed}. Skipped: {skipped}. Failed: {failed}. Total time: {int(total_elapsed)}s")
    return {"processed": processed, "skipped": skipped, "failed": failed, "elapsed_s": int(total_elapsed)}


# -------------------------
# CLI: process-all or single-file modes
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SID extractor / batch processor")
    parser.add_argument("pdf_path", nargs="?", help="Path to a single SID PDF (optional)")
    parser.add_argument("scheme_code", nargs="?", help="Scheme code for single PDF (optional)")
    parser.add_argument("--process-all", dest="process_all", action="store_true", help="Process all entries in data/sid_index.json")
    parser.add_argument("--index", default=SID_INDEX_PATH_DEFAULT, help="Path to sid_index.json")
    parser.add_argument("--force", action="store_true", help="Overwrite / re-extract regardless of timestamps")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of entries to process")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (1 = sequential)")
    parser.add_argument("--model", default="gpt-4o-mini", help="LLM model to use (if available)")
    args = parser.parse_args()

    if args.process_all:
        process_sid_index(index_path=args.index, force=args.force, limit=args.limit, workers=args.workers, model=args.model)
    else:
        # single-file mode
        if not args.pdf_path or not args.scheme_code:
            print("Usage: python -m app.doc_extractor <pdf_path> <scheme_code>    OR")
            print("       python -m app.doc_extractor --process-all [--index data/sid_index.json] [--force] [--workers 4]")
            raise SystemExit(1)
        parse_scheme_pdf(args.pdf_path, args.scheme_code, model=args.model)
