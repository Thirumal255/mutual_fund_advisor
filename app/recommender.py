# app/recommender.py
"""
Recommender module (Phase 1)
- Uses mftool.get_scheme_codes() to build a master list (code -> name)
- Provides mf_search(seed) which returns scheme_name -> scheme_code
- Caches scheme quotes with TTL
- Provides simple scoring, SIP projection
- Provides lightweight PDF text extraction and scheme candidate finder
"""

from typing import Tuple, List, Dict, Optional
from cachetools import TTLCache, cached
import numpy as np
import pandas as pd
import io
import re
import pdfplumber
import traceback

# mftool is the canonical source for AMFI data
from mftool import Mftool

# Globals
_mf: Optional[Mftool] = None
_quote_cache = TTLCache(maxsize=2000, ttl=600)  # 10-minute cache
_master_name_to_code: Optional[Dict[str, str]] = None  # normalized name -> code cache

# ------------------------------
# Initialization / Master list
# ------------------------------
def init_mftool():
    """Initialize the mftool client (call once at app startup)."""
    global _mf
    if _mf is None:
        _mf = Mftool()

def build_master_list_cache(force: bool = False) -> Dict[str, str]:
    """
    Build and cache a dict mapping normalized scheme_name -> scheme_code (string).
    Uses _mf.get_scheme_codes() which returns {code: name}.
    """
    global _master_name_to_code, _mf
    if _master_name_to_code is not None and not force:
        return _master_name_to_code

    _master_name_to_code = {}
    if _mf is None:
        # Not initialized
        return _master_name_to_code

    try:
        codes = _mf.get_scheme_codes()  # {code: name}
    except Exception as e:
        print(f"[build_master_list_cache] error fetching scheme codes: {e}")
        traceback.print_exc()
        return _master_name_to_code

    out: Dict[str, str] = {}
    for code, name in codes.items():
        if name and code:
            key = str(name).lower().strip()
            out[key] = str(code)
    _master_name_to_code = out
    return _master_name_to_code

# ------------------------------
# Search helper
# ------------------------------
def mf_search(seed: str, max_results: int = 200) -> Dict[str, str]:
    """
    Search the cached master list for names containing the seed (case-insensitive).
    Returns dict: scheme_name -> scheme_code
    """
    global _mf
    if not seed or not isinstance(seed, str):
        return {}

    if _mf is None:
        init_mftool()

    master = build_master_list_cache()
    if not master:
        # if master is empty, attempt to rebuild (maybe first-time)
        master = build_master_list_cache(force=True)

    seed_norm = seed.lower().strip()
    results: Dict[str, str] = {}

    # 1) Direct substring match against normalized names
    for name_norm, code in master.items():
        if seed_norm in name_norm:
            # retrieve original name from codes dictionary
            try:
                original_name = _mf.get_scheme_codes().get(code, name_norm)
            except Exception:
                original_name = name_norm
            results[original_name] = code
            if max_results and len(results) >= max_results:
                break

    # 2) Token based fallback if nothing found
    if not results and " " in seed_norm:
        tokens = [t for t in seed_norm.split() if len(t) >= 3]
        if tokens:
            for name_norm, code in master.items():
                if any(tok in name_norm for tok in tokens):
                    try:
                        original_name = _mf.get_scheme_codes().get(code, name_norm)
                    except Exception:
                        original_name = name_norm
                    results[original_name] = code
                    if max_results and len(results) >= max_results:
                        break

    return results

# ------------------------------
# Quote caching wrapper
# ------------------------------
@cached(_quote_cache)
def get_scheme_quote_cached(amfi_code: str) -> dict:
    """
    Return the quote dict for a given scheme code using mftool.
    Cached to reduce repeated network calls.
    """
    if _mf is None:
        init_mftool()
    try:
        return _mf.get_scheme_quote(amfi_code)
    except Exception as e:
        print(f"[get_scheme_quote_cached] error for code {amfi_code}: {e}")
        traceback.print_exc()
        return {}

# ------------------------------
# Sample funds (smoke test)
# ------------------------------
def list_sample_funds(n: int = 5) -> List[dict]:
    """
    Return a small list of sample funds using a broad seed like 'Large Cap'.
    Format: [{scheme_name, amfi_code, nav, last_updated}, ...]
    """
    try:
        # use mf_search to find matching schemes
        results = mf_search("Large Cap", max_results=100)
        out = []
        i = 0
        for name, code in results.items():
            if i >= n:
                break
            try:
                q = get_scheme_quote_cached(code)
                out.append({
                    "scheme_name": q.get("scheme_name") or name,
                    "amfi_code": code,
                    "nav": q.get("nav"),
                    "last_updated": q.get("last_updated")
                })
                i += 1
            except Exception:
                continue
        return out
    except Exception as e:
        print(f"[list_sample_funds] error: {e}")
        traceback.print_exc()
        return []

# ------------------------------
# Scoring & SIP projection
# ------------------------------
def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return np.nan

def score_scheme_from_quote(quote: dict, risk_profile: str) -> float:
    """
    Heuristic scoring using available quoted returns (5y/3y/1y), AUM and expense.
    Returns a float score (higher = better).
    """
    ret_keys = ['5yr', '5Y', '3yr', '3Y', '1yr', '1Y']
    returns = np.nan
    for k in ret_keys:
        v = quote.get(k)
        if v:
            try:
                returns = float(v)
                break
            except Exception:
                continue

    aum = _safe_float(quote.get('aum') or quote.get('AUM') or 0.0)
    expense = _safe_float(quote.get('expense_ratio') or quote.get('expense') or 0.0)
    aum_score = np.log1p(aum) if aum > 0 else 0.0

    if risk_profile == "low":
        w = {'ret': 0.45, 'aum': 0.35, 'exp': -0.2}
    elif risk_profile == "high":
        w = {'ret': 0.70, 'aum': 0.15, 'exp': -0.15}
    else:
        w = {'ret': 0.55, 'aum': 0.25, 'exp': -0.2}

    ret_val = returns if not np.isnan(returns) else 10.0
    score = w['ret'] * ret_val + w['aum'] * aum_score + w['exp'] * (expense if expense > 0 else 0.0)
    return float(score)

def sip_projection(monthly_sip: float, years: int, annual_return_pct: float) -> float:
    """
    Future value of monthly SIP with monthly compounding approximation.
    """
    r = annual_return_pct / 100.0 / 12.0
    n = years * 12
    if r == 0:
        return monthly_sip * n
    fv = monthly_sip * (((1 + r) ** n - 1) / r) * (1 + r)
    return float(fv)

# ------------------------------
# Recommend function
# ------------------------------
def recommend_funds_for_profile(profile: Dict, top_k: int = 5) -> Tuple[List[Dict], float]:
    """
    Main recommendation function used by FastAPI.
    profile: dict with keys monthly_sip, horizon_years, risk_profile, preferences (opt)
    Returns: (list of recommendation dicts, sip_projection_inr)
    """
    risk = profile.get('risk_profile', 'moderate')
    preferences = profile.get('preferences') or []

    mapping = {
        'low': ['Liquid', 'Short Duration', 'Conservative Hybrid'],
        'moderate': ['Large Cap', 'Hybrid', 'Flexi Cap', 'Multi Cap'],
        'high': ['Mid Cap', 'Small Cap', 'Sectoral', 'Thematic']
    }
    seeds = preferences if preferences else mapping.get(risk, ['Large Cap'])

    candidates: Dict[str, str] = {}
    # Primary collection via mf_search
    for s in seeds:
        try:
            results = mf_search(s, max_results=300)
            for name, code in results.items():
                candidates[name] = code
        except Exception as e:
            print(f"[recommend] mf_search error for seed '{s}': {e}")
            traceback.print_exc()
            continue

    # Fallback: broader seeds
    if not candidates:
        fallback_seeds = ['Equity', 'Fund', 'Mutual Fund', 'All']
        for s in fallback_seeds:
            try:
                results = mf_search(s, max_results=300)
                for name, code in results.items():
                    candidates[name] = code
                if candidates:
                    print(f"[recommend] found candidates using fallback seed '{s}'")
                    break
            except Exception as e:
                print(f"[recommend] fallback search error for '{s}': {e}")
                continue

    if not candidates:
        raise ValueError("No candidate funds found for given seeds/profile. Try broader 'preferences' values or check mftool connectivity.")

    # Fetch quotes and score
    ranked = []
    for name, code in list(candidates.items()):
        try:
            q = get_scheme_quote_cached(code)
            if not q:
                continue
            sc = score_scheme_from_quote(q, risk)
            nav = None
            try:
                nav = float(q.get('nav')) if q.get('nav') else None
            except Exception:
                nav = None
            ranked.append({
                'scheme_name': q.get('scheme_name') or name,
                'amfi_code': code,
                'category': q.get('type') or None,
                'score': sc,
                'nav': nav,
                'last_updated': q.get('last_updated')
            })
        except Exception as e:
            print(f"[recommend] error fetching quote for code {code}: {e}")
            traceback.print_exc()
            continue

    if not ranked:
        raise ValueError("No candidate funds found after fetching quotes. Check mftool/get_scheme_quote behavior.")

    df = pd.DataFrame(ranked).drop_duplicates(subset=['scheme_name']).sort_values('score', ascending=False).head(top_k)

    # Estimate avg_return from available quoted returns in top picks
    annual_returns: List[float] = []
    for _, row in df.iterrows():
        try:
            q = get_scheme_quote_cached(row['amfi_code'])
            for k in ['5yr', '5Y', '3yr', '3Y', '1yr', '1Y']:
                v = q.get(k)
                if v:
                    try:
                        annual_returns.append(float(v))
                        break
                    except Exception:
                        continue
        except Exception:
            continue

    avg_return = float(np.nanmean(annual_returns)) if len(annual_returns) > 0 else 10.0
    projection = sip_projection(profile.get('monthly_sip', 10000), profile.get('horizon_years', 5), avg_return)

    recommendations: List[Dict] = []
    for _, r in df.iterrows():
        recommendations.append({
            'scheme_name': r['scheme_name'],
            'amfi_code': r['amfi_code'],
            'category': r.get('category'),
            'score': round(float(r['score']), 4),
            'nav': round(r['nav'], 4) if r['nav'] is not None else None,
            'last_updated': r.get('last_updated')
        })

    return recommendations, round(projection, 2)

# ------------------------------
# PDF parsing helpers
# ------------------------------
def extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    """
    Extract text from PDF bytes using pdfplumber (best-effort).
    """
    text_chunks: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text_chunks.append(page_text)
    except Exception:
        # fallback: attempt to decode as utf-8 text
        try:
            text_chunks.append(file_bytes.decode('utf-8', errors='ignore'))
        except Exception:
            pass
    return "\n".join(text_chunks)

def find_scheme_candidates_from_text(text: str, max_candidates: int = 50) -> Dict[str, str]:
    """
    Naive extraction: split text into tokens/lines, search each token via mf_search,
    and return mapping scheme_name -> amfi_code.
    """
    if not text or not isinstance(text, str):
        return {}
    norm = re.sub(r'[^A-Za-z0-9\-\&\.\,\/\s]', ' ', text)
    lines = [line.strip() for line in re.split(r'[\n\r,;]+', norm) if len(line.strip()) >= 3]
    candidates: Dict[str, str] = {}
    checked = 0
    for token in lines:
        if checked >= 500 or len(candidates) >= max_candidates:
            break
        checked += 1
        try:
            results = mf_search(token, max_results=20)
            for name, code in results.items():
                if name not in candidates:
                    candidates[name] = code
                if len(candidates) >= max_candidates:
                    break
        except Exception:
            continue
    return candidates
