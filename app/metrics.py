# app/metrics.py
"""
Metrics Engine (Phase 2)

✔ Fetch historical NAV via mftool.get_scheme_historical_nav
✔ Compute performance metrics (CAGR, rolling returns)
✔ Compute risk metrics (volatility, max drawdown, Sharpe, Sortino)
✔ Fetch expense ratio / exit load / AUM from mftool

Designed to be used on representative child codes (one per parent scheme).

Benchmark-based metrics (beta, tracking error) are LEFT as placeholders for now.
"""

import os
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd
from mftool import Mftool

# ---------------------------
# CONFIG
# ---------------------------
RISK_FREE_RATE_DEFAULT = 0.06  # 6% annual
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
NAV_CACHE_DIR = os.path.join(DATA_DIR, "nav_cache")
os.makedirs(NAV_CACHE_DIR, exist_ok=True)

CACHE_MAX_AGE_DAYS = 7

# ---------------------------
# Logging setup for this module
# ---------------------------
logger = logging.getLogger("app.metrics")
if not logger.handlers:
    # configure default handler so importers see logs even if logging not configured
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)


# =====================================================
# Helpers
# =====================================================
def _log(msg: str) -> None:
    logger.info(msg)


def _nav_cache_path(code: str) -> str:
    safe = str(code).strip()
    return os.path.join(NAV_CACHE_DIR, f"{safe}.parquet")


def _cache_is_fresh(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        age_days = (datetime.now() - mtime).days
        return age_days <= CACHE_MAX_AGE_DAYS
    except Exception:
        return False


# =====================================================
# NAV parsing helpers for mftool
# =====================================================
def _nav_from_dataframe(df: pd.DataFrame) -> pd.Series:
    """
    Normalize DataFrame returned by:
      mf.get_scheme_historical_nav(code, as_Dataframe=True)
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    if "nav" not in df.columns:
        # fallback: first column as nav
        if df.columns.size > 0:
            df = df.rename(columns={df.columns[0]: "nav"})
        else:
            return pd.Series(dtype=float)

    # index is date-like strings
    idx = pd.to_datetime(df.index, format="%d-%m-%Y", errors="coerce")
    s = pd.to_numeric(df["nav"], errors="coerce")
    s.index = idx
    s = s.dropna()
    s = s[~s.index.to_series().isna()]
    return s.sort_index()


def _nav_from_dict(data: dict) -> pd.Series:
    """
    Normalize dict returned by mftool.get_scheme_historical_nav(code)
    """
    if not isinstance(data, dict):
        return pd.Series(dtype=float)
    rows = data.get("data") or []
    out = []
    for r in rows:
        try:
            d = pd.to_datetime(r.get("date"), format="%d-%m-%Y", errors="coerce")
            v = float(str(r.get("nav")).replace(",", ""))
            if pd.notna(d):
                out.append((d, v))
        except Exception:
            continue
    if not out:
        return pd.Series(dtype=float)
    df = pd.DataFrame(out, columns=["date", "nav"]).set_index("date")
    s = df["nav"]
    return s.sort_index()


# =====================================================
# NAV FETCH via mftool + cache
# =====================================================
def fetch_nav_series(code: str) -> pd.Series:
    """
    Fetch NAV series for a scheme code.

    1) Try local cache (nav_cache/{code}.parquet) if fresh.
    2) Else call:
         a) mf.get_scheme_historical_nav(code, as_Dataframe=True)  # preferred
         b) fallback: mf.get_scheme_historical_nav(code)           # dict mode
    3) Normalize to pandas.Series(date -> nav), ascending.
    4) Save to cache.

    If nothing usable, returns empty Series.
    """
    code = str(code).strip()
    cache_path = _nav_cache_path(code)

    # 1️⃣ Cache
    if _cache_is_fresh(cache_path):
        try:
            obj = pd.read_parquet(cache_path)
            if isinstance(obj, pd.Series):
                s = obj
            else:
                # assume single-column or "nav"
                if "nav" in obj.columns:
                    s = obj["nav"]
                else:
                    s = obj.iloc[:, 0]
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.dropna()
            s = s.sort_index()
            if len(s) > 1:
                _log(f"Cache hit for NAV {code} ({len(s)} points)")
                return s
        except Exception as e:
            _log(f"Cache read failed for {code}: {e}")

    # 2️⃣ Fetch from mftool
    mf = Mftool()

    nav_series = pd.Series(dtype=float)

    # a) preferred: DataFrame
    try:
        df = mf.get_scheme_historical_nav(code, as_Dataframe=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            nav_series = _nav_from_dataframe(df)
    except TypeError:
        # older mftool may not support as_Dataframe param
        pass
    except Exception as e:
        _log(f"mftool.get_scheme_historical_nav(as_Dataframe=True) failed for {code}: {e}")

    # b) dict fallback
    if nav_series.empty:
        try:
            raw = mf.get_scheme_historical_nav(code)
            nav_series = _nav_from_dict(raw)
        except Exception as e:
            _log(f"mftool.get_scheme_historical_nav() dict mode failed for {code}: {e}")
            return pd.Series(dtype=float)

    if nav_series.empty:
        _log(f"No NAV data returned for {code}")
        return nav_series

    # 3️⃣ Save to cache
    try:
        nav_series.to_frame(name="nav").to_parquet(cache_path)
        _log(f"Wrote NAV cache for {code} ({len(nav_series)} points)")
    except Exception as e:
        _log(f"Failed to write NAV cache for {code}: {e}")

    return nav_series


# =====================================================
# NAV -> RETURNS
# =====================================================
def nav_to_returns(nav: pd.Series) -> pd.Series:
    """Daily percentage returns."""
    if nav is None or len(nav) < 2:
        return pd.Series(dtype=float)
    return nav.pct_change().dropna()


def compute_periodic_returns(nav: pd.Series) -> Optional[float]:
    """CAGR over full available period."""
    if nav is None or len(nav) < 2:
        return None
    start_val = nav.iloc[0]
    end_val = nav.iloc[-1]
    if start_val <= 0:
        return None
    days = (nav.index[-1] - nav.index[0]).days
    if days <= 0:
        return None
    years = days / 365.0
    try:
        return (end_val / start_val) ** (1.0 / years) - 1.0
    except Exception:
        return None


def rolling_return(nav: pd.Series, window_days: int) -> Optional[float]:
    """Rolling return over last window_days."""
    if nav is None or len(nav) < 2:
        return None
    cutoff = nav.index[-1] - pd.Timedelta(days=window_days)
    window_nav = nav[nav.index >= cutoff]
    if len(window_nav) < 2:
        return None
    return compute_periodic_returns(window_nav)


# =====================================================
# Risk metrics
# =====================================================
def annualized_volatility(returns: pd.Series) -> Optional[float]:
    if returns is None or len(returns) < 2:
        return None
    try:
        return float(returns.std(ddof=1) * np.sqrt(252.0))
    except Exception:
        return None


def annualized_return_from_returns(returns: pd.Series) -> Optional[float]:
    if returns is None or len(returns) < 2:
        return None
    try:
        return float(returns.mean() * 252.0)
    except Exception:
        return None


def max_drawdown(nav: pd.Series) -> Optional[float]:
    if nav is None or len(nav) < 2:
        return None
    try:
        running_max = nav.cummax()
        dd = (nav / running_max) - 1.0
        return float(dd.min())
    except Exception:
        return None


# =====================================================
# Sharpe / Sortino
# =====================================================
def sharpe_ratio(returns: pd.Series, risk_free: float = RISK_FREE_RATE_DEFAULT) -> Optional[float]:
    if returns is None or len(returns) < 2:
        return None
    ann_ret = annualized_return_from_returns(returns)
    ann_vol = annualized_volatility(returns)
    if ann_ret is None or ann_vol in (None, 0.0):
        return None
    try:
        excess = ann_ret - risk_free
        return float(excess / ann_vol)
    except Exception:
        return None


def sortino_ratio(returns: pd.Series, risk_free: float = RISK_FREE_RATE_DEFAULT) -> Optional[float]:
    if returns is None or len(returns) < 2:
        return None
    downside = returns[returns < 0]
    if len(downside) < 2:
        return None
    ann_ret = annualized_return_from_returns(returns)
    if ann_ret is None:
        return None
    try:
        dd = downside.std(ddof=1) * np.sqrt(252.0)
        if dd == 0:
            return None
        excess = ann_ret - risk_free
        return float(excess / dd)
    except Exception:
        return None


# =====================================================
# Fees & AUM via mftool
# =====================================================
def _to_float_safe(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).replace(",", "").replace("%", "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def fetch_scheme_fees_and_aum(code: str):
    """
    Try to extract:
        - expense_ratio_percent
        - exit_load_percent (simple)
        - aum
    from mftool get_scheme_details / get_scheme_quote.
    """
    mf = Mftool()
    details = {}
    quote = {}

    try:
        details = mf.get_scheme_details(str(code)) or {}
    except Exception:
        logger.debug("get_scheme_details failed for %s", code)

    try:
        quote = mf.get_scheme_quote(str(code)) or {}
    except Exception:
        logger.debug("get_scheme_quote failed for %s", code)

    exp_ratio = None
    aum = None
    exit_load = None

    # Expense ratio keys
    for src in (details, quote):
        if not isinstance(src, dict):
            continue
        for k in ("expense_ratio", "expenseRatio", "Expense Ratio", "Expense_Ratio"):
            if k in src:
                exp_ratio = _to_float_safe(src[k])
                if exp_ratio is not None:
                    break
        if exp_ratio is not None:
            break

    # AUM keys
    for src in (details, quote):
        if not isinstance(src, dict):
            continue
        for k in ("scheme_aum", "AUM", "aum", "assets_under_management"):
            if k in src:
                aum = _to_float_safe(src[k])
                if aum is not None:
                    break
        if aum is not None:
            break

    # Exit load (simple % in text)
    import re
    text_candidates = []
    for src in (details, quote):
        if isinstance(src, dict):
            for v in src.values():
                if isinstance(v, str):
                    text_candidates.append(v.lower())
    combined = " ".join(text_candidates)
    m = re.search(r"exit\s*load[^\d%]{0,15}(\d+(?:\.\d+)?)\s*%?", combined)
    if m:
        exit_load = _to_float_safe(m.group(1))

    return exp_ratio, exit_load, aum, details, quote


# =====================================================
# Core aggregator
# =====================================================
def compute_metrics_for_code(
    code: str,
    risk_free_rate: float = RISK_FREE_RATE_DEFAULT,
    benchmark_nav_series: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    """
    Compute metrics for a single scheme code (typically the representative child for a parent).

    Returns dict with keys:
      scheme_code, data_points, first_date, last_date,
      cagr, rolling_1y, rolling_3y, rolling_5y,
      volatility_annual, sharpe, sortino, max_drawdown,
      expense_ratio_percent, exit_load_percent, aum,
      scheme_details_raw, scheme_quote_raw,
      beta, tracking_error (currently None)
    """

    code = str(code).strip()
    out: Dict[str, Any] = {
        "scheme_code": code,
        "data_points": 0,
        "first_date": None,
        "last_date": None,
        "cagr": None,
        "rolling_1y": None,
        "rolling_3y": None,
        "rolling_5y": None,
        "volatility_annual": None,
        "sharpe": None,
        "sortino": None,
        "max_drawdown": None,
        "expense_ratio_percent": None,
        "exit_load_percent": None,
        "aum": None,
        "scheme_details_raw": {},
        "scheme_quote_raw": {},
        "beta": None,
        "tracking_error": None,
    }

    t_start = time.time()
    nav = fetch_nav_series(code)
    if nav is None or len(nav) < 2:
        _log(f"compute_metrics_for_code: insufficient NAV for {code}")
        return out

    returns = nav_to_returns(nav)

    out["data_points"] = len(nav)
    out["first_date"] = str(nav.index[0].date())
    out["last_date"] = str(nav.index[-1].date())

    # Performance
    out["cagr"] = compute_periodic_returns(nav)
    out["rolling_1y"] = rolling_return(nav, 365)
    out["rolling_3y"] = rolling_return(nav, 365 * 3)
    out["rolling_5y"] = rolling_return(nav, 365 * 5)

    # Risk
    out["volatility_annual"] = annualized_volatility(returns)
    out["max_drawdown"] = max_drawdown(nav)

    # Risk-adjusted
    out["sharpe"] = sharpe_ratio(returns, risk_free=risk_free_rate)
    out["sortino"] = sortino_ratio(returns, risk_free=risk_free_rate)

    # Fees / AUM
    exp, exit_ld, aum, det, qte = fetch_scheme_fees_and_aum(code)
    out["expense_ratio_percent"] = exp
    out["exit_load_percent"] = exit_ld
    out["aum"] = aum
    out["scheme_details_raw"] = det
    out["scheme_quote_raw"] = qte

    # Benchmark-based metrics (beta / tracking_error) intentionally left None for now.

    t_done = time.time()
    logger.info("Computed metrics for %s (%d points) in %.2fs", code, out["data_points"], t_done - t_start)
    return out


# =====================================================
# Batch computation
# =====================================================
def compute_metrics_batch(
    codes: List[str],
    max_workers: int = 8,
    risk_free_rate: float = RISK_FREE_RATE_DEFAULT,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute metrics for many scheme codes in parallel.
    Only basic progress logging:
        Progress: 10/50 (20.0%). Elapsed: 44.0s. Avg/task: 4.3s. ETA: 218.0s
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: Dict[str, Dict[str, Any]] = {}
    total = len(codes)
    if total == 0:
        return results

    start_time = time.time()
    per_item_times: List[float] = []
    processed = 0

    def worker(c: str) -> Dict[str, Any]:
        return compute_metrics_for_code(c, risk_free_rate=risk_free_rate)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(worker, c): c for c in codes}
        for fut in as_completed(future_map):
            t0 = time.time()
            c = future_map[fut]

            try:
                results[str(c)] = fut.result()
            except Exception as e:
                # If one fails, set error but don't log anything else
                results[str(c)] = {"scheme_code": str(c), "error": str(e)}

            dur = time.time() - t0
            per_item_times.append(dur)
            processed += 1

            # Log minimal progress every 10 items or at end
            if processed % 10 == 0 or processed == total:
                elapsed = time.time() - start_time
                avg = sum(per_item_times) / len(per_item_times)
                remaining = (total - processed) * avg
                pct = (processed / total) * 100.0
                logger.info(
                    "Progress: %d/%d (%.1f%%). Elapsed: %.1fs. Avg/task: %.1fs. ETA: %.1fs",
                    processed, total, pct, elapsed, avg, remaining
                )

    return results
