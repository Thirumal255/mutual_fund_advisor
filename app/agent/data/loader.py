import json
import math
from pathlib import Path
from app.agent.data.schema import Scheme


def _safe_float(value):
    try:
        if value is None:
            return None
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def load_metrics_ui(path: str | Path) -> list[Scheme]:
    path = Path(path)

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    schemes: list[Scheme] = []

    for parent in raw.values():
        for s in parent.get("children", []):

            scheme: Scheme = {
                "scheme_code": str(s["scheme_code"]),
                "scheme_name": s["scheme_name"],

                # SID
                "category": s.get("category"),
                "scheme_type": s.get("scheme_type"),
                "declared_benchmark": s.get("declared_benchmark"),
                "fund_objective_summary": s.get("fund_objective_summary"),
                "plans_and_options": s.get("plans_and_options"),
                "exit_load": s.get("exit_load"),
                "fund_manager": s.get("fund_manager"),
                "expense_ratio_percent": _safe_float(s.get("expense_ratio_percent")),
                "asset_allocation_summary": s.get("asset_allocation_summary"),

                # Metrics
                "metrics": {
                    "cagr": _safe_float(s.get("cagr")),
                    "rolling_1y": _safe_float(s.get("rolling_1y")),
                    "rolling_3y": _safe_float(s.get("rolling_3y")),
                    "rolling_5y": _safe_float(s.get("rolling_5y")),
                    "volatility_annual": _safe_float(s.get("volatility_annual")),
                    "sharpe": _safe_float(s.get("sharpe")),
                    "sortino": _safe_float(s.get("sortino")),
                    "max_drawdown": _safe_float(s.get("max_drawdown")),
                    "beta": _safe_float(s.get("beta")),
                    "tracking_error": _safe_float(s.get("tracking_error")),
                    "scheme_start_date": s.get("scheme_start_date"),
                    "scheme_latest_date": s.get("scheme_latest_date"),
                    "scheme_initial_nav": _safe_float(s.get("scheme_initial_nav")),
                    "scheme_current_nav": _safe_float(s.get("scheme_current_nav")),
                }
            }

            schemes.append(scheme)

    return schemes
