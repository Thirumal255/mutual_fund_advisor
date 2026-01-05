from datetime import datetime, timezone
from typing import List


def build_recommendation_payload(
    scored_schemes: List[dict],
    top_n: int = 5
) -> dict:
    """
    Builds a stable recommendation payload from scored schemes.
    """

    top = scored_schemes[:top_n]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_n": top_n,
        "schemes": []
    }

    for idx, s in enumerate(top, start=1):
        payload["schemes"].append({
            "rank": idx,
            "scheme_code": s["scheme_code"],
            "scheme_name": s["scheme_name"],
            "score": s["score"],

            # SID
            "category": s.get("category"),
            "scheme_type": s.get("scheme_type"),
            "declared_benchmark": s.get("declared_benchmark"),
            "fund_objective_summary": s.get("fund_objective_summary"),
            "asset_allocation_summary": s.get("asset_allocation_summary"),

            # Metrics
            "metrics": {
                "cagr": s["metrics"].get("cagr"),
                "rolling_3y": s["metrics"].get("rolling_3y"),
                "rolling_5y": s["metrics"].get("rolling_5y"),
                "alpha": s["metrics"].get("alpha"),
                "sharpe": s["metrics"].get("sharpe"),
                "sortino": s["metrics"].get("sortino"),
                "volatility_annual": s["metrics"].get("volatility_annual"),
                "max_drawdown": s["metrics"].get("max_drawdown"),
                "beta": s["metrics"].get("beta"),
            }
        })

    return payload
