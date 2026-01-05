import math
from copy import deepcopy
from typing import List, Optional


LOWER_IS_BETTER = {
    "volatility_annual",
    "max_drawdown"
}


TARGET_IS_ONE = {
    "beta"
}


WEIGHTS = {
    "cagr": 0.20,
    "rolling_3y": 0.10,
    "rolling_5y": 0.10,
    "alpha": 0.15,
    "sharpe": 0.15,
    "sortino": 0.10,
    "volatility_annual": 0.08,
    "max_drawdown": 0.07,
    "beta": 0.05
}


def _safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _normalize(values: list[float]):
    valid = [v for v in values if v is not None]
    if not valid:
        return {}

    min_v, max_v = min(valid), max(valid)
    if min_v == max_v:
        return {i: 1.0 for i, v in enumerate(values) if v is not None}

    return {
        i: (v - min_v) / (max_v - min_v)
        for i, v in enumerate(values) if v is not None
    }


def score_schemes(
    schemes: List[dict],
    profile: Optional[object] = None  # 👈 added, unused for now
) -> List[dict]:
    """
    Deterministic scoring.

    - `profile` is accepted for future extensions
    - current logic remains 100% unchanged

    Returns new list with `score` added.
    """
    schemes = deepcopy(schemes)

    # collect metric values
    metric_matrix = {k: [] for k in WEIGHTS}

    for s in schemes:
        for m in WEIGHTS:
            metric_matrix[m].append(_safe(s["metrics"].get(m)))

    normalized = {
        m: _normalize(vals)
        for m, vals in metric_matrix.items()
    }

    for idx, scheme in enumerate(schemes):
        score = 0.0
        used_weight = 0.0

        for metric, weight in WEIGHTS.items():
            if idx not in normalized[metric]:
                continue

            val = normalized[metric][idx]

            if metric in LOWER_IS_BETTER:
                val = 1 - val

            if metric in TARGET_IS_ONE:
                beta = scheme["metrics"].get("beta")
                if beta is not None:
                    val = max(0.0, 1 - abs(beta - 1))

            score += weight * val
            used_weight += weight

        scheme["score"] = round(score / used_weight, 4) if used_weight else 0.0

    return sorted(schemes, key=lambda x: x["score"], reverse=True)
