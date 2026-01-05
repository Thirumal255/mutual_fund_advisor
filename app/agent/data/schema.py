from typing import TypedDict, Optional


class Metrics(TypedDict, total=False):
    cagr: Optional[float]
    rolling_1y: Optional[float]
    rolling_3y: Optional[float]
    rolling_5y: Optional[float]
    volatility_annual: Optional[float]
    sharpe: Optional[float]
    sortino: Optional[float]
    max_drawdown: Optional[float]
    beta: Optional[float]
    tracking_error: Optional[float]
    scheme_start_date: Optional[str]
    scheme_latest_date: Optional[str]
    scheme_initial_nav: Optional[float]
    scheme_current_nav: Optional[float]


class Scheme(TypedDict):
    scheme_code: str
    scheme_name: str

    # SID
    category: Optional[str]
    scheme_type: Optional[str]
    declared_benchmark: Optional[str]
    fund_objective_summary: Optional[str]
    plans_and_options: Optional[str]
    exit_load: Optional[str]
    fund_manager: Optional[str]
    expense_ratio_percent: Optional[float]
    asset_allocation_summary: Optional[str]

    # Metrics
    metrics: Metrics
