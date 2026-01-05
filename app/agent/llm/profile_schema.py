from dataclasses import dataclass
from typing import Optional

@dataclass
class UserProfile:
    goal: Optional[str] = None
    time_horizon_years: Optional[int] = None
    risk: Optional[str] = None
    investment_type: Optional[str] = None
    investment_amount: Optional[float] = None

    def is_complete(self) -> bool:
        return all([
            self.goal,
            self.time_horizon_years,
            self.risk,
            self.investment_type
        ])
