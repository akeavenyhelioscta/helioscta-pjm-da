"""Model adapters for the shared backtesting engine."""

from src.backtesting.adapters.base import ForecastAdapter, ForecastResult
from src.backtesting.adapters.like_day import LikeDayAdapter
from src.backtesting.adapters.lasso_qr import LassoQRAdapter
from src.backtesting.adapters.supply_stack import SupplyStackAdapter

__all__ = [
    "ForecastAdapter",
    "ForecastResult",
    "LikeDayAdapter",
    "LassoQRAdapter",
    "SupplyStackAdapter",
]
