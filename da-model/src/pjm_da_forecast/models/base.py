"""Abstract base class for quantile forecasting models."""
from abc import ABC, abstractmethod
import pandas as pd


class QuantileForecaster(ABC):
    """Base class for probabilistic quantile forecasting models."""

    def __init__(self, quantiles: list[float], name: str):
        self.quantiles = quantiles
        self.name = name

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Train the model."""

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate quantile predictions. Returns columns q_0.01, q_0.05, ..., q_0.99."""

    @abstractmethod
    def save(self, path: str) -> None:
        """Save model artifacts to disk."""

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> "QuantileForecaster":
        """Load model artifacts from disk."""
