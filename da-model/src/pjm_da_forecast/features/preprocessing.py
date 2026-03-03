"""Variance Stabilizing Transformation (Uniejewski 2018).

asinh(x) = ln(x + sqrt(x^2 + 1))
- Behaves like log(2x) for large x, compresses price spikes
- Well-defined at zero and for negative prices (unlike log)
- Critical for PJM where Western Hub spikes to $2,300+
"""
import numpy as np
import pandas as pd


def asinh_transform(x: pd.Series | np.ndarray) -> pd.Series | np.ndarray:
    """Apply area hyperbolic sine transformation."""
    return np.arcsinh(x)


def asinh_inverse(y: pd.Series | np.ndarray) -> pd.Series | np.ndarray:
    """Inverse asinh transform: sinh(y) to convert back to $/MWh."""
    return np.sinh(y)
