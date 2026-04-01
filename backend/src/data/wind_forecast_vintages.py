"""Pull wind forecast vintage data — split by provider.

PJM wind: Latest vintage only (RTO). Short forward horizon makes DA cutoffs redundant.
Meteologica wind: Latest + DA cutoff vintages, all regions.

All functions return a DataFrame with columns: source, region, forecast_date,
hour_ending, forecast_mw, forecast_execution_datetime, vintage_label,
vintage_anchor_execution_datetime.
"""
import logging

import pandas as pd

from src.data import pjm_wind_forecast_hourly, meteologica_generation_forecast_hourly

logger = logging.getLogger(__name__)

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]

_COLS = [
    "source", "region", "forecast_date", "hour_ending", "forecast_mw",
    "forecast_execution_datetime", "vintage_label",
    "vintage_anchor_execution_datetime",
]


def pull_pjm_vintages() -> pd.DataFrame:
    """Pull PJM wind Latest vintage (RTO only)."""
    df_latest = pjm_wind_forecast_hourly.pull()
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest["vintage_anchor_execution_datetime"] = pd.to_datetime(
            df_latest["forecast_execution_datetime"]
        ).max()
        df_latest["forecast_mw"] = pd.to_numeric(df_latest["wind_forecast"], errors="coerce")
        df_latest["source"] = "pjm"
        df_latest["region"] = "RTO"
        df_latest = df_latest[_COLS]
    else:
        df_latest = pd.DataFrame(columns=_COLS)

    return _clean_dtypes(df_latest)


def pull_meteologica_vintages() -> pd.DataFrame:
    """Pull Meteologica wind Latest + DA cutoff vintages for all regions."""
    frames: list[pd.DataFrame] = []
    for region in REGIONS:
        df = _pull_meteologica_region(region)
        if len(df) > 0:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_COLS)

    return pd.concat(frames, ignore_index=True)


# ── Internal helpers ───────────────────────────────────────────────


def _pull_meteologica_region(region: str) -> pd.DataFrame:
    """Pull Meteologica wind vintages for one region."""
    df_latest = meteologica_generation_forecast_hourly.pull(source="wind", region=region)
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest["vintage_anchor_execution_datetime"] = pd.to_datetime(
            df_latest["forecast_execution_datetime"]
        ).max()
        df_latest["forecast_mw"] = pd.to_numeric(df_latest["forecast_generation_mw"], errors="coerce")
        df_latest["source"] = "meteologica"
        df_latest["region"] = region
        df_latest = df_latest[_COLS]
    else:
        df_latest = pd.DataFrame(columns=_COLS)

    df_da = meteologica_generation_forecast_hourly.pull_da_cutoff_vintages(source="wind", region=region)
    if df_da is not None and len(df_da) > 0:
        df_da["forecast_date"] = pd.to_datetime(df_da["forecast_date"])
        df_da["forecast_mw"] = pd.to_numeric(df_da["forecast_generation_mw"], errors="coerce")
        df_da["source"] = "meteologica"
        df_da["region"] = region
        df_da = df_da[_COLS]
    else:
        df_da = pd.DataFrame(columns=_COLS)

    return _clean_dtypes(pd.concat([df_latest, df_da], ignore_index=True))


def _clean_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise dtypes after concat."""
    if len(df) == 0:
        return pd.DataFrame(columns=_COLS)
    df = df.copy()
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_mw"] = pd.to_numeric(df["forecast_mw"], errors="coerce")
    df["forecast_execution_datetime"] = pd.to_datetime(df["forecast_execution_datetime"])
    df["vintage_anchor_execution_datetime"] = pd.to_datetime(df["vintage_anchor_execution_datetime"])
    df = df.dropna(subset=["forecast_date", "hour_ending", "forecast_mw", "vintage_label"]).copy()
    df["hour_ending"] = df["hour_ending"].astype(int)
    return df
