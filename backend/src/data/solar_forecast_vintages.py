"""Pull solar forecast vintage data.

PJM solar: Latest vintage only (short forward horizon makes DA cutoffs redundant).
Meteologica solar: Latest + DA cutoff vintages (all 5).

Provides two functions:
  - ``pull_source_vintages(region)`` — PJM (RTO only) or Meteologica (any region)
  - ``pull_combined_vintages()`` — all sources × regions in one DataFrame

Both return a DataFrame with columns: source, region, forecast_date,
hour_ending, forecast_mw, forecast_execution_datetime, vintage_label,
vintage_anchor_execution_datetime.
"""
import logging

import pandas as pd

from src.data import pjm_solar_forecast_hourly, meteologica_generation_forecast_hourly

logger = logging.getLogger(__name__)

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]

_COLS = [
    "source", "region", "forecast_date", "hour_ending", "forecast_mw",
    "forecast_execution_datetime", "vintage_label",
    "vintage_anchor_execution_datetime",
]


def pull_source_vintages(source: str, region: str = "RTO") -> pd.DataFrame:
    """Pull latest + DA cutoff vintages for one source and one region.

    Parameters
    ----------
    source : str
        ``"pjm"`` or ``"meteologica"``.
    region : str
        PJM region code. Only ``"RTO"`` is valid for PJM solar.
    """
    if source == "pjm":
        return _pull_pjm(region)
    elif source == "meteologica":
        return _pull_meteologica(region)
    else:
        raise ValueError(f"Unknown source: {source!r}")


def pull_combined_vintages() -> pd.DataFrame:
    """Pull latest + DA cutoff vintages for ALL sources × regions.

    PJM solar: RTO only.
    Meteologica solar: all 4 regions.
    """
    frames: list[pd.DataFrame] = []

    # PJM — RTO only
    df = _pull_pjm("RTO")
    if len(df) > 0:
        frames.append(df)

    # Meteologica — all regions
    for region in REGIONS:
        df = _pull_meteologica(region)
        if len(df) > 0:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_COLS)

    return pd.concat(frames, ignore_index=True)


# ── Source-specific pull helpers ────────────────────────────────────


def _pull_pjm(region: str) -> pd.DataFrame:
    """Pull PJM solar Latest vintage only (RTO only).

    PJM solar has very short forward horizon (~2-3 days), so DA cutoff
    vintages are mostly empty or identical to Latest.
    """
    if region != "RTO":
        return pd.DataFrame(columns=_COLS)

    df_latest = pjm_solar_forecast_hourly.pull()
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest["vintage_anchor_execution_datetime"] = pd.to_datetime(
            df_latest["forecast_execution_datetime"]
        ).max()
        df_latest["forecast_mw"] = pd.to_numeric(df_latest["solar_forecast"], errors="coerce")
        df_latest["source"] = "pjm"
        df_latest["region"] = "RTO"
        df_latest = df_latest[_COLS]
    else:
        df_latest = pd.DataFrame(columns=_COLS)

    return _clean_dtypes(df_latest)


def _pull_meteologica(region: str) -> pd.DataFrame:
    """Pull Meteologica solar vintages for one region."""
    df_latest = meteologica_generation_forecast_hourly.pull(source="solar", region=region)
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

    df_da = meteologica_generation_forecast_hourly.pull_da_cutoff_vintages(source="solar", region=region)
    if df_da is not None and len(df_da) > 0:
        df_da["forecast_date"] = pd.to_datetime(df_da["forecast_date"])
        df_da["forecast_mw"] = pd.to_numeric(df_da["forecast_generation_mw"], errors="coerce")
        df_da["source"] = "meteologica"
        df_da["region"] = region
        df_da = df_da[_COLS]
    else:
        df_da = pd.DataFrame(columns=_COLS)

    return _clean_dtypes(pd.concat([df_latest, df_da], ignore_index=True))


# ── Helpers ─────────────────────────────────────────────────────────


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
