"""Pull load forecast vintage data (Latest + DA cutoff vintages).

Provides two functions:
  - ``pull_source_vintages(source, region)`` — single region
  - ``pull_combined_vintages(source)`` — all regions in one DataFrame

Both return a DataFrame with columns: region, forecast_date, hour_ending,
forecast_load_mw, forecast_execution_datetime, vintage_label,
vintage_anchor_execution_datetime.

The combined variant is the preferred cache unit — one parquet per source
holds every region, avoiding per-region cache file proliferation.
"""
import logging

import pandas as pd

from src.data import pjm_load_forecast_hourly, meteologica_load_forecast_hourly

logger = logging.getLogger(__name__)

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]

_COLS = [
    "region", "forecast_date", "hour_ending", "forecast_load_mw",
    "forecast_execution_datetime", "vintage_label",
    "vintage_anchor_execution_datetime",
]

_SOURCE_MODULES = {
    "pjm": pjm_load_forecast_hourly,
    "meteologica": meteologica_load_forecast_hourly,
}


def pull_source_vintages(source: str, region: str = "RTO") -> pd.DataFrame:
    """Pull latest + DA cutoff vintages for one source and one region.

    Parameters
    ----------
    source : str
        ``"pjm"`` or ``"meteologica"``.
    region : str
        PJM region code (``"RTO"``, ``"WEST"``, ``"MIDATL"``, ``"SOUTH"``).

    Returns
    -------
    pd.DataFrame
        Columns: region, forecast_date, hour_ending, forecast_load_mw,
        forecast_execution_datetime, vintage_label,
        vintage_anchor_execution_datetime.
    """
    if source not in _SOURCE_MODULES:
        raise ValueError(
            f"Unknown source: {source!r}. Must be one of {list(_SOURCE_MODULES)}"
        )

    mod = _SOURCE_MODULES[source]

    # 1. Latest vintage
    df_latest = mod.pull(region=region)
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest["vintage_anchor_execution_datetime"] = pd.to_datetime(
            df_latest["forecast_execution_datetime"]
        ).max()
        df_latest["region"] = region
        df_latest = df_latest[_COLS]
    else:
        df_latest = pd.DataFrame(columns=_COLS)

    # 2. DA cutoff vintages
    df_da = mod.pull_da_cutoff_vintages(region=region)
    if df_da is not None and len(df_da) > 0:
        df_da["forecast_date"] = pd.to_datetime(df_da["forecast_date"])
        df_da["region"] = region
        df_da = df_da[_COLS]
    else:
        df_da = pd.DataFrame(columns=_COLS)

    df = pd.concat([df_latest, df_da], ignore_index=True)
    if len(df) == 0:
        logger.warning(f"No vintage rows for source={source}, region={region}")
        return pd.DataFrame(columns=_COLS)

    return _clean_dtypes(df)


def pull_combined_vintages(source: str) -> pd.DataFrame:
    """Pull latest + DA cutoff vintages for ALL regions.

    One parquet cache file per source holds every region, so both the
    view-model endpoint and the reporting fragment share the same cache.

    Parameters
    ----------
    source : str
        ``"pjm"`` or ``"meteologica"``.

    Returns
    -------
    pd.DataFrame
        Same columns as ``pull_source_vintages`` with rows for every region.
    """
    frames: list[pd.DataFrame] = []
    for region in REGIONS:
        df = pull_source_vintages(source, region)
        if len(df) > 0:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_COLS)

    return pd.concat(frames, ignore_index=True)


# ── Helpers ───────────────────────────────────────────────────────


def _clean_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise dtypes after concat."""
    df = df.copy()
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["forecast_execution_datetime"] = pd.to_datetime(df["forecast_execution_datetime"])
    df["vintage_anchor_execution_datetime"] = pd.to_datetime(
        df["vintage_anchor_execution_datetime"],
    )
    df = df.dropna(
        subset=["forecast_date", "hour_ending", "forecast_load_mw", "vintage_label"],
    ).copy()
    df["hour_ending"] = df["hour_ending"].astype(int)
    return df
