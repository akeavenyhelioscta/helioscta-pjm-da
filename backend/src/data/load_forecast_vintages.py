"""Pull load forecast data for multiple vintage anchors (latest, -12h, -24h, -48h).

Each vendor (PJM, Meteologica) is queried independently.  The module:
  1. Finds distinct execution timestamps for the source.
  2. Selects up to 4 anchors relative to the latest execution.
  3. Pulls forecast data only for those execution timestamps.
  4. Tags each row with its vintage label and offset.

Returns a single DataFrame suitable for parquet caching.
"""
import logging

import pandas as pd

from src.utils.azure_postgresql import pull_from_db

logger = logging.getLogger(__name__)

VINTAGE_OFFSETS: list[tuple[str, int]] = [
    ("Latest", 0),
    ("-12h", 12),
    ("-24h", 24),
    ("-48h", 48),
]

_SOURCE_CONFIG: dict[str, tuple[str, str]] = {
    "pjm": ("pjm_cleaned", "pjm_load_forecast_hourly"),
    "meteologica": ("meteologica_cleaned", "meteologica_pjm_demand_forecast_hourly"),
}


def pull_all_vintages(source: str = "pjm", region: str = "RTO") -> pd.DataFrame:
    """Pull load forecast data for up to 4 vintage anchors.

    Parameters
    ----------
    source : str
        ``"pjm"`` or ``"meteologica"``.
    region : str
        PJM region code, default ``"RTO"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``date``, ``hour_ending``, ``forecast_load_mw``,
        ``forecast_execution_datetime``, ``vintage_label``,
        ``vintage_offset_hours``.  Empty DataFrame when no data is available.
    """
    if source not in _SOURCE_CONFIG:
        raise ValueError(f"Unknown source: {source!r}. Must be one of {list(_SOURCE_CONFIG)}")

    schema, table = _SOURCE_CONFIG[source]
    logger.info(f"Pulling {source} vintage data: {schema}.{table}, region={region}")

    # ── 1. Distinct execution timestamps, newest first ────────────
    exec_df = pull_from_db(
        f"SELECT DISTINCT forecast_execution_datetime "
        f"FROM {schema}.{table} "
        f"WHERE region = '{region}' "
        f"ORDER BY forecast_execution_datetime DESC "
        f"LIMIT 200"
    )

    if exec_df is None or len(exec_df) == 0:
        logger.warning(f"No execution timestamps found for {source}")
        return pd.DataFrame()

    # Parse timestamps for offset comparison; keep raw strings for SQL matching
    exec_df["_ts"] = pd.to_datetime(exec_df["forecast_execution_datetime"])
    exec_df = exec_df.sort_values("_ts", ascending=False).reset_index(drop=True)
    latest_ts = exec_df["_ts"].iloc[0]

    # ── 2. Find vintage anchors ───────────────────────────────────
    anchors: list[dict] = []
    for label, hours in VINTAGE_OFFSETS:
        target = latest_ts - pd.Timedelta(hours=hours)
        mask = exec_df["_ts"] <= target
        if mask.any():
            row = exec_df.loc[mask].iloc[0]
            anchors.append({
                "vintage_label": label,
                "vintage_offset_hours": hours,
                "raw_exec_ts": row["forecast_execution_datetime"],
                "parsed_exec_ts": row["_ts"],
            })
            logger.info(
                f"  {label}: target<={target}, matched={row['_ts']} "
                f"(raw='{row['forecast_execution_datetime']}')"
            )

    if not anchors:
        logger.warning(f"No vintage anchors resolved for {source}")
        return pd.DataFrame()

    # ── 3. Pull data for unique execution timestamps ──────────────
    unique_raws = list({a["raw_exec_ts"] for a in anchors})
    ts_sql = ", ".join(f"'{r}'" for r in unique_raws)

    data_df = pull_from_db(
        f"SELECT forecast_date AS date, hour_ending, forecast_load_mw, "
        f"       forecast_execution_datetime "
        f"FROM {schema}.{table} "
        f"WHERE region = '{region}' "
        f"  AND forecast_execution_datetime IN ({ts_sql}) "
        f"ORDER BY forecast_execution_datetime, forecast_date, hour_ending"
    )

    if data_df is None or len(data_df) == 0:
        logger.warning(f"No forecast data for {source} vintages")
        return pd.DataFrame()

    logger.info(f"Pulled {len(data_df):,} rows for {len(unique_raws)} unique execution timestamp(s)")

    # ── 4. Tag rows with vintage labels via merge ─────────────────
    anchor_df = pd.DataFrame([
        {
            "forecast_execution_datetime": a["raw_exec_ts"],
            "vintage_label": a["vintage_label"],
            "vintage_offset_hours": a["vintage_offset_hours"],
        }
        for a in anchors
    ])

    result = data_df.merge(anchor_df, on="forecast_execution_datetime", how="inner")
    result["date"] = pd.to_datetime(result["date"])
    result["forecast_execution_datetime"] = pd.to_datetime(result["forecast_execution_datetime"])

    logger.info(
        f"Result: {len(result):,} rows, "
        f"vintages={result['vintage_label'].unique().tolist()}, "
        f"dates={sorted(result['date'].dt.date.unique())}"
    )

    return result
