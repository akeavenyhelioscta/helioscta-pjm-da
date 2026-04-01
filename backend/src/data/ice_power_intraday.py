from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.data.frame_validation import validate_source_frame

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"

SYMBOL_LABELS: dict[str, str] = {
    "PDA D1-IUS": "NxtDay DA",
    "PDP D0-IUS": "BalDay RT",
    "PDP D1-IUS": "NxtDay RT",
    "PDP W0-IUS": "BalWeek",
    "PDP W1-IUS": "Week1",
    "PDP W2-IUS": "Week2",
    "PDP W3-IUS": "Week3",
    "PDP W4-IUS": "Week4",
    "PJL D1-IUS": "NxtDay JCPL",
}

SYMBOL_PEAK_TYPE: dict[str, str] = {
    "PDA D1-IUS": "onpeak",
    "PDP D0-IUS": "onpeak",
    "PDP D1-IUS": "onpeak",
    "PDP W0-IUS": "onpeak",
    "PDP W1-IUS": "onpeak",
    "PDP W2-IUS": "onpeak",
    "PDP W3-IUS": "onpeak",
    "PDP W4-IUS": "onpeak",
    "PJL D1-IUS": "onpeak",
}


def pull_settles(
    lookback_days: int = 30,
    symbols: list[str] | None = None,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull daily settlement prices for ICE PJM power products.

    Returns one row per (trade_date, symbol) with settle, prior_settle,
    vwap, high, low, volume, and settle_vs_prior.
    """
    sql_file = SQL_DIR / "ice_power_settles.sql"
    overrides: dict[str, str | int | bool | None] = {
        "lookback_days": lookback_days,
        "symbols": ",".join(symbols) if symbols else "",
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)

    label = ",".join(symbols) if symbols else "ALL"
    logger.info(f"Pulling ICE power settles: symbols={label}, lookback={lookback_days}d")

    df = pull_from_db(query=query)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["product"] = df["symbol"].map(SYMBOL_LABELS)
    df["peak_type"] = df["symbol"].map(SYMBOL_PEAK_TYPE)

    df = validate_source_frame(
        df=df,
        source_name="ice_power_settles",
        required_columns=["trade_date", "symbol", "settle"],
        unique_key_columns=["trade_date", "symbol"],
        drop_duplicate_keys=True,
    )

    logger.info(f"Pulled ICE power settles: {len(df):,} rows, {df['symbol'].nunique()} products")
    return df


def pull_intraday(
    lookback_days: int = 7,
    symbols: list[str] | None = None,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull full intraday snapshot tape for ICE PJM power products.

    Returns one row per (trade_date, symbol, snapshot_at) with bid, ask,
    spread, last_px, vwap, high, low, open_px, volume, and last_chg.
    """
    sql_file = SQL_DIR / "ice_power_intraday.sql"
    overrides: dict[str, str | int | bool | None] = {
        "lookback_days": lookback_days,
        "symbols": ",".join(symbols) if symbols else "",
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)

    label = ",".join(symbols) if symbols else "ALL"
    logger.info(f"Pulling ICE power intraday: symbols={label}, lookback={lookback_days}d")

    df = pull_from_db(query=query)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["snapshot_at"] = pd.to_datetime(df["snapshot_at"])
    df["product"] = df["symbol"].map(SYMBOL_LABELS)
    df["peak_type"] = df["symbol"].map(SYMBOL_PEAK_TYPE)

    df = validate_source_frame(
        df=df,
        source_name="ice_power_intraday",
        required_columns=["trade_date", "symbol", "snapshot_at", "last_px"],
        unique_key_columns=["trade_date", "symbol", "snapshot_at"],
        drop_duplicate_keys=True,
    )

    logger.info(f"Pulled ICE power intraday: {len(df):,} rows, {df['trade_date'].nunique()} days")
    return df
