"""Source pulls and hourly input assembly for the supply stack model.

This module builds the first implementation building block:
one 24-row hourly input table for a delivery date containing load,
renewables, gas, outages, and net load.
"""
from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

# Allow direct execution via `python sources.py` from this folder.
if __package__ in (None, ""):
    backend_root = Path(__file__).resolve().parents[3]
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))

from src.data import (
    gas_prices_hourly,
    meteologica_generation_forecast_hourly,
    meteologica_load_forecast_hourly,
    outages_forecast_daily,
    pjm_load_forecast_hourly,
    solar_forecast_hourly,
    wind_forecast_hourly,
)

logger = logging.getLogger(__name__)

DEFAULT_GAS_HUB = "gas_m3"
DEFAULT_OUTAGE_COLUMN = "total_outages_mw"

REGION_PRESETS: dict[str, dict[str, str]] = {
    "rto": {
        "load_source": "pjm",
        "load_region": "RTO",
        "renewable_source": "gridstatus",
        "renewable_solar_region": "RTO",
        "renewable_wind_region": "RTO",
        "outage_region": "RTO",
        "default_gas_hub": "gas_m3",
    },
    "south": {
        # Zonal pilot: uses Meteologica zonal load/renewables and RTO outage proxy.
        "load_source": "meteologica",
        "load_region": "SOUTH",
        "renewable_source": "meteologica",
        "renewable_solar_region": "SOUTH",
        "renewable_wind_region": "SOUTH",
        "outage_region": "RTO",
        "default_gas_hub": "gas_dom_south",
    },
    "dominion": {
        # Dominion proxy: SOUTH_DOM demand/renewables + MIDATL_DOM outages.
        "load_source": "meteologica",
        "load_region": "SOUTH_DOM",
        "renewable_source": "meteologica",
        # Meteologica currently publishes wind for SOUTH_DOM but solar at SOUTH.
        "renewable_solar_region": "SOUTH",
        "renewable_wind_region": "SOUTH_DOM",
        "outage_region": "MIDATL_DOM",
        "default_gas_hub": "gas_tz5",
    },
}


def resolve_forecast_date(forecast_date: str | date | None = None) -> date:
    """Resolve target delivery date; defaults to tomorrow."""
    if forecast_date is None:
        return date.today() + timedelta(days=1)
    if isinstance(forecast_date, date):
        return forecast_date
    return pd.to_datetime(forecast_date).date()


def _resolve_input_scope(
    region: str,
    region_preset: str | None,
) -> dict[str, str]:
    """Resolve source/region strategy for hourly input pulls."""
    if region_preset is None:
        return {
            "load_source": "pjm",
            "load_region": region,
            "renewable_source": "gridstatus",
            "renewable_solar_region": region,
            "renewable_wind_region": region,
            "outage_region": region,
            "default_gas_hub": DEFAULT_GAS_HUB,
        }

    key = region_preset.strip().lower()
    if key not in REGION_PRESETS:
        raise ValueError(
            f"Unknown region_preset={region_preset!r}. "
            f"Valid values: {sorted(REGION_PRESETS)}"
        )
    return dict(REGION_PRESETS[key])


def _coerce_generation_forecast_frame(
    df_generation: pd.DataFrame,
    output_col: str,
) -> pd.DataFrame:
    """Convert Meteologica/PJM generation pull frames to [date, hour_ending, output_col]."""
    if df_generation is None or len(df_generation) == 0:
        return pd.DataFrame(columns=["date", "hour_ending", output_col])

    if "hour_ending" not in df_generation.columns:
        raise ValueError("Generation frame missing required column: hour_ending")

    if "forecast_date" in df_generation.columns:
        date_col = "forecast_date"
    elif "date" in df_generation.columns:
        date_col = "date"
    else:
        raise ValueError("Generation frame missing date/forecast_date column")

    if "forecast_generation_mw" in df_generation.columns:
        value_col = "forecast_generation_mw"
    elif output_col in df_generation.columns:
        value_col = output_col
    else:
        raise ValueError(
            f"Generation frame missing value column: forecast_generation_mw/{output_col}"
        )

    out = df_generation[[date_col, "hour_ending", value_col]].copy()
    out = out.rename(columns={date_col: "date", value_col: output_col})
    return out


def align_gas_to_electric_day(df_gas: pd.DataFrame) -> pd.DataFrame:
    """Align gas day timestamps to electric-day HE convention.

    Gas day D prices map to electric day:
    - HE10-24 on day D
    - HE1-9 on day D+1
    """
    required = {"date", "hour_ending"}
    missing = required.difference(df_gas.columns)
    if missing:
        raise ValueError(f"Gas frame missing required columns: {sorted(missing)}")

    df = df_gas.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    gas_he10_24 = df[df["hour_ending"] >= 10].copy()
    gas_he1_9 = df[df["hour_ending"] < 10].copy()
    gas_he1_9["date"] = gas_he1_9["date"] + timedelta(days=1)

    aligned = pd.concat([gas_he10_24, gas_he1_9], ignore_index=True)
    aligned = aligned.sort_values(["date", "hour_ending"]).reset_index(drop=True)
    return aligned


def select_outage_for_date(
    df_outages: pd.DataFrame,
    target_date: date,
    region: str = "RTO",
    outage_column: str = DEFAULT_OUTAGE_COLUMN,
) -> float:
    """Select latest available outage MW forecast for target delivery date."""
    if df_outages is None or len(df_outages) == 0:
        raise ValueError("Outage forecast frame is empty")
    if outage_column not in df_outages.columns:
        raise ValueError(f"Outage column '{outage_column}' not found in outage frame")

    df = df_outages.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    if "forecast_execution_date" in df.columns:
        df["forecast_execution_date"] = pd.to_datetime(df["forecast_execution_date"]).dt.date

    mask = df["forecast_date"] == target_date
    if "region" in df.columns:
        mask = mask & (df["region"] == region)
    day_df = df[mask].copy()
    if len(day_df) == 0:
        raise ValueError(
            f"No outage forecast rows found for target_date={target_date} region={region}"
        )

    sort_cols = []
    if "forecast_execution_date" in day_df.columns:
        sort_cols.append("forecast_execution_date")
    if "forecast_rank" in day_df.columns:
        sort_cols.append("forecast_rank")

    if sort_cols:
        day_df = day_df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    selected = day_df.iloc[0]
    value = selected[outage_column]
    if pd.isna(value):
        raise ValueError(
            f"Selected outage value is null for target_date={target_date} region={region}"
        )
    return float(value)


def _day_slice(
    df: pd.DataFrame,
    target_date: date,
    date_col: str = "date",
) -> pd.DataFrame:
    """Filter a frame to one date."""
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col]).dt.date
    return out[out[date_col] == target_date].copy()


def _normalize_hourly_load(
    df_load: pd.DataFrame,
    target_date: date,
) -> pd.DataFrame:
    """Return load rows for one forecast date with canonical column names."""
    if "forecast_date" not in df_load.columns or "hour_ending" not in df_load.columns:
        raise ValueError("Load frame missing required columns: forecast_date, hour_ending")

    day = _day_slice(df_load, target_date=target_date, date_col="forecast_date")
    if len(day) == 0:
        raise ValueError(f"No load forecast rows found for forecast_date={target_date}")

    sort_cols: list[str] = []
    for c in ("hour_ending", "forecast_execution_datetime", "forecast_rank"):
        if c in day.columns:
            sort_cols.append(c)
    if sort_cols:
        day = day.sort_values(sort_cols)
    day = day.drop_duplicates(subset=["hour_ending"], keep="last")

    if "forecast_load_mw" in day.columns:
        load_col = "forecast_load_mw"
    elif "load_mw" in day.columns:
        load_col = "load_mw"
    else:
        raise ValueError("Load frame missing forecast_load_mw/load_mw column")

    out = day[["hour_ending", load_col]].copy()
    out = out.rename(columns={load_col: "load_mw"})
    out["date"] = target_date
    return out


def _normalize_hourly_generation(
    df: pd.DataFrame,
    target_date: date,
    value_col: str,
    output_col: str,
) -> pd.DataFrame:
    """Normalize one hourly renewable frame to a target date."""
    if value_col not in df.columns:
        raise ValueError(f"Generation frame missing required column: {value_col}")
    if "date" not in df.columns or "hour_ending" not in df.columns:
        raise ValueError("Generation frame missing required columns: date, hour_ending")

    day = _day_slice(df, target_date=target_date, date_col="date")
    if len(day) == 0:
        logger.warning(
            "No %s rows for target_date=%s; defaulting to zeros",
            output_col,
            target_date,
        )
        return pd.DataFrame(columns=["hour_ending", output_col])

    out = day[["hour_ending", value_col]].copy()
    has_zero = bool((out["hour_ending"] == 0).any())
    if has_zero:
        # Some upstream feeds encode midnight HE24 as 0. Normalize to PJM HE1-24.
        logger.info("Normalizing %s hour_ending 0->24 for %s", output_col, target_date)
        out["hour_ending"] = out["hour_ending"].replace({0: 24})

    out = out.rename(columns={value_col: output_col})
    out = out.drop_duplicates(subset=["hour_ending"], keep="last")
    return out


def _normalize_hourly_gas(
    df_gas: pd.DataFrame,
    target_date: date,
    gas_hub_col: str,
) -> pd.DataFrame:
    """Normalize hourly gas for one electric day."""
    if gas_hub_col not in df_gas.columns:
        raise ValueError(f"Gas frame missing hub column: {gas_hub_col}")

    aligned = align_gas_to_electric_day(df_gas)
    day = _day_slice(aligned, target_date=target_date, date_col="date")
    if len(day) == 0:
        # Next-day gas for the target electric day may not be published yet.
        # Fall back to the latest available electric day.
        aligned_dates = pd.to_datetime(aligned["date"]).dt.date
        latest = aligned_dates.max()
        logger.warning(
            "No gas rows for electric day %s — falling back to %s",
            target_date, latest,
        )
        day = _day_slice(aligned, target_date=latest, date_col="date")
        if len(day) == 0:
            raise ValueError(
                f"No gas rows found after alignment for target_date={target_date} "
                f"(fallback to {latest} also empty)"
            )

    out = day[["hour_ending", gas_hub_col]].copy()
    out = out.rename(columns={gas_hub_col: "gas_price_usd_mmbtu"})
    out = out.drop_duplicates(subset=["hour_ending"], keep="last")
    return out


def build_hourly_inputs(
    target_date: str | date,
    df_load: pd.DataFrame,
    df_solar: pd.DataFrame,
    df_wind: pd.DataFrame,
    df_gas: pd.DataFrame,
    df_outages: pd.DataFrame,
    gas_hub_col: str = DEFAULT_GAS_HUB,
    outage_column: str = DEFAULT_OUTAGE_COLUMN,
    region: str = "RTO",
) -> pd.DataFrame:
    """Build 24-row hourly input table for one delivery date."""
    resolved_date = resolve_forecast_date(target_date)
    hours = pd.DataFrame({"hour_ending": list(range(1, 25))})
    hours["date"] = resolved_date

    load = _normalize_hourly_load(df_load, resolved_date)
    solar = _normalize_hourly_generation(df_solar, resolved_date, "solar_forecast", "solar_mw")
    wind = _normalize_hourly_generation(df_wind, resolved_date, "wind_forecast", "wind_mw")
    gas = _normalize_hourly_gas(df_gas, resolved_date, gas_hub_col)
    outage_mw = select_outage_for_date(
        df_outages=df_outages,
        target_date=resolved_date,
        region=region,
        outage_column=outage_column,
    )

    hourly = hours.merge(load, on=["date", "hour_ending"], how="left")
    hourly = hourly.merge(solar, on="hour_ending", how="left")
    hourly = hourly.merge(wind, on="hour_ending", how="left")
    hourly = hourly.merge(gas, on="hour_ending", how="left")

    hourly["solar_mw"] = hourly["solar_mw"].fillna(0.0)
    hourly["wind_mw"] = hourly["wind_mw"].fillna(0.0)

    if hourly["load_mw"].isna().any():
        missing_hours = hourly.loc[hourly["load_mw"].isna(), "hour_ending"].tolist()
        raise ValueError(f"Missing load rows for hours: {missing_hours}")
    if hourly["gas_price_usd_mmbtu"].isna().any():
        missing_hours = hourly.loc[
            hourly["gas_price_usd_mmbtu"].isna(), "hour_ending"
        ].tolist()
        raise ValueError(f"Missing gas rows for hours: {missing_hours}")

    hourly["outages_mw"] = outage_mw
    hourly["net_load_mw"] = hourly["load_mw"] - hourly["solar_mw"] - hourly["wind_mw"]

    ordered_cols = [
        "date",
        "hour_ending",
        "load_mw",
        "solar_mw",
        "wind_mw",
        "net_load_mw",
        "gas_price_usd_mmbtu",
        "outages_mw",
    ]
    hourly = hourly[ordered_cols].sort_values("hour_ending").reset_index(drop=True)
    return hourly


def pull_hourly_inputs(
    forecast_date: str | date | None = None,
    region: str = "RTO",
    region_preset: str | None = None,
    gas_hub_col: str | None = None,
    outage_column: str = DEFAULT_OUTAGE_COLUMN,
    outages_lookback_days: int = 14,
    source_overrides: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Pull source datasets and build hourly inputs for one delivery date."""
    target_date = resolve_forecast_date(forecast_date)
    source_overrides = source_overrides or {}
    scope = _resolve_input_scope(region=region, region_preset=region_preset)
    effective_gas_hub = gas_hub_col or scope["default_gas_hub"]

    load_overrides = source_overrides.get("load_sql_overrides")
    if scope["load_source"] == "pjm":
        df_load = pjm_load_forecast_hourly.pull(
            region=scope["load_region"],
            sql_overrides=load_overrides,
        )
    elif scope["load_source"] == "meteologica":
        df_load = meteologica_load_forecast_hourly.pull(
            region=scope["load_region"],
            sql_overrides=load_overrides,
        )
    else:
        raise ValueError(f"Unsupported load_source={scope['load_source']!r}")

    if scope["renewable_source"] == "gridstatus":
        date_override = {"start_date": str(target_date), "end_date": str(target_date)}
        solar_overrides = source_overrides.get("solar_sql_overrides", {})
        solar_overrides = {**date_override, **solar_overrides}
        df_solar = solar_forecast_hourly.pull(sql_overrides=solar_overrides)

        wind_overrides = source_overrides.get("wind_sql_overrides", {})
        wind_overrides = {**date_override, **wind_overrides}
        df_wind = wind_forecast_hourly.pull(sql_overrides=wind_overrides)
    elif scope["renewable_source"] == "meteologica":
        solar_raw = meteologica_generation_forecast_hourly.pull(
            source="solar",
            region=scope["renewable_solar_region"],
            sql_overrides=source_overrides.get("solar_sql_overrides"),
        )
        wind_raw = meteologica_generation_forecast_hourly.pull(
            source="wind",
            region=scope["renewable_wind_region"],
            sql_overrides=source_overrides.get("wind_sql_overrides"),
        )
        df_solar = _coerce_generation_forecast_frame(solar_raw, output_col="solar_forecast")
        df_wind = _coerce_generation_forecast_frame(wind_raw, output_col="wind_forecast")
    else:
        raise ValueError(
            f"Unsupported renewable_source={scope['renewable_source']!r}"
        )

    df_gas = gas_prices_hourly.pull()
    df_outages = outages_forecast_daily.pull(
        region=scope["outage_region"],
        lookback_days=outages_lookback_days,
        sql_overrides=source_overrides.get("outages_sql_overrides"),
    )

    logger.info(
        "Building supply-stack hourly inputs for %s (preset=%s, load=%s/%s, renew=%s/solar:%s wind:%s, outages=%s, gas=%s)",
        target_date,
        region_preset or "none",
        scope["load_source"],
        scope["load_region"],
        scope["renewable_source"],
        scope["renewable_solar_region"],
        scope["renewable_wind_region"],
        scope["outage_region"],
        effective_gas_hub,
    )
    return build_hourly_inputs(
        target_date=target_date,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
        gas_hub_col=effective_gas_hub,
        outage_column=outage_column,
        region=scope["outage_region"],
    )


def _run_cli() -> None:
    """Run source pull from CLI.

    CLI behavior:
    - Defaults to today's delivery date when no --date is provided.
    - Prints a 24-row hourly input table and a short summary line.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Pull and assemble hourly supply-stack inputs.",
    )
    parser.add_argument(
        "--date",
        dest="forecast_date",
        default=None,
        help="Delivery date (YYYY-MM-DD). Defaults to today for CLI runs.",
    )
    parser.add_argument(
        "--region",
        default="RTO",
        help="PJM region (used when --region-preset is not provided).",
    )
    parser.add_argument(
        "--region-preset",
        choices=sorted(REGION_PRESETS),
        default=None,
        help="Preconfigured zonal scope: rto, south, dominion.",
    )
    parser.add_argument(
        "--gas-hub",
        dest="gas_hub_col",
        default=None,
        help="Gas hub override. If omitted, uses preset default (or gas_m3).",
    )
    parser.add_argument(
        "--outage-col",
        dest="outage_column",
        default=DEFAULT_OUTAGE_COLUMN,
        help=f"Outage MW column (default: {DEFAULT_OUTAGE_COLUMN}).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    target_date = (
        pd.to_datetime(args.forecast_date).date()
        if args.forecast_date
        else date.today()
    )

    scope = _resolve_input_scope(region=args.region, region_preset=args.region_preset)
    effective_gas_hub = args.gas_hub_col or scope["default_gas_hub"]

    df = pull_hourly_inputs(
        forecast_date=target_date,
        region=args.region,
        region_preset=args.region_preset,
        gas_hub_col=args.gas_hub_col,
        outage_column=args.outage_column,
    )
    print(df.to_string(index=False))
    print(
        f"\nrows={len(df)} date={target_date} preset={args.region_preset or 'none'} "
        f"region={scope['load_region']} outage_region={scope['outage_region']} "
        f"gas={effective_gas_hub}"
    )


if __name__ == "__main__":
    _run_cli()
