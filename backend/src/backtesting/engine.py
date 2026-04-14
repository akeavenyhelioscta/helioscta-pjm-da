"""Walk-forward backtesting engine shared across forecast model adapters."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import logging

import pandas as pd

from src.backtesting.adapters import LassoQRAdapter, LikeDayAdapter, SupplyStackAdapter
from src.backtesting.adapters.base import ForecastAdapter
from src.backtesting.config import BacktestConfig
from src.backtesting.metrics import (
    build_period_slice,
    evaluate_period_slice,
    period_hours,
)
from src.like_day_forecast.features.builder import build_daily_features

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Outputs of a completed backtest run."""

    hourly_predictions: pd.DataFrame
    daily_metrics: pd.DataFrame
    aggregate_metrics: pd.DataFrame
    run_metadata: pd.DataFrame


def _build_adapters(config: BacktestConfig) -> list[ForecastAdapter]:
    model_names = [m.strip().lower() for m in config.models]
    adapters: list[ForecastAdapter] = []
    for name in model_names:
        if name == "like_day":
            adapters.append(
                LikeDayAdapter(
                    quantiles=config.quantiles,
                    cache_dir=config.cache_dir,
                    cache_enabled=config.cache_enabled,
                    cache_ttl_hours=config.cache_ttl_hours,
                    force_refresh=config.force_refresh,
                ),
            )
        elif name == "lasso_qr":
            adapters.append(LassoQRAdapter(quantiles=config.quantiles))
        elif name == "supply_stack":
            adapters.append(SupplyStackAdapter(quantiles=config.quantiles))
        else:
            raise ValueError(f"Unsupported model '{name}'")
    return adapters


def _pull_actuals(config: BacktestConfig, dates: list[date]) -> pd.DataFrame:
    from src.data import lmps_hourly
    from src.utils.cache_utils import pull_with_cache

    min_date = min(dates)
    max_date = max(dates)
    df = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": config.schema, "market": "da"},
        cache_dir=config.cache_dir,
        cache_enabled=config.cache_enabled,
        ttl_hours=config.cache_ttl_hours,
        force_refresh=config.force_refresh,
    )
    df = df[df["hub"] == config.hub].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = df["hour_ending"].astype(int)
    df = df[(df["date"] >= min_date) & (df["date"] <= max_date)]
    return df[["date", "hour_ending", "lmp_total"]].copy()


def _actuals_by_date(df_actuals: pd.DataFrame) -> dict[date, dict[int, float]]:
    out: dict[date, dict[int, float]] = {}
    for d, grp in df_actuals.groupby("date"):
        hm: dict[int, float] = {}
        for _, row in grp.iterrows():
            he = int(row["hour_ending"])
            val = row["lmp_total"]
            if pd.notna(val):
                hm[he] = float(val)
        out[d] = hm
    return out


def _aggregate_metrics(df_daily: pd.DataFrame) -> pd.DataFrame:
    if len(df_daily) == 0:
        return pd.DataFrame()
    metric_cols = [
        c for c in [
            "mae", "rmse", "mape", "bias",
            "mean_pinball", "crps", "coverage_80pct", "n_hours",
        ]
        if c in df_daily.columns
    ]
    if not metric_cols:
        return pd.DataFrame()
    agg = (
        df_daily.groupby(["model", "period"], as_index=False)[metric_cols]
        .mean(numeric_only=True)
    )
    return agg


def run_backtest(
    config: BacktestConfig,
    adapters: list[ForecastAdapter] | None = None,
    df_actuals: pd.DataFrame | None = None,
) -> BacktestResult:
    """Run a walk-forward backtest for one or more model adapters."""
    forecast_dates = config.forecast_dates()
    if not forecast_dates:
        raise ValueError("No forecast dates generated from config")

    _adapters_injected = adapters is not None
    if adapters is None:
        adapters = _build_adapters(config)
    if df_actuals is None:
        df_actuals = _pull_actuals(config, forecast_dates)
    actual_map = _actuals_by_date(df_actuals)

    hourly_rows: list[dict] = []
    daily_rows: list[dict] = []
    run_rows: list[dict] = []

    # Pre-build the shared feature matrix once when using real adapters.
    # When callers inject custom adapters (tests), skip the pre-build.
    df_features: pd.DataFrame | None = None
    if _adapters_injected is False:
        logger.info("Pre-building shared feature matrix for backtest...")
        df_features = build_daily_features(
            schema=config.schema,
            hub=config.hub,
            cache_dir=config.cache_dir,
            cache_enabled=config.cache_enabled,
            cache_ttl_hours=config.cache_ttl_hours,
            force_refresh=config.force_refresh,
        )
        logger.info(
            "Feature matrix ready: %d rows, %d features",
            len(df_features),
            len(df_features.columns) - 1,
        )

    retrain_n = max(int(config.retrain_every_n_days), 1)

    for idx, forecast_date in enumerate(forecast_dates):
        for adapter in adapters:
            force_retrain = adapter.name == "lasso_qr" and (idx % retrain_n == 0)
            logger.info(
                "Backtest %s %s (force_retrain=%s)",
                adapter.name,
                forecast_date,
                force_retrain,
            )

            try:
                fresult = adapter.forecast_for_date(
                    forecast_date=forecast_date,
                    force_retrain=force_retrain,
                    **({"df_features": df_features} if df_features is not None else {}),
                )
            except Exception as exc:
                run_rows.append(
                    {
                        "model": adapter.name,
                        "forecast_date": forecast_date,
                        "status": "error",
                        "error": str(exc),
                    },
                )
                continue

            if fresult.reference_date >= forecast_date:
                raise ValueError(
                    f"Leakage guard failed for {adapter.name} on {forecast_date}: "
                    f"reference_date={fresult.reference_date}"
                )

            actual_by_he = actual_map.get(forecast_date, {})
            complete_actuals = len(actual_by_he) >= 24
            if not complete_actuals and config.drop_incomplete_days:
                run_rows.append(
                    {
                        "model": adapter.name,
                        "forecast_date": forecast_date,
                        "reference_date": fresult.reference_date,
                        "status": "skipped_incomplete_actuals",
                        "n_actual_hours": len(actual_by_he),
                    },
                )
                continue

            for he in range(1, 25):
                row = {
                    "model": adapter.name,
                    "forecast_date": forecast_date,
                    "reference_date": fresult.reference_date,
                    "hour_ending": he,
                    "period": (
                        "on_peak" if he in range(8, 24) else "off_peak"
                    ),
                    "forecast": fresult.point_by_he.get(he),
                    "actual": actual_by_he.get(he),
                }
                if row["forecast"] is not None and row["actual"] is not None:
                    row["error"] = float(row["forecast"]) - float(row["actual"])
                else:
                    row["error"] = None

                for q in sorted(adapter.quantiles):
                    row[f"q_{q:.2f}"] = fresult.quantiles_by_he.get(he, {}).get(q)
                hourly_rows.append(row)

            common = {
                "model": adapter.name,
                "forecast_date": forecast_date,
                "reference_date": fresult.reference_date,
                "month": int(pd.to_datetime(forecast_date).month),
                "dow": int(pd.to_datetime(forecast_date).dayofweek),
                "is_weekend": bool(pd.to_datetime(forecast_date).dayofweek >= 5),
            }

            if complete_actuals:
                for period in ["all", "on_peak", "off_peak"]:
                    p_slice = build_period_slice(
                        actual_by_he=actual_by_he,
                        point_by_he=fresult.point_by_he,
                        quantiles_by_he=fresult.quantiles_by_he,
                        quantiles=adapter.quantiles,
                        hours=period_hours(period),
                    )
                    if p_slice is None:
                        continue
                    metrics = evaluate_period_slice(
                        period_slice=p_slice,
                        quantiles=adapter.quantiles,
                    )
                    daily_rows.append(
                        {
                            **common,
                            "period": period,
                            **metrics,
                        },
                    )

            run_rows.append(
                {
                    "model": adapter.name,
                    "forecast_date": forecast_date,
                    "reference_date": fresult.reference_date,
                    "status": "ok",
                    "n_actual_hours": len(actual_by_he),
                    "n_pred_hours": len(fresult.point_by_he),
                    "metadata_json": json.dumps(fresult.metadata, default=str),
                },
            )

    df_hourly = pd.DataFrame(hourly_rows)
    df_daily = pd.DataFrame(daily_rows)
    df_run = pd.DataFrame(run_rows)
    df_agg = _aggregate_metrics(df_daily)

    return BacktestResult(
        hourly_predictions=df_hourly,
        daily_metrics=df_daily,
        aggregate_metrics=df_agg,
        run_metadata=df_run,
    )
