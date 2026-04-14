"""Tests for shared backtesting engine."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import pytest

from src.backtesting.adapters.base import ForecastResult
from src.backtesting.config import BacktestConfig
from src.backtesting.engine import run_backtest


def _make_actuals(start: date, n_days: int) -> pd.DataFrame:
    rows = []
    for d in range(n_days):
        day = start + timedelta(days=d)
        for h in range(1, 25):
            rows.append(
                {
                    "date": day,
                    "hour_ending": h,
                    "lmp_total": float(20 + h + d),
                },
            )
    return pd.DataFrame(rows)


@dataclass
class _FakeAdapter:
    name: str
    quantiles: list[float]
    offset: float = 0.0
    bad_reference: bool = False
    calls: list[date] | None = None

    def forecast_for_date(self, forecast_date: date, force_retrain: bool = False) -> ForecastResult:
        _ = force_retrain
        if self.calls is not None:
            self.calls.append(forecast_date)
        reference_date = forecast_date if self.bad_reference else forecast_date - timedelta(days=1)

        point = {h: float(20 + h + self.offset) for h in range(1, 25)}
        qmap = {}
        for h in range(1, 25):
            base = point[h]
            qmap[h] = {
                0.10: base - 3.0,
                0.25: base - 1.5,
                0.50: base,
                0.75: base + 1.5,
                0.90: base + 3.0,
            }
        return ForecastResult(
            model=self.name,
            forecast_date=forecast_date,
            reference_date=reference_date,
            point_by_he=point,
            quantiles_by_he=qmap,
            metadata={"fake": True},
        )


def test_engine_runs_and_scores_with_synthetic_inputs() -> None:
    start = date(2026, 4, 1)
    df_actuals = _make_actuals(start, 2)
    cfg = BacktestConfig(
        start_date="2026-04-01",
        end_date="2026-04-02",
        models=["like_day"],
    )
    adapters = [_FakeAdapter(name="fake_a", quantiles=cfg.quantiles)]

    result = run_backtest(cfg, adapters=adapters, df_actuals=df_actuals)

    assert len(result.hourly_predictions) == 48
    assert len(result.daily_metrics) == 6  # 2 dates x 3 periods
    assert (result.daily_metrics["mae"] >= 0).all()
    assert "coverage_80pct" in result.daily_metrics.columns
    assert len(result.aggregate_metrics) > 0


def test_leakage_guard_raises_when_reference_not_prior_day() -> None:
    start = date(2026, 4, 1)
    df_actuals = _make_actuals(start, 1)
    cfg = BacktestConfig(
        start_date="2026-04-01",
        end_date="2026-04-01",
        models=["like_day"],
    )
    adapters = [_FakeAdapter(name="bad_ref", quantiles=cfg.quantiles, bad_reference=True)]

    with pytest.raises(ValueError, match="Leakage guard failed"):
        run_backtest(cfg, adapters=adapters, df_actuals=df_actuals)


def test_drop_incomplete_days_skips_metrics_and_hourly_rows() -> None:
    start = date(2026, 4, 1)
    df_actuals = _make_actuals(start, 1)
    # Drop one hour to make date incomplete.
    df_actuals = df_actuals[df_actuals["hour_ending"] != 24].copy()

    cfg = BacktestConfig(
        start_date="2026-04-01",
        end_date="2026-04-01",
        models=["like_day"],
        drop_incomplete_days=True,
    )
    adapters = [_FakeAdapter(name="fake_incomplete", quantiles=cfg.quantiles)]

    result = run_backtest(cfg, adapters=adapters, df_actuals=df_actuals)
    assert len(result.hourly_predictions) == 0
    assert len(result.daily_metrics) == 0
    assert len(result.run_metadata) == 1
    assert result.run_metadata.iloc[0]["status"] == "skipped_incomplete_actuals"


def test_engine_calls_adapters_in_ascending_date_order() -> None:
    start = date(2026, 4, 1)
    df_actuals = _make_actuals(start, 3)
    calls: list[date] = []
    cfg = BacktestConfig(
        start_date="2026-04-01",
        end_date="2026-04-03",
        models=["like_day"],
    )
    adapters = [_FakeAdapter(name="fake_calls", quantiles=cfg.quantiles, calls=calls)]

    _ = run_backtest(cfg, adapters=adapters, df_actuals=df_actuals)
    assert calls == [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]
