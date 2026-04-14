"""Tests for backtest HTML report generation."""
from datetime import date, timedelta

import pandas as pd

from src.backtesting.engine import BacktestResult
from src.backtesting.report import build_backtest_report


def _make_synthetic_result() -> BacktestResult:
    """Build a minimal BacktestResult with synthetic data."""
    dates = [date(2026, 4, 1) + timedelta(days=i) for i in range(3)]
    hourly_rows = []
    daily_rows = []
    for d in dates:
        for h in range(1, 25):
            hourly_rows.append({
                "model": "fake",
                "forecast_date": d,
                "reference_date": d - timedelta(days=1),
                "hour_ending": h,
                "period": "on_peak" if 8 <= h < 24 else "off_peak",
                "forecast": 30.0 + h,
                "actual": 29.0 + h,
                "error": 1.0,
                "q_0.10": 27.0 + h,
                "q_0.25": 28.0 + h,
                "q_0.50": 30.0 + h,
                "q_0.75": 32.0 + h,
                "q_0.90": 33.0 + h,
            })
        for period in ["all", "on_peak", "off_peak"]:
            daily_rows.append({
                "model": "fake",
                "forecast_date": d,
                "reference_date": d - timedelta(days=1),
                "month": d.month,
                "dow": d.weekday(),
                "is_weekend": d.weekday() >= 5,
                "period": period,
                "mae": 1.0,
                "rmse": 1.2,
                "mape": 3.0,
                "bias": 0.5,
            })

    run_rows = [
        {"model": "fake", "forecast_date": d, "status": "ok",
         "n_actual_hours": 24, "n_pred_hours": 24}
        for d in dates
    ]

    agg = pd.DataFrame([{
        "model": "fake", "period": "all",
        "mae": 1.0, "rmse": 1.2, "mape": 3.0, "bias": 0.5,
    }])

    return BacktestResult(
        hourly_predictions=pd.DataFrame(hourly_rows),
        daily_metrics=pd.DataFrame(daily_rows),
        aggregate_metrics=agg,
        run_metadata=pd.DataFrame(run_rows),
    )


def test_build_backtest_report_returns_valid_html():
    result = _make_synthetic_result()
    html = build_backtest_report(result)
    assert "<!DOCTYPE html>" in html or "<html" in html
    assert "Backtest Report" in html
    # Should have per-day sections with quantile bands
    assert "Quantile Bands vs Actual" in html
    assert "Forecast" in html
    assert "Actual" in html
    # Should have the bands table rows
    assert "P10" in html
    assert "P90" in html


def test_build_backtest_report_handles_empty_result():
    result = BacktestResult(
        hourly_predictions=pd.DataFrame(),
        daily_metrics=pd.DataFrame(),
        aggregate_metrics=pd.DataFrame(),
        run_metadata=pd.DataFrame(),
    )
    html = build_backtest_report(result)
    assert "<!DOCTYPE html>" in html or "<html" in html
