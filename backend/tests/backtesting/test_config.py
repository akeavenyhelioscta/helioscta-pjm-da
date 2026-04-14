"""Tests for BacktestConfig, especially weekdays_only filtering."""
from src.backtesting.config import BacktestConfig


def test_forecast_dates_all_days():
    # 2026-04-06 (Mon) through 2026-04-12 (Sun) = 7 days
    cfg = BacktestConfig(start_date="2026-04-06", end_date="2026-04-12")
    dates = cfg.forecast_dates()
    assert len(dates) == 7


def test_forecast_dates_weekdays_only():
    # Same week: should drop Sat + Sun, leaving Mon-Fri = 5
    cfg = BacktestConfig(
        start_date="2026-04-06",
        end_date="2026-04-12",
        weekdays_only=True,
    )
    dates = cfg.forecast_dates()
    assert len(dates) == 5
    for d in dates:
        assert d.weekday() < 5, f"{d} is a weekend day"


def test_forecast_dates_weekdays_only_with_max_days():
    # Two full weeks of weekdays = 10, but max_days=3 caps it
    cfg = BacktestConfig(
        start_date="2026-04-06",
        end_date="2026-04-17",
        weekdays_only=True,
        max_days=3,
    )
    dates = cfg.forecast_dates()
    assert len(dates) == 3
    for d in dates:
        assert d.weekday() < 5


def test_forecast_dates_weekdays_only_no_weekdays_in_range():
    # 2026-04-11 (Sat) through 2026-04-12 (Sun) — weekend only
    cfg = BacktestConfig(
        start_date="2026-04-11",
        end_date="2026-04-12",
        weekdays_only=True,
    )
    dates = cfg.forecast_dates()
    assert len(dates) == 0
