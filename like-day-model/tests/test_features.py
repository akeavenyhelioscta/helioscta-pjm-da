"""Unit tests for feature modules (using synthetic data)."""
import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta

from pjm_like_day_forecast.features import (
    preprocessing,
    lmp_features,
    gas_features,
    load_features,
    calendar_features,
    composite,
)


# --- Synthetic data generators ---

def _make_lmp_hourly(n_days: int = 60, start_date: date = date(2025, 1, 1)) -> pd.DataFrame:
    """Generate synthetic hourly LMP data."""
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        for h in range(1, 25):
            base = 30 + 10 * np.sin(2 * np.pi * h / 24) + np.random.normal(0, 3)
            rows.append({
                "date": dt,
                "hour_ending": h,
                "lmp_total": base,
                "lmp_system_energy_price": base * 0.7,
                "lmp_congestion_price": base * 0.2,
                "lmp_marginal_loss_price": base * 0.1,
            })
    return pd.DataFrame(rows)


def _make_gas_prices(n_days: int = 60, start_date: date = date(2025, 1, 1)) -> pd.DataFrame:
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        m3 = 3.5 + np.random.normal(0, 0.3)
        rows.append({
            "date": dt,
            "gas_m3_price": m3,
            "gas_hh_price": m3 - 0.5 + np.random.normal(0, 0.1),
        })
    return pd.DataFrame(rows)


def _make_load_hourly(n_days: int = 60, start_date: date = date(2025, 1, 1)) -> pd.DataFrame:
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        for h in range(1, 25):
            load = 80000 + 20000 * np.sin(2 * np.pi * (h - 6) / 24) + np.random.normal(0, 2000)
            rows.append({"date": dt, "hour_ending": h, "da_load_mw": load})
    return pd.DataFrame(rows)


def _make_dates_daily(n_days: int = 60, start_date: date = date(2025, 1, 1)) -> pd.DataFrame:
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        dow = pd.Timestamp(dt).dayofweek
        rows.append({
            "date": dt,
            "day_of_week_number": dow,
            "is_weekend": dow >= 5,
            "is_nerc_holiday": False,
            "summer_winter": "WINTER" if pd.Timestamp(dt).month in [11, 12, 1, 2, 3] else "SUMMER",
        })
    return pd.DataFrame(rows)


# --- Tests ---

class TestPreprocessing:
    def test_asinh_roundtrip(self):
        x = pd.Series([0, 1, -1, 100, -100, 0.5])
        y = preprocessing.asinh_transform(x)
        x_back = preprocessing.asinh_inverse(y)
        np.testing.assert_allclose(x.values, x_back.values, atol=1e-10)

    def test_asinh_handles_zero(self):
        assert preprocessing.asinh_transform(pd.Series([0]))[0] == 0.0

    def test_standardize(self):
        values = np.array([10, 20, 30])
        result = preprocessing.standardize(values, mean=20, std=10)
        np.testing.assert_allclose(result, [-1, 0, 1])


class TestLmpFeatures:
    def test_output_shape(self):
        df = _make_lmp_hourly(n_days=60)
        result = lmp_features.build(df)
        assert "date" in result.columns
        # Should have ~24 profile cols + summary stats
        profile_cols = [c for c in result.columns if c.startswith("lmp_profile_h")]
        assert len(profile_cols) == 24
        assert len(result) == 60  # one row per day

    def test_no_nan_after_warmup(self):
        df = _make_lmp_hourly(n_days=60)
        result = lmp_features.build(df)
        # After 30-day warmup, rolling features should be populated
        late = result[result["date"] >= date(2025, 2, 1)]
        # Profile columns should never be NaN
        profile_cols = [c for c in late.columns if c.startswith("lmp_profile_h")]
        assert late[profile_cols].isna().sum().sum() == 0


class TestGasFeatures:
    def test_output_shape(self):
        df = _make_gas_prices(n_days=60)
        result = gas_features.build(df)
        assert "date" in result.columns
        assert "gas_m3_price" in result.columns
        assert "gas_m3_hh_spread" in result.columns
        assert len(result) == 60

    def test_spread_computation(self):
        df = _make_gas_prices(n_days=10)
        result = gas_features.build(df)
        for _, row in result.iterrows():
            orig = df[df["date"] == row["date"]].iloc[0]
            expected_spread = orig["gas_m3_price"] - orig["gas_hh_price"]
            assert row["gas_m3_hh_spread"] == pytest.approx(expected_spread, abs=1e-10)


class TestLoadFeatures:
    def test_output_shape(self):
        df = _make_load_hourly(n_days=60)
        result = load_features.build(df_da_load=df)
        assert "date" in result.columns
        assert "load_daily_avg" in result.columns
        assert "load_daily_peak" in result.columns
        assert len(result) == 60

    def test_peak_greater_than_avg(self):
        df = _make_load_hourly(n_days=30)
        result = load_features.build(df_da_load=df)
        assert (result["load_daily_peak"] >= result["load_daily_avg"]).all()

    def test_no_data_returns_empty(self):
        result = load_features.build()
        assert len(result) == 0


class TestCalendarFeatures:
    def test_output_shape(self):
        df = _make_dates_daily(n_days=60)
        result = calendar_features.build(df)
        assert "date" in result.columns
        assert "dow_group" in result.columns
        assert "dow_sin" in result.columns
        assert "day_of_year_sin" in result.columns
        assert len(result) == 60

    def test_cyclical_bounds(self):
        df = _make_dates_daily(n_days=365)
        result = calendar_features.build(df)
        assert result["dow_sin"].between(-1, 1).all()
        assert result["dow_cos"].between(-1, 1).all()
        assert result["month_sin"].between(-1, 1).all()
        assert result["month_cos"].between(-1, 1).all()

    def test_dow_one_hot_sums_to_one(self):
        df = _make_dates_daily(n_days=30)
        result = calendar_features.build(df)
        dow_cols = [f"dow_{d}" for d in range(7)]
        assert (result[dow_cols].sum(axis=1) == 1).all()


class TestCompositeFeatures:
    def test_implied_heat_rate(self):
        df_lmp = pd.DataFrame({"date": [date(2025, 1, 1)], "lmp_daily_flat": [np.arcsinh(50.0)]})
        df_gas = pd.DataFrame({"date": [date(2025, 1, 1)], "gas_m3_price": [5.0]})
        df_load = pd.DataFrame({"date": [date(2025, 1, 1)], "load_daily_avg": [80000]})
        result = composite.build(df_lmp, df_gas, df_load)
        assert "implied_heat_rate" in result.columns
        assert result["implied_heat_rate"].iloc[0] == pytest.approx(50.0 / 5.0, rel=0.01)
