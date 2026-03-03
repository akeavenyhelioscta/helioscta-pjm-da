"""Unit tests for pre-filtering."""
import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta

from pjm_like_day_forecast.similarity.filtering import (
    calendar_filter,
    regime_filter,
    ensure_minimum_pool,
)


def _make_feature_df(n_days=365, start_date=date(2024, 1, 1)):
    """Generate synthetic daily feature DataFrame."""
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        dow = pd.Timestamp(dt).dayofweek
        if dow in [0, 1, 2]:
            dow_group = 0
        elif dow in [3, 4]:
            dow_group = 1
        elif dow == 5:
            dow_group = 2
        else:
            dow_group = 3

        rows.append({
            "date": dt,
            "dow_group": dow_group,
            "lmp_daily_flat": 30 + 10 * np.sin(2 * np.pi * d / 365) + np.random.normal(0, 5),
            "gas_m3_price": 3.5 + 0.5 * np.sin(2 * np.pi * d / 365) + np.random.normal(0, 0.3),
        })
    return pd.DataFrame(rows)


class TestCalendarFilter:
    def test_excludes_target_date(self):
        df = _make_feature_df()
        target = date(2024, 7, 1)
        result = calendar_filter(df, target, same_dow_group=False, season_window_days=0)
        assert target not in result["date"].values

    def test_excludes_future_dates(self):
        df = _make_feature_df()
        target = date(2024, 7, 1)
        result = calendar_filter(df, target, same_dow_group=False, season_window_days=0)
        assert all(d < target for d in result["date"])

    def test_dow_group_filter(self):
        df = _make_feature_df()
        target = date(2024, 7, 1)  # Monday -> dow_group 0
        result = calendar_filter(df, target, same_dow_group=True, season_window_days=0)
        assert (result["dow_group"] == 0).all()

    def test_season_window(self):
        df = _make_feature_df()
        target = date(2024, 7, 1)
        result = calendar_filter(df, target, same_dow_group=False, season_window_days=30)
        # All dates should be within 30 days of July 1 (DOY ~182)
        target_doy = pd.Timestamp(target).dayofyear
        for d in result["date"]:
            doy = pd.Timestamp(d).dayofyear
            diff = abs(doy - target_doy)
            diff = min(diff, 365 - diff)
            assert diff <= 30

    def test_reduces_pool(self):
        df = _make_feature_df()
        target = date(2024, 7, 1)
        result = calendar_filter(df, target, same_dow_group=True, season_window_days=30)
        assert len(result) < len(df)
        assert len(result) > 0


class TestRegimeFilter:
    def test_excludes_extreme_lmp(self):
        df = _make_feature_df(n_days=200)
        # Add an extreme outlier
        df.loc[50, "lmp_daily_flat"] = 200.0  # extreme spike
        target = date(2024, 7, 1)  # within the 200-day range
        result = regime_filter(df, target)
        # Pool should be smaller than original (excludes future + extreme outlier)
        full_historical = df[(df["date"] != target) & (df["date"] < target)]
        assert len(result) < len(full_historical)


class TestEnsureMinimumPool:
    def test_returns_minimum(self):
        df_full = _make_feature_df(n_days=100)
        df_filtered = df_full.head(5)  # very small
        target = date(2024, 12, 1)
        result = ensure_minimum_pool(df_filtered, df_full, target, min_size=20)
        assert len(result) >= 20

    def test_no_change_if_sufficient(self):
        df_full = _make_feature_df(n_days=100)
        df_filtered = df_full.head(50)
        target = date(2024, 12, 1)
        result = ensure_minimum_pool(df_filtered, df_full, target, min_size=20)
        assert len(result) == 50  # unchanged
