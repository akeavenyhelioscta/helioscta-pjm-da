"""Regression tests for LASSO QR alignment and inference safeguards."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.lasso_quantile_regression.configs import LassoQRConfig
from src.lasso_quantile_regression.features import builder as feature_builder
from src.lasso_quantile_regression.pipelines import forecast as forecast_pipe
from src.lasso_quantile_regression.pipelines import strip_forecast as strip_pipe
from src.lasso_quantile_regression.training import trainer


def test_model_dir_is_absolute_and_stable() -> None:
    cfg = LassoQRConfig()
    assert cfg.model_dir.is_absolute()
    assert cfg.model_dir.name == "models"
    assert cfg.model_dir.parent.name == "lasso_quantile_regression"


def test_pull_hourly_lmp_wide_shifts_delivery_to_reference_date(monkeypatch) -> None:
    raw = pd.DataFrame(
        [
            {
                "date": "2026-04-11",
                "hour_ending": 1,
                "lmp_total": 52.5,
                "hub": "WESTERN HUB",
            },
            {
                "date": "2026-04-11",
                "hour_ending": 2,
                "lmp_total": 55.0,
                "hub": "WESTERN HUB",
            },
        ]
    )
    monkeypatch.setattr(feature_builder, "pull_with_cache", lambda **_: raw)

    wide = feature_builder._pull_hourly_lmp_wide(LassoQRConfig())
    assert len(wide) == 1
    assert wide.iloc[0]["date"] == date(2026, 4, 10)
    assert wide.iloc[0]["target_HE1"] == 52.5
    assert wide.iloc[0]["target_HE2"] == 55.0


def test_forecast_quantiles_are_monotone_after_rearrangement() -> None:
    forecasts = {
        1: {0.10: 35.0, 0.50: 30.0, 0.90: 33.0},
        2: {0.10: 12.0, 0.50: 15.0, 0.90: 18.0},
    }
    forecast_pipe._enforce_monotonic_quantiles(forecasts, [0.10, 0.50, 0.90])

    for hour_preds in forecasts.values():
        assert hour_preds[0.10] <= hour_preds[0.50] <= hour_preds[0.90]


def test_output_table_actuals_use_reference_row_indexing() -> None:
    delivery = date(2026, 4, 11)
    ref_row_date = delivery - timedelta(days=1)
    row = {"date": ref_row_date}
    for h in range(1, 25):
        row[f"target_HE{h}"] = float(h)
    df = pd.DataFrame([row])
    forecasts = {h: {0.50: 100.0 + h} for h in range(1, 25)}

    out = forecast_pipe._build_output_table(df=df, forecast_date=delivery, forecasts=forecasts)
    actual = out[out["Type"] == "Actual"]
    assert len(actual) == 1
    assert actual.iloc[0]["HE1"] == 1.0
    assert actual.iloc[0]["HE24"] == 24.0


def test_strip_refreshes_lagged_features_from_predicted_and_historical() -> None:
    target_date = date(2026, 4, 13)
    ref_row = pd.DataFrame([{"lmp_lag1_HE1": -1.0, "lmp_lag2_HE1": -1.0, "lmp_lag7_HE1": -1.0}])

    # Historical mapping: delivery d is stored on row date d-1 in target_HE*.
    df_features = pd.DataFrame(
        [
            {"date": date(2026, 4, 10), "target_HE1": 77.0},  # delivery 2026-04-11 (lag2)
            {"date": date(2026, 4, 5), "target_HE1": 66.0},   # delivery 2026-04-06 (lag7)
        ]
    )
    predicted = {date(2026, 4, 12): {1: 88.0}}  # lag1 delivery gets forecast value

    strip_pipe._refresh_lagged_lmp_features(
        ref_row=ref_row,
        target_date=target_date,
        predicted_delivery_prices=predicted,
        df_features=df_features,
    )

    assert ref_row.iloc[0]["lmp_lag1_HE1"] == 88.0
    assert ref_row.iloc[0]["lmp_lag2_HE1"] == 77.0
    assert ref_row.iloc[0]["lmp_lag7_HE1"] == 66.0


def test_alpha_cv_uses_all_expanding_folds(monkeypatch) -> None:
    train_sizes: list[int] = []

    class _FakePipe:
        def fit(self, X, y, **kwargs):
            train_sizes.append(len(X))
            self._pred = float(np.mean(y))
            return self

        def predict(self, X):
            return np.full(len(X), self._pred)

    monkeypatch.setattr(trainer, "Pipeline", lambda *_args, **_kwargs: _FakePipe())
    monkeypatch.setattr(trainer, "StandardScaler", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(trainer, "QuantileRegressor", lambda **_kwargs: object())

    n = 8
    df_train = pd.DataFrame(
        {
            "x": np.arange(n, dtype=float),
            "target_HE8": np.arange(n, dtype=float),
            "target_HE12": np.arange(n, dtype=float),
            "target_HE17": np.arange(n, dtype=float),
            "target_HE20": np.arange(n, dtype=float),
        }
    )
    cfg = LassoQRConfig(alpha_grid=[0.1], quantiles=[0.50])

    alpha = trainer._select_alpha_cv(df_train=df_train, feature_cols=["x"], config=cfg, n_folds=3)
    assert alpha == 0.1
    assert sorted(set(train_sizes)) == [2, 4, 6]


def test_select_feature_cols_applies_pruning_and_force_include() -> None:
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 4, 1),
                "target_HE1": 10.0,
                "lmp_profile_h1": 30.0,
                "fuel_share_gas": 0.4,
                "gas_basis_m3_dom_south": 0.7,
                "congestion_daily_avg": 1.2,
                "tgt_load_daily_avg": 100000.0,
                "dart_spread_daily": 4.0,
                "rt_lmp_daily_flat": 38.0,
            }
        ]
    )
    cfg = LassoQRConfig(
        feature_set="full",
        include_lagged_lmp=False,
        include_interaction_terms=False,
    )

    cols = feature_builder._select_feature_cols(df, cfg)

    assert "lmp_profile_h1" not in cols
    assert "fuel_share_gas" not in cols
    assert "gas_basis_m3_dom_south" not in cols
    assert "congestion_daily_avg" not in cols
    assert "tgt_load_daily_avg" in cols
    assert "dart_spread_daily" in cols
    assert "rt_lmp_daily_flat" in cols
