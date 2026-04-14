"""Adapter normalization tests for like-day and LASSO QR wrappers."""
from __future__ import annotations

from datetime import date

import pandas as pd

from src.backtesting.adapters.lasso_qr import LassoQRAdapter
from src.backtesting.adapters.like_day import LikeDayAdapter


def _mock_pipeline_result(forecast_date: date, reference_date: date) -> dict:
    output = pd.DataFrame(
        [
            {"Date": forecast_date, "Type": "Forecast", **{f"HE{h}": float(h) for h in range(1, 25)}},
        ]
    )
    quantiles = pd.DataFrame(
        [
            {"Date": forecast_date, "Type": "P10", **{f"HE{h}": float(h - 1) for h in range(1, 25)}},
            {"Date": forecast_date, "Type": "P50", **{f"HE{h}": float(h) for h in range(1, 25)}},
            {"Date": forecast_date, "Type": "P90", **{f"HE{h}": float(h + 1) for h in range(1, 25)}},
        ]
    )
    return {
        "output_table": output,
        "quantiles_table": quantiles,
        "forecast_date": str(forecast_date),
        "reference_date": str(reference_date),
        "n_analogs_used": 10,
        "has_actuals": False,
        "model_info": {"alpha": 0.1},
    }


def test_like_day_adapter_normalizes_rows(monkeypatch) -> None:
    from src.backtesting.adapters import like_day as mod

    def _fake_run(**kwargs):
        fd = pd.to_datetime(kwargs["forecast_date"]).date()
        return _mock_pipeline_result(fd, fd - pd.Timedelta(days=1))

    monkeypatch.setattr(mod, "run_like_day", _fake_run)
    adapter = LikeDayAdapter(quantiles=[0.10, 0.50, 0.90])
    out = adapter.forecast_for_date(date(2026, 4, 10))

    assert out.model == "like_day"
    assert out.reference_date == date(2026, 4, 9)
    assert out.point_by_he[1] == 1.0
    assert out.quantiles_by_he[1][0.10] == 0.0
    assert out.quantiles_by_he[1][0.90] == 2.0


def test_lasso_adapter_normalizes_rows(monkeypatch) -> None:
    from src.backtesting.adapters import lasso_qr as mod

    def _fake_run(config, df_features=None):
        fd = pd.to_datetime(config.forecast_date).date()
        return _mock_pipeline_result(fd, fd - pd.Timedelta(days=1))

    monkeypatch.setattr(mod, "run_lasso_qr", _fake_run)
    adapter = LassoQRAdapter(quantiles=[0.10, 0.50, 0.90])
    out = adapter.forecast_for_date(date(2026, 4, 10), force_retrain=True)

    assert out.model == "lasso_qr"
    assert out.reference_date == date(2026, 4, 9)
    assert out.point_by_he[24] == 24.0
    assert out.quantiles_by_he[24][0.10] == 23.0
