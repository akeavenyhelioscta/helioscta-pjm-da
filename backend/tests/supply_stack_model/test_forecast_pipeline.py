from __future__ import annotations

from datetime import date

import pandas as pd

from src.supply_stack_model.configs import SupplyStackConfig
from src.supply_stack_model.pipelines.forecast import run


def _mock_hourly_inputs(target: date) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [target] * 24,
            "hour_ending": list(range(1, 25)),
            "load_mw": [80000.0] * 24,
            "solar_mw": [3000.0] * 24,
            "wind_mw": [7000.0] * 24,
            "net_load_mw": [70000.0] * 24,
            "gas_price_usd_mmbtu": [2.75] * 24,
            "outages_mw": [12000.0] * 24,
        }
    )


def _mock_fleet() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "block_id": "nuclear",
                "fuel_type": "nuclear",
                "capacity_mw": 35000.0,
                "heat_rate_mmbtu_mwh": 0.0,
                "vom_usd_mwh": 2.0,
                "must_run": True,
                "gas_hub": "",
                "outage_weight": 0.0,
            },
            {
                "block_id": "cc",
                "fuel_type": "cc_gas",
                "capacity_mw": 45000.0,
                "heat_rate_mmbtu_mwh": 7.0,
                "vom_usd_mwh": 2.0,
                "must_run": False,
                "gas_hub": "gas_m3",
                "outage_weight": 1.0,
            },
        ]
    )


def test_run_pipeline_with_mocked_inputs(monkeypatch) -> None:
    target = date(2026, 4, 14)
    monkeypatch.setattr(
        "src.supply_stack_model.pipelines.forecast.pull_hourly_inputs",
        lambda **_: _mock_hourly_inputs(target),
    )
    monkeypatch.setattr(
        "src.supply_stack_model.pipelines.forecast.load_fleet",
        lambda *_: _mock_fleet(),
    )
    monkeypatch.setattr(
        "src.supply_stack_model.pipelines.forecast._pull_actuals",
        lambda **_: None,
    )

    cfg = SupplyStackConfig(
        forecast_date=target,
        n_monte_carlo_draws=40,
        quantiles=[0.1, 0.5, 0.9],
    )
    result = run(config=cfg)

    assert "output_table" in result
    assert "quantiles_table" in result
    assert len(result["df_forecast"]) == 24
    assert not result["has_actuals"]
    assert "q_0.50" in result["df_forecast"].columns
    assert set(result["df_forecast"]["marginal_fuel"]) == {"cc_gas"}
