"""Layer 4: Forecast/pipeline contract tests — output shape and reproducibility."""
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
            "load_mw": [80000.0 + h * 500 for h in range(1, 25)],
            "solar_mw": [0.0] * 7 + [2000.0 + h * 200 for h in range(8, 18)] + [0.0] * 7,
            "wind_mw": [5000.0] * 24,
            "net_load_mw": [75000.0 + h * 300 for h in range(1, 25)],
            "gas_price_usd_mmbtu": [2.80] * 9 + [3.20] * 15,
            "outages_mw": [12000.0] * 24,
        }
    )


def _mock_fleet() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "block_id": "nuclear",
                "fuel_type": "nuclear",
                "capacity_mw": 33000.0,
                "heat_rate_mmbtu_mwh": 0.0,
                "vom_usd_mwh": 2.0,
                "must_run": True,
                "gas_hub": "",
                "outage_weight": 0.0,
            },
            {
                "block_id": "coal",
                "fuel_type": "coal",
                "capacity_mw": 25000.0,
                "heat_rate_mmbtu_mwh": 10.0,
                "vom_usd_mwh": 4.0,
                "must_run": False,
                "gas_hub": "",
                "outage_weight": 1.0,
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
            {
                "block_id": "ct",
                "fuel_type": "ct_gas",
                "capacity_mw": 15000.0,
                "heat_rate_mmbtu_mwh": 10.5,
                "vom_usd_mwh": 3.5,
                "must_run": False,
                "gas_hub": "gas_m3",
                "outage_weight": 1.0,
            },
        ]
    )


def _run_mocked_pipeline(monkeypatch, target: date, **config_kwargs) -> dict:
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
        quantiles=[0.10, 0.25, 0.50, 0.75, 0.90],
        **config_kwargs,
    )
    return run(config=cfg)


# ---------------------------------------------------------------------------
# Output shape
# ---------------------------------------------------------------------------


def test_pipeline_returns_24_hourly_rows(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    assert len(result["df_forecast"]) == 24


def test_pipeline_hour_endings_complete(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    hes = sorted(result["df_forecast"]["hour_ending"].astype(int).tolist())
    assert hes == list(range(1, 25))


# ---------------------------------------------------------------------------
# Required output keys
# ---------------------------------------------------------------------------


def test_pipeline_output_has_required_keys(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    required = {"output_table", "quantiles_table", "df_forecast", "metrics", "config"}
    assert required.issubset(set(result.keys()))


def test_pipeline_config_key_contains_model_params(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    cfg = result["config"]
    for key in ("hub", "region", "congestion_adder_usd", "n_monte_carlo_draws"):
        assert key in cfg, f"Missing config key: {key}"


# ---------------------------------------------------------------------------
# df_forecast required columns
# ---------------------------------------------------------------------------


def test_df_forecast_has_required_columns(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    df = result["df_forecast"]
    required_cols = {
        "hour_ending",
        "point_forecast",
        "marginal_fuel",
        "marginal_heat_rate",
        "reserve_margin_mw",
        "stack_position_pct",
        "dispatch_status",
        "net_load_mw",
        "gas_price_usd_mmbtu",
    }
    assert required_cols.issubset(set(df.columns))


def test_df_forecast_has_quantile_columns(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    df = result["df_forecast"]
    for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
        col = f"q_{q:.2f}"
        assert col in df.columns, f"Missing quantile column: {col}"


# ---------------------------------------------------------------------------
# Output table shape
# ---------------------------------------------------------------------------


def test_output_table_has_forecast_row(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    ot = result["output_table"]
    assert "Forecast" in ot["Type"].values


def test_quantiles_table_has_expected_bands(monkeypatch) -> None:
    result = _run_mocked_pipeline(monkeypatch, date(2026, 4, 14))
    qt = result["quantiles_table"]
    expected_labels = {"P10", "P25", "P50", "P75", "P90"}
    actual_labels = set(qt["Type"].values)
    assert expected_labels.issubset(actual_labels)


# ---------------------------------------------------------------------------
# Deterministic reproducibility
# ---------------------------------------------------------------------------


def test_deterministic_with_fixed_seed(monkeypatch) -> None:
    target = date(2026, 4, 14)
    result1 = _run_mocked_pipeline(monkeypatch, target, monte_carlo_seed=42)
    result2 = _run_mocked_pipeline(monkeypatch, target, monte_carlo_seed=42)

    df1 = result1["df_forecast"]
    df2 = result2["df_forecast"]
    for col in ["point_forecast", "q_0.10", "q_0.50", "q_0.90"]:
        vals1 = df1[col].tolist()
        vals2 = df2[col].tolist()
        assert vals1 == vals2, f"Non-deterministic output in column {col}"
