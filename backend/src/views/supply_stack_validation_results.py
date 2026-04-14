"""Validation view model for supply stack forecast — input quality, stack
invariants, sensitivity analysis, and forecast summary."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.supply_stack_model.configs import SupplyStackConfig
from src.supply_stack_model.data.fleet import load_fleet
from src.supply_stack_model.data.sources import pull_hourly_inputs
from src.supply_stack_model.pipelines.forecast import run as run_forecast
from src.supply_stack_model.stack.dispatch import dispatch
from src.supply_stack_model.stack.merit_order import build_merit_order

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


# ---------------------------------------------------------------------------
# Check helpers
# ---------------------------------------------------------------------------


def _check(name: str, passed: bool, detail: str = "") -> dict:
    return {"check": name, "status": "pass" if passed else "fail", "detail": detail}


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(out) else out


# ---------------------------------------------------------------------------
# 1. Input quality checks
# ---------------------------------------------------------------------------


def _run_input_quality_checks(df_inputs: pd.DataFrame) -> list[dict]:
    checks: list[dict] = []

    # Row count
    n = len(df_inputs)
    checks.append(_check("row_count_24", n == 24, f"got {n} rows"))

    # Hour completeness
    hours = sorted(df_inputs["hour_ending"].astype(int).tolist())
    checks.append(_check(
        "hour_completeness",
        hours == list(range(1, 25)),
        f"hours={hours[:5]}...{hours[-3:]}" if hours != list(range(1, 25)) else "HE1-24 complete",
    ))

    # Null checks
    for col in ("load_mw", "gas_price_usd_mmbtu", "net_load_mw", "outages_mw"):
        if col in df_inputs.columns:
            n_null = int(df_inputs[col].isna().sum())
            checks.append(_check(f"no_nulls_{col}", n_null == 0, f"{n_null} nulls"))

    # Range plausibility
    if "load_mw" in df_inputs.columns:
        load_min = float(df_inputs["load_mw"].min())
        load_max = float(df_inputs["load_mw"].max())
        ok = 20_000 <= load_min and load_max <= 200_000
        checks.append(_check(
            "load_range_plausible",
            ok,
            f"min={load_min:,.0f} max={load_max:,.0f} MW",
        ))

    if "gas_price_usd_mmbtu" in df_inputs.columns:
        gas_min = float(df_inputs["gas_price_usd_mmbtu"].min())
        gas_max = float(df_inputs["gas_price_usd_mmbtu"].max())
        ok = 0.5 <= gas_min and gas_max <= 50.0
        checks.append(_check(
            "gas_price_range_plausible",
            ok,
            f"min=${gas_min:.2f} max=${gas_max:.2f}/MMBtu",
        ))

    if "solar_mw" in df_inputs.columns:
        solar_min = float(df_inputs["solar_mw"].min())
        checks.append(_check("solar_non_negative", solar_min >= 0, f"min={solar_min:,.0f}"))

    if "wind_mw" in df_inputs.columns:
        wind_min = float(df_inputs["wind_mw"].min())
        checks.append(_check("wind_non_negative", wind_min >= 0, f"min={wind_min:,.0f}"))

    if "outages_mw" in df_inputs.columns:
        out_min = float(df_inputs["outages_mw"].min())
        checks.append(_check("outages_non_negative", out_min >= 0, f"min={out_min:,.0f}"))

    return checks


# ---------------------------------------------------------------------------
# 2. Stack mechanics invariants
# ---------------------------------------------------------------------------


def _run_stack_invariant_checks(
    fleet_df: pd.DataFrame,
    gas_price: float,
    outage_mw: float,
    coal_price: float,
    oil_price: float,
) -> list[dict]:
    checks: list[dict] = []

    merit = build_merit_order(
        fleet_df=fleet_df,
        gas_price_usd_mmbtu=gas_price,
        outage_mw=outage_mw,
        coal_price_usd_mmbtu=coal_price,
        oil_price_usd_mmbtu=oil_price,
    )

    # Non-decreasing variable cost
    costs = merit["variable_cost_usd_mwh"].tolist()
    non_decreasing = all(costs[i] >= costs[i - 1] for i in range(1, len(costs)))
    checks.append(_check(
        "merit_order_non_decreasing",
        non_decreasing,
        f"{len(costs)} blocks, range ${min(costs):.2f}-${max(costs):.2f}",
    ))

    # Monotonic cumulative capacity
    cum = merit["cumulative_capacity_mw"].tolist()
    monotonic = all(cum[i] > cum[i - 1] for i in range(1, len(cum)))
    checks.append(_check(
        "cumulative_capacity_monotonic",
        monotonic,
        f"total={cum[-1]:,.0f} MW" if cum else "empty",
    ))

    # Available capacity bounds
    cap_ok = (merit["available_capacity_mw"] >= 0).all() and (
        merit["available_capacity_mw"] <= merit["capacity_mw"]
    ).all()
    checks.append(_check(
        "capacity_bounds_valid",
        bool(cap_ok),
        f"min_avail={merit['available_capacity_mw'].min():,.0f}, "
        f"max_nameplate={merit['capacity_mw'].max():,.0f}",
    ))

    # Block count
    n_blocks = len(merit)
    checks.append(_check(
        "positive_block_count",
        n_blocks > 0,
        f"{n_blocks} blocks with available capacity > 0",
    ))

    return checks


# ---------------------------------------------------------------------------
# 3. Sensitivity analysis
# ---------------------------------------------------------------------------


def _run_sensitivity(
    fleet_df: pd.DataFrame,
    df_inputs: pd.DataFrame,
    config: SupplyStackConfig,
) -> list[dict]:
    """Re-dispatch with perturbed inputs and report price deltas."""

    def _dispatch_prices(
        gas_mult: float = 1.0,
        outage_add: float = 0.0,
        load_mult: float = 1.0,
    ) -> dict[int, float]:
        prices: dict[int, float] = {}
        for row in df_inputs.itertuples(index=False):
            gas = float(row.gas_price_usd_mmbtu) * gas_mult
            outage = max(0.0, float(row.outages_mw) + outage_add)
            net_load = float(row.net_load_mw) * load_mult

            merit = build_merit_order(
                fleet_df=fleet_df,
                gas_price_usd_mmbtu=gas,
                outage_mw=outage,
                coal_price_usd_mmbtu=config.coal_price_usd_mmbtu,
                oil_price_usd_mmbtu=config.oil_price_usd_mmbtu,
            )
            result = dispatch(
                merit_order_df=merit,
                net_load_mw=net_load,
                congestion_adder_usd=config.congestion_adder_usd,
                scarcity_price_cap_usd_mwh=config.scarcity_price_cap_usd_mwh,
            )
            prices[int(row.hour_ending)] = result["price"]
        return prices

    base_prices = _dispatch_prices()

    def _period_avg(prices: dict[int, float], hours: list[int]) -> float | None:
        vals = [prices[h] for h in hours if h in prices]
        return float(np.mean(vals)) if vals else None

    def _delta_summary(scenario_prices: dict[int, float]) -> dict:
        base_on = _period_avg(base_prices, ONPEAK_HOURS)
        base_off = _period_avg(base_prices, OFFPEAK_HOURS)
        base_flat = _period_avg(base_prices, list(range(1, 25)))
        scen_on = _period_avg(scenario_prices, ONPEAK_HOURS)
        scen_off = _period_avg(scenario_prices, OFFPEAK_HOURS)
        scen_flat = _period_avg(scenario_prices, list(range(1, 25)))
        return {
            "on_peak_delta": round(scen_on - base_on, 4) if (scen_on and base_on) else None,
            "off_peak_delta": round(scen_off - base_off, 4) if (scen_off and base_off) else None,
            "flat_delta": round(scen_flat - base_flat, 4) if (scen_flat and base_flat) else None,
        }

    scenarios = [
        {"name": "gas_+1%", "kwargs": {"gas_mult": 1.01}},
        {"name": "gas_+5%", "kwargs": {"gas_mult": 1.05}},
        {"name": "outages_+1GW", "kwargs": {"outage_add": 1000.0}},
        {"name": "outages_+5GW", "kwargs": {"outage_add": 5000.0}},
        {"name": "load_+1%", "kwargs": {"load_mult": 1.01}},
        {"name": "load_+5%", "kwargs": {"load_mult": 1.05}},
    ]

    results: list[dict] = []
    for s in scenarios:
        scen_prices = _dispatch_prices(**s["kwargs"])
        deltas = _delta_summary(scen_prices)
        results.append({"scenario": s["name"], **deltas})
    return results


# ---------------------------------------------------------------------------
# 4. Forecast summary
# ---------------------------------------------------------------------------


def _build_forecast_summary(pipeline_result: dict) -> dict:
    df = pipeline_result["df_forecast"]

    fuel_counts = df["marginal_fuel"].value_counts().to_dict()
    total = len(df)
    fuel_pcts = {k: round(v / total * 100, 1) for k, v in fuel_counts.items()}

    def _period_mean(col: str, hours: list[int]) -> float | None:
        vals = df[df["hour_ending"].isin(hours)][col]
        return round(float(vals.mean()), 2) if len(vals) > 0 else None

    return {
        "forecast_date": pipeline_result["forecast_date"],
        "has_actuals": pipeline_result["has_actuals"],
        "marginal_fuel_distribution": fuel_pcts,
        "on_peak_avg_price": _period_mean("point_forecast", ONPEAK_HOURS),
        "off_peak_avg_price": _period_mean("point_forecast", OFFPEAK_HOURS),
        "flat_avg_price": _period_mean("point_forecast", list(range(1, 25))),
        "metrics": pipeline_result.get("metrics"),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_validation_view_model(config: SupplyStackConfig) -> dict:
    """Run all validation checks and return structured results."""
    target_date = config.resolved_forecast_date()

    # Pull inputs
    try:
        df_inputs = pull_hourly_inputs(
            forecast_date=target_date,
            region=config.region,
            region_preset=config.region_preset,
            gas_hub_col=config.gas_hub_col,
            outage_column=config.outage_column,
            outages_lookback_days=config.outages_lookback_days,
        )
    except Exception as exc:
        return {"error": f"Failed to pull inputs: {exc}"}

    # Load fleet
    try:
        fleet_df = load_fleet(config.fleet_csv_path)
    except Exception as exc:
        return {"error": f"Failed to load fleet: {exc}"}

    # 1. Input quality
    input_quality = _run_input_quality_checks(df_inputs)

    # 2. Stack invariants (use peak-hour gas price)
    peak_row = df_inputs[df_inputs["hour_ending"] == 16]
    if len(peak_row) > 0:
        peak_gas = float(peak_row.iloc[0]["gas_price_usd_mmbtu"])
        peak_outage = float(peak_row.iloc[0]["outages_mw"])
    else:
        peak_gas = float(df_inputs["gas_price_usd_mmbtu"].median())
        peak_outage = float(df_inputs["outages_mw"].median())

    stack_invariants = _run_stack_invariant_checks(
        fleet_df=fleet_df,
        gas_price=peak_gas,
        outage_mw=peak_outage,
        coal_price=config.coal_price_usd_mmbtu,
        oil_price=config.oil_price_usd_mmbtu,
    )

    # 3. Sensitivity
    sensitivity = _run_sensitivity(fleet_df, df_inputs, config)

    # 4. Forecast summary
    try:
        pipeline_result = run_forecast(config=config)
        forecast_summary = _build_forecast_summary(pipeline_result)
    except Exception as exc:
        logger.warning("Pipeline run failed during validation: %s", exc)
        forecast_summary = {"error": str(exc)}

    all_checks = input_quality + stack_invariants
    n_pass = sum(1 for c in all_checks if c["status"] == "pass")
    n_fail = sum(1 for c in all_checks if c["status"] == "fail")

    return {
        "forecast_date": str(target_date),
        "region": config.region,
        "region_preset": config.region_preset,
        "overall_status": "pass" if n_fail == 0 else "fail",
        "checks_passed": n_pass,
        "checks_failed": n_fail,
        "input_quality": input_quality,
        "stack_invariants": stack_invariants,
        "sensitivity": sensitivity,
        "forecast_summary": forecast_summary,
    }
