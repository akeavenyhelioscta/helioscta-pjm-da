"""Single-day supply stack forecast pipeline."""
from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.data import lmps_hourly
from src.supply_stack_model.configs import SupplyStackConfig
from src.supply_stack_model.data.fleet import load_fleet
from src.supply_stack_model.data.sources import pull_hourly_inputs
from src.supply_stack_model.stack.dispatch import dispatch
from src.supply_stack_model.stack.merit_order import build_merit_order
from src.supply_stack_model.uncertainty.monte_carlo import monte_carlo_dispatch

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def _quantile_label(q: float) -> str:
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def _add_summary_cols(row_dict: dict) -> dict:
    onpeak_vals = [row_dict.get(f"HE{h}") for h in ONPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    offpeak_vals = [row_dict.get(f"HE{h}") for h in OFFPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    all_vals = [row_dict.get(f"HE{h}") for h in range(1, 25) if row_dict.get(f"HE{h}") is not None]

    row_dict["OnPeak"] = float(np.mean(onpeak_vals)) if onpeak_vals else np.nan
    row_dict["OffPeak"] = float(np.mean(offpeak_vals)) if offpeak_vals else np.nan
    row_dict["Flat"] = float(np.mean(all_vals)) if all_vals else np.nan
    return row_dict


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actuals_hourly: dict[int, float] | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []

    if actuals_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in range(1, 25):
            actual_row[f"HE{h}"] = actuals_hourly.get(h)
        rows.append(_add_summary_cols(actual_row))

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in range(1, 25):
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    rows.append(_add_summary_cols(forecast_row))

    if actuals_hourly is not None:
        error_row = {"Date": target_date, "Type": "Error"}
        for h in range(1, 25):
            a = actuals_hourly.get(h)
            f = forecast_hourly.get(h)
            error_row[f"HE{h}"] = (f - a) if (a is not None and f is not None) else None
        rows.append(_add_summary_cols(error_row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _build_quantiles_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    rows: list[dict] = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col not in df_forecast.columns:
            continue
        q_row = {"Date": target_date, "Type": _quantile_label(q)}
        q_map = dict(
            zip(
                df_forecast["hour_ending"].astype(int),
                df_forecast[col].astype(float),
            )
        )
        for h in range(1, 25):
            q_row[f"HE{h}"] = q_map.get(h)
        rows.append(_add_summary_cols(q_row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _pull_actuals(
    target_date: date,
    schema: str,
    hub: str,
) -> dict[int, float] | None:
    try:
        df = lmps_hourly.pull(
            schema=schema,
            hub=hub,
            market="da",
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not pull DA actuals for %s (%s): %s", target_date, hub, exc)
        return None

    day = df[pd.to_datetime(df["date"]).dt.date == target_date].copy()
    if len(day) < 24:
        return None
    day = day.sort_values("hour_ending")
    return dict(zip(day["hour_ending"].astype(int), day["lmp_total"].astype(float)))


def _compute_metrics(
    actuals_hourly: dict[int, float] | None,
    forecast_hourly: dict[int, float],
) -> dict | None:
    if actuals_hourly is None:
        return None
    hours = sorted(set(actuals_hourly.keys()).intersection(forecast_hourly.keys()))
    if not hours:
        return None
    y_true = np.array([actuals_hourly[h] for h in hours], dtype=float)
    y_pred = np.array([forecast_hourly[h] for h in hours], dtype=float)

    mae = float(np.mean(np.abs(y_pred - y_true)))
    rmse = float(np.sqrt(np.mean((y_pred - y_true) ** 2)))
    mape = float(np.mean(np.abs((y_pred - y_true) / np.maximum(1.0, np.abs(y_true)))) * 100.0)
    return {"mae": mae, "rmse": rmse, "mape": mape}


def run(
    config: SupplyStackConfig | None = None,
    **kwargs,
) -> dict:
    """Run supply stack forecast for one day."""
    if config is None:
        config = SupplyStackConfig(**kwargs)

    target_date = config.resolved_forecast_date()
    quantiles = config.sorted_quantiles()
    if 0.50 not in quantiles:
        quantiles = sorted(set([0.50, *quantiles]))

    logger.info("=" * 60)
    logger.info("Supply stack forecast for %s (%s)", target_date, config.hub)
    logger.info(
        "Scope: region=%s preset=%s gas_hub=%s",
        config.region,
        config.region_preset or "none",
        config.gas_hub_col or "auto",
    )
    logger.info("=" * 60)

    df_inputs = pull_hourly_inputs(
        forecast_date=target_date,
        region=config.region,
        region_preset=config.region_preset,
        gas_hub_col=config.gas_hub_col,
        outage_column=config.outage_column,
        outages_lookback_days=config.outages_lookback_days,
    )
    fleet_df = load_fleet(config.fleet_csv_path)

    hourly_rows: list[dict] = []
    for row in df_inputs.sort_values("hour_ending").itertuples(index=False):
        merit = build_merit_order(
            fleet_df=fleet_df,
            gas_price_usd_mmbtu=float(row.gas_price_usd_mmbtu),
            outage_mw=float(row.outages_mw),
            coal_price_usd_mmbtu=config.coal_price_usd_mmbtu,
            oil_price_usd_mmbtu=config.oil_price_usd_mmbtu,
        )
        dispatch_result = dispatch(
            merit_order_df=merit,
            net_load_mw=float(row.net_load_mw),
            congestion_adder_usd=config.congestion_adder_usd,
            scarcity_price_cap_usd_mwh=config.scarcity_price_cap_usd_mwh,
        )
        mc = monte_carlo_dispatch(
            fleet_df=fleet_df,
            net_load_mw=float(row.net_load_mw),
            gas_price_usd_mmbtu=float(row.gas_price_usd_mmbtu),
            outage_mw=float(row.outages_mw),
            quantiles=quantiles,
            n_draws=config.n_monte_carlo_draws,
            seed=(config.monte_carlo_seed + int(row.hour_ending))
            if config.monte_carlo_seed is not None
            else None,
            net_load_error_std_pct=config.net_load_error_std_pct,
            gas_price_error_std_pct=config.gas_price_error_std_pct,
            outage_error_std_pct=config.outage_error_std_pct,
            coal_price_usd_mmbtu=config.coal_price_usd_mmbtu,
            oil_price_usd_mmbtu=config.oil_price_usd_mmbtu,
            congestion_adder_usd=config.congestion_adder_usd,
            scarcity_price_cap_usd_mwh=config.scarcity_price_cap_usd_mwh,
        )

        out = {
            "date": row.date,
            "hour_ending": int(row.hour_ending),
            "point_forecast": float(dispatch_result["price"]),
            "marginal_fuel": dispatch_result["marginal_fuel"],
            "marginal_heat_rate": float(dispatch_result["marginal_heat_rate"]),
            "marginal_variable_cost": float(dispatch_result["marginal_variable_cost"]),
            "reserve_margin_mw": float(dispatch_result["reserve_margin_mw"]),
            "stack_position_pct": float(dispatch_result["stack_position_pct"])
            if dispatch_result["stack_position_pct"] is not None
            else None,
            "dispatch_status": dispatch_result["dispatch_status"],
            "shortage_mw": float(dispatch_result["shortage_mw"]),
            "load_mw": float(row.load_mw),
            "solar_mw": float(row.solar_mw),
            "wind_mw": float(row.wind_mw),
            "net_load_mw": float(row.net_load_mw),
            "gas_price_usd_mmbtu": float(row.gas_price_usd_mmbtu),
            "outages_mw": float(row.outages_mw),
        }
        for q, q_val in mc["quantiles"].items():
            out[f"q_{q:.2f}"] = float(q_val)
        hourly_rows.append(out)

    df_forecast = pd.DataFrame(hourly_rows).sort_values("hour_ending").reset_index(drop=True)
    forecast_hourly = dict(
        zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"].astype(float))
    )

    actuals_hourly = _pull_actuals(
        target_date=target_date,
        schema=config.schema,
        hub=config.hub,
    )
    has_actuals = actuals_hourly is not None

    output_table = _build_output_table(
        target_date=target_date,
        forecast_hourly=forecast_hourly,
        actuals_hourly=actuals_hourly,
    )
    quantiles_table = _build_quantiles_table(
        target_date=target_date,
        df_forecast=df_forecast,
        quantiles=quantiles,
    )
    metrics = _compute_metrics(actuals_hourly=actuals_hourly, forecast_hourly=forecast_hourly)

    return {
        "forecast_date": str(target_date),
        "reference_date": str(target_date - timedelta(days=1)),
        "has_actuals": has_actuals,
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "df_forecast": df_forecast,
        "metrics": metrics,
        "config": {
            "hub": config.hub,
            "region": config.region,
            "region_preset": config.region_preset,
            "gas_hub_col": config.gas_hub_col,
            "congestion_adder_usd": config.congestion_adder_usd,
            "coal_price_usd_mmbtu": config.coal_price_usd_mmbtu,
            "oil_price_usd_mmbtu": config.oil_price_usd_mmbtu,
            "n_monte_carlo_draws": config.n_monte_carlo_draws,
        },
    }


def main() -> None:  # pragma: no cover - CLI helper
    import logging as _logging

    _logging.basicConfig(level=_logging.INFO)
    result = run()
    print(result["output_table"].to_string(index=False))


if __name__ == "__main__":  # pragma: no cover - CLI helper
    main()
