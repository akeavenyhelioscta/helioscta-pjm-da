"""Regression-adjusted like-day forecast — correct for fundamental deltas.

After the analog blend, compare today's fundamentals against the analog-weighted
average. Translate deltas (nuclear deficit, outage surplus, load revision,
renewable change) into $/MWh adjustments using configurable sensitivities.

The adjustment is additive: each hour gets shifted by the sum of all
fundamental-delta contributions. On-peak and off-peak hours can have
different sensitivities.

Usage:
    from src.like_day_forecast.pipelines.regression_adjusted_forecast import run
    result = run()  # uses default sensitivities
    result = run(sensitivities={"nuclear_mw": {"onpeak": -2.5, "offpeak": -1.5}})
"""
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_base_forecast

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


# ── Default sensitivities ($/MWh per 1,000 MW delta) ────────────────
#
# Positive sensitivity = higher fundamental → higher price.
# Nuclear & renewables are inverted: more MW = lower price → negative.
#
# These are expert priors based on PJM shoulder-season marginal cost
# reasoning. Tune via backtesting or W&B sweeps.

DEFAULT_SENSITIVITIES: dict[str, dict[str, float]] = {
    # $/MWh per 1,000 MW of nuclear DEFICIT (less nuclear = higher price)
    "nuclear_mw": {"onpeak": -2.0, "offpeak": -1.0},
    # $/MWh per 1,000 MW of ADDITIONAL outages vs analogs
    "outage_total_mw": {"onpeak": 1.0, "offpeak": 0.5},
    # $/MWh per 1,000 MW of ADDITIONAL load vs analogs
    "load_mw": {"onpeak": 0.8, "offpeak": 0.4},
    # $/MWh per 1,000 MW of ADDITIONAL renewables (more = lower price)
    "renewable_mw": {"onpeak": -0.8, "offpeak": -0.5},
    # $/MWh per $/MWh of ADDITIONAL congestion vs analogs (pass-through)
    "congestion_dollar": {"onpeak": 0.6, "offpeak": 0.3},
}


@dataclass
class FundamentalDelta:
    """A single fundamental comparison: today vs analog-weighted average."""
    name: str
    label: str
    unit: str
    today_value: float
    analog_avg: float
    delta: float           # today - analog_avg (raw units)
    delta_normalized: float  # delta in sensitivity units (e.g., per 1,000 MW)
    sensitivity_onpeak: float
    sensitivity_offpeak: float
    adj_onpeak: float      # delta_normalized * sensitivity
    adj_offpeak: float


def run(
    forecast_date: str | None = None,
    sensitivities: dict[str, dict[str, float]] | None = None,
    config: configs.ScenarioConfig | None = None,
    **kwargs,
) -> dict:
    """Run the like-day forecast with regression adjustment for fundamental deltas.

    Args:
        forecast_date: YYYY-MM-DD, defaults to tomorrow.
        sensitivities: Override sensitivity coefficients. Keys match
            DEFAULT_SENSITIVITIES; values are {"onpeak": x, "offpeak": y}.
        config: Optional ScenarioConfig override.
        **kwargs: Passed through to base forecast pipeline.

    Returns:
        Dict with base forecast output plus adjusted tables and delta details.
    """
    sens = dict(DEFAULT_SENSITIVITIES)
    if sensitivities:
        for k, v in sensitivities.items():
            sens[k] = {**sens.get(k, {}), **v}

    # 1. Run base forecast (returns analogs, feature matrix via builder)
    base = run_base_forecast(forecast_date=forecast_date, config=config, **kwargs)

    if "error" in base:
        return base

    analogs_df = base["analogs"]
    output_table = base["output_table"]
    quantiles_table = base["quantiles_table"]
    df_forecast = base["df_forecast"]
    reference_date_str = base["reference_date"]
    reference_date = pd.to_datetime(reference_date_str).date()

    # 2. Rebuild feature matrix to access fundamental values
    #    (the base forecast already built this — we rebuild from cache, which is fast)
    from src.like_day_forecast.features.builder import build_daily_features

    cache_kwargs = {k: v for k, v in kwargs.items()
                    if k in ("cache_dir", "cache_enabled", "cache_ttl_hours", "force_refresh")}
    cfg = config or configs.ScenarioConfig(forecast_date=forecast_date)

    df_features = build_daily_features(
        schema=cfg.schema,
        hub=cfg.hub,
        renewable_mode=cfg.resolved_renewable_mode(),
        renewable_region=cfg.renewable_forecast_region,
        renewable_blend_pjm_weight=cfg.renewable_blend_weight(offset=1),
        **cache_kwargs,
    )

    # 3. Extract reference date's fundamentals
    ref_mask = df_features["date"] == reference_date
    if not ref_mask.any():
        logger.warning(f"Reference date {reference_date} not in feature matrix")
        return {**base, "adjustment": None, "deltas": []}

    ref_row = df_features[ref_mask].iloc[0]

    # 4. Extract analog dates' fundamentals and compute weighted average
    analog_dates = analogs_df["date"].tolist()
    analog_weights = analogs_df["weight"].values
    analog_mask = df_features["date"].isin(analog_dates)
    df_analog_feats = df_features[analog_mask].copy()

    # Align weights to feature rows (some analogs might be missing)
    date_to_weight = dict(zip(analogs_df["date"], analogs_df["weight"]))
    df_analog_feats["_weight"] = df_analog_feats["date"].map(date_to_weight)
    df_analog_feats = df_analog_feats.dropna(subset=["_weight"])
    weights = df_analog_feats["_weight"].values
    weights = weights / weights.sum()  # renormalize

    def _weighted_avg(col: str) -> float:
        vals = df_analog_feats[col].values
        w = weights.copy()
        mask = ~np.isnan(vals)
        if not mask.any():
            return np.nan
        return float(np.average(vals[mask], weights=w[mask]))

    # 5. Compute deltas for each fundamental
    deltas: list[FundamentalDelta] = []

    def _safe(val):
        return float(val) if pd.notna(val) else np.nan

    # Nuclear (MW) — more nuclear = lower price, so sensitivity is negative
    if "nuclear_daily_avg" in df_features.columns:
        today_val = _safe(ref_row.get("nuclear_daily_avg"))
        analog_val = _weighted_avg("nuclear_daily_avg")
        if not np.isnan(today_val) and not np.isnan(analog_val):
            delta = today_val - analog_val  # negative = deficit
            delta_norm = delta / 1000  # per 1,000 MW
            s = sens.get("nuclear_mw", {"onpeak": 0, "offpeak": 0})
            deltas.append(FundamentalDelta(
                name="nuclear_mw", label="Nuclear Generation",
                unit="MW", today_value=today_val, analog_avg=analog_val,
                delta=delta, delta_normalized=delta_norm,
                sensitivity_onpeak=s["onpeak"], sensitivity_offpeak=s["offpeak"],
                adj_onpeak=delta_norm * s["onpeak"],
                adj_offpeak=delta_norm * s["offpeak"],
            ))

    # Outages (MW) — target outage for D+1
    outage_col = "tgt_outage_total_mw" if "tgt_outage_total_mw" in df_features.columns else "outage_total_mw"
    if outage_col in df_features.columns:
        today_val = _safe(ref_row.get(outage_col))
        analog_val = _weighted_avg(outage_col)
        if not np.isnan(today_val) and not np.isnan(analog_val):
            delta = today_val - analog_val
            delta_norm = delta / 1000
            s = sens.get("outage_total_mw", {"onpeak": 0, "offpeak": 0})
            deltas.append(FundamentalDelta(
                name="outage_total_mw", label="Total Outages (D+1 Forecast)",
                unit="MW", today_value=today_val, analog_avg=analog_val,
                delta=delta, delta_normalized=delta_norm,
                sensitivity_onpeak=s["onpeak"], sensitivity_offpeak=s["offpeak"],
                adj_onpeak=delta_norm * s["onpeak"],
                adj_offpeak=delta_norm * s["offpeak"],
            ))

    # Load (MW) — target load for D+1
    load_col = "tgt_load_daily_avg" if "tgt_load_daily_avg" in df_features.columns else "load_daily_avg"
    if load_col in df_features.columns:
        today_val = _safe(ref_row.get(load_col))
        analog_val = _weighted_avg(load_col)
        if not np.isnan(today_val) and not np.isnan(analog_val):
            delta = today_val - analog_val
            delta_norm = delta / 1000
            s = sens.get("load_mw", {"onpeak": 0, "offpeak": 0})
            deltas.append(FundamentalDelta(
                name="load_mw", label="Load Forecast (D+1)",
                unit="MW", today_value=today_val, analog_avg=analog_val,
                delta=delta, delta_normalized=delta_norm,
                sensitivity_onpeak=s["onpeak"], sensitivity_offpeak=s["offpeak"],
                adj_onpeak=delta_norm * s["onpeak"],
                adj_offpeak=delta_norm * s["offpeak"],
            ))

    # Renewables (MW) — target renewable for D+1
    renew_col = "tgt_renewable_daily_avg" if "tgt_renewable_daily_avg" in df_features.columns else "renewable_daily_avg"
    if renew_col in df_features.columns:
        today_val = _safe(ref_row.get(renew_col))
        analog_val = _weighted_avg(renew_col)
        if not np.isnan(today_val) and not np.isnan(analog_val):
            delta = today_val - analog_val
            delta_norm = delta / 1000
            s = sens.get("renewable_mw", {"onpeak": 0, "offpeak": 0})
            deltas.append(FundamentalDelta(
                name="renewable_mw", label="Renewable Forecast (D+1)",
                unit="MW", today_value=today_val, analog_avg=analog_val,
                delta=delta, delta_normalized=delta_norm,
                sensitivity_onpeak=s["onpeak"], sensitivity_offpeak=s["offpeak"],
                adj_onpeak=delta_norm * s["onpeak"],
                adj_offpeak=delta_norm * s["offpeak"],
            ))

    # Congestion ($/MWh) — reference date congestion level
    if "congestion_onpeak_avg" in df_features.columns:
        today_val = _safe(ref_row.get("congestion_onpeak_avg"))
        analog_val = _weighted_avg("congestion_onpeak_avg")
        if not np.isnan(today_val) and not np.isnan(analog_val):
            delta = today_val - analog_val  # $/MWh delta
            s = sens.get("congestion_dollar", {"onpeak": 0, "offpeak": 0})
            deltas.append(FundamentalDelta(
                name="congestion_dollar", label="DA Congestion (On-Peak)",
                unit="$/MWh", today_value=today_val, analog_avg=analog_val,
                delta=delta, delta_normalized=delta,  # already in $/MWh
                sensitivity_onpeak=s["onpeak"], sensitivity_offpeak=s["offpeak"],
                adj_onpeak=delta * s["onpeak"],
                adj_offpeak=delta * s["offpeak"],
            ))

    # 6. Sum adjustments
    total_adj_onpeak = sum(d.adj_onpeak for d in deltas)
    total_adj_offpeak = sum(d.adj_offpeak for d in deltas)

    logger.info(f"Regression adjustment: on-peak {total_adj_onpeak:+.2f}, "
                f"off-peak {total_adj_offpeak:+.2f} (from {len(deltas)} factors)")
    for d in deltas:
        logger.info(f"  {d.label}: {d.delta:+,.0f} {d.unit} → "
                     f"on-peak {d.adj_onpeak:+.2f}, off-peak {d.adj_offpeak:+.2f}")

    # 7. Apply adjustment to hourly forecast
    hour_deltas = {}
    for h in range(1, 25):
        hour_deltas[h] = total_adj_onpeak if h in ONPEAK_HOURS else total_adj_offpeak

    # Adjusted point forecast
    fcst_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    adj_hourly = {}
    for h in range(1, 25):
        base_val = fcst_row.get(f"HE{h}")
        if pd.notna(base_val):
            adj_hourly[h] = base_val + hour_deltas[h]

    # Build adjusted output row
    target_date = base["forecast_date"]
    adj_row = {"Date": target_date, "Type": "Regression Adj"}
    for h in range(1, 25):
        adj_row[f"HE{h}"] = adj_hourly.get(h)

    onpeak_vals = [adj_hourly[h] for h in ONPEAK_HOURS if h in adj_hourly]
    offpeak_vals = [adj_hourly[h] for h in OFFPEAK_HOURS if h in adj_hourly]
    all_vals = [adj_hourly[h] for h in range(1, 25) if h in adj_hourly]
    adj_row["OnPeak"] = np.mean(onpeak_vals) if onpeak_vals else np.nan
    adj_row["OffPeak"] = np.mean(offpeak_vals) if offpeak_vals else np.nan
    adj_row["Flat"] = np.mean(all_vals) if all_vals else np.nan

    adj_output = pd.concat([output_table, pd.DataFrame([adj_row])], ignore_index=True)

    # Adjusted quantile bands
    adj_q_rows = []
    for _, qrow in quantiles_table.iterrows():
        new_row = qrow.copy()
        for h in range(1, 25):
            col = f"HE{h}"
            if pd.notna(new_row[col]):
                new_row[col] = new_row[col] + hour_deltas[h]
        onpk = [new_row[f"HE{h}"] for h in ONPEAK_HOURS if pd.notna(new_row.get(f"HE{h}"))]
        offpk = [new_row[f"HE{h}"] for h in OFFPEAK_HOURS if pd.notna(new_row.get(f"HE{h}"))]
        allh = [new_row[f"HE{h}"] for h in range(1, 25) if pd.notna(new_row.get(f"HE{h}"))]
        new_row["OnPeak"] = np.mean(onpk) if onpk else np.nan
        new_row["OffPeak"] = np.mean(offpk) if offpk else np.nan
        new_row["Flat"] = np.mean(allh) if allh else np.nan
        adj_q_rows.append(new_row)
    adj_quantiles = pd.DataFrame(adj_q_rows)

    # Adjusted df_forecast
    adj_df_forecast = df_forecast.copy()
    for idx, row in adj_df_forecast.iterrows():
        h = int(row["hour_ending"])
        delta = hour_deltas.get(h, 0)
        adj_df_forecast.at[idx, "point_forecast"] = row["point_forecast"] + delta
        for q in configs.QUANTILES:
            col = f"q_{q:.2f}"
            if col in adj_df_forecast.columns and pd.notna(row[col]):
                adj_df_forecast.at[idx, col] = row[col] + delta

    return {
        **base,
        "output_table": adj_output,
        "quantiles_table": adj_quantiles,
        "df_forecast": adj_df_forecast,
        "base_output_table": output_table,
        "base_quantiles_table": quantiles_table,
        "base_df_forecast": df_forecast,
        "deltas": deltas,
        "adjustment": {
            "total_onpeak": total_adj_onpeak,
            "total_offpeak": total_adj_offpeak,
            "base_onpeak": float(fcst_row["OnPeak"]),
            "base_offpeak": float(fcst_row["OffPeak"]),
            "adj_onpeak": float(adj_row["OnPeak"]),
            "adj_offpeak": float(adj_row["OffPeak"]),
            "n_factors": len(deltas),
            "sensitivities": sens,
        },
    }


def main():
    """Entry point."""
    import src.like_day_forecast.settings
    result = run()
    if "error" not in result:
        adj = result["adjustment"]
        print(f"\nOn-Peak: ${adj['base_onpeak']:.2f} → ${adj['adj_onpeak']:.2f} "
              f"({adj['total_onpeak']:+.2f})")
        print(f"Off-Peak: ${adj['base_offpeak']:.2f} → ${adj['adj_offpeak']:.2f} "
              f"({adj['total_offpeak']:+.2f})")
        for d in result["deltas"]:
            print(f"  {d.label}: {d.delta:+,.0f} {d.unit} → "
                  f"on-peak {d.adj_onpeak:+.2f}, off-peak {d.adj_offpeak:+.2f}")


if __name__ == "__main__":
    main()
