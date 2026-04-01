"""Market-adjusted like-day forecast — rescale analog shape to observed market level.

The like-day model produces an hourly profile (shape) from analog days. This
pipeline takes that shape and rescales it so the on-peak average matches a
market-observed anchor (e.g., ICE next-day DA settle). Quantile bands are
shifted by the same delta.

Usage:
    from src.like_day_forecast.pipelines.market_adjusted_forecast import run as run_adjusted
    result = run_adjusted(market_onpeak=60.80)

    # Or with explicit off-peak anchor too:
    result = run_adjusted(market_onpeak=60.80, market_offpeak=35.00)
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_base_forecast

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def run(
    market_onpeak: float,
    market_offpeak: float | None = None,
    forecast_date: str | None = None,
    config: configs.ScenarioConfig | None = None,
    **kwargs,
) -> dict:
    """Run the like-day forecast and rescale to market-observed levels.

    The model's hourly shape is preserved — each hour's ratio to the period
    average is maintained. Only the level shifts.

    Args:
        market_onpeak: Observed on-peak price ($/MWh) — e.g., ICE NxtDay DA settle.
        market_offpeak: Observed off-peak price ($/MWh). If None, off-peak is
            shifted by the same $/MWh delta as on-peak.
        forecast_date: YYYY-MM-DD, defaults to tomorrow.
        config: Optional ScenarioConfig override.
        **kwargs: Passed through to the base forecast pipeline.

    Returns:
        Dict matching base forecast output, plus adjusted tables and metadata.
    """
    # 1. Run base like-day forecast
    base = run_base_forecast(forecast_date=forecast_date, config=config, **kwargs)

    if "error" in base:
        return base

    output_table = base["output_table"]
    quantiles_table = base["quantiles_table"]
    df_forecast = base["df_forecast"]

    # 2. Extract base period averages from the Forecast row
    fcst_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    base_onpeak = fcst_row["OnPeak"]
    base_offpeak = fcst_row["OffPeak"]

    # 3. Compute deltas
    onpeak_delta = market_onpeak - base_onpeak
    if market_offpeak is not None:
        offpeak_delta = market_offpeak - base_offpeak
    else:
        offpeak_delta = onpeak_delta

    logger.info(f"Market adjustment: OnPeak {base_onpeak:.2f} → {market_onpeak:.2f} "
                f"(+{onpeak_delta:+.2f}), OffPeak {base_offpeak:.2f} → "
                f"{base_offpeak + offpeak_delta:.2f} (+{offpeak_delta:+.2f})")

    # 4. Build hour-level delta map
    hour_deltas = {}
    for h in range(1, 25):
        if h in ONPEAK_HOURS:
            hour_deltas[h] = onpeak_delta
        else:
            hour_deltas[h] = offpeak_delta

    # 5. Adjust point forecast
    adjusted_hourly = {}
    for h in range(1, 25):
        base_val = fcst_row.get(f"HE{h}")
        if pd.notna(base_val):
            adjusted_hourly[h] = base_val + hour_deltas[h]

    # Build adjusted output table
    target_date = base["forecast_date"]
    adj_row = {"Date": target_date, "Type": "Adjusted"}
    for h in range(1, 25):
        adj_row[f"HE{h}"] = adjusted_hourly.get(h)

    onpeak_vals = [adjusted_hourly[h] for h in ONPEAK_HOURS if h in adjusted_hourly]
    offpeak_vals = [adjusted_hourly[h] for h in OFFPEAK_HOURS if h in adjusted_hourly]
    all_vals = [adjusted_hourly[h] for h in range(1, 25) if h in adjusted_hourly]
    adj_row["OnPeak"] = np.mean(onpeak_vals) if onpeak_vals else np.nan
    adj_row["OffPeak"] = np.mean(offpeak_vals) if offpeak_vals else np.nan
    adj_row["Flat"] = np.mean(all_vals) if all_vals else np.nan

    # Append adjusted row to output table
    adj_output = pd.concat([output_table, pd.DataFrame([adj_row])], ignore_index=True)

    # 6. Adjust quantile bands — shift by same delta per hour
    adj_q_rows = []
    for _, qrow in quantiles_table.iterrows():
        new_row = qrow.copy()
        for h in range(1, 25):
            col = f"HE{h}"
            if pd.notna(new_row[col]):
                delta = hour_deltas[h]
                new_row[col] = new_row[col] + delta

        # Recompute summary columns
        onpk = [new_row[f"HE{h}"] for h in ONPEAK_HOURS if pd.notna(new_row.get(f"HE{h}"))]
        offpk = [new_row[f"HE{h}"] for h in OFFPEAK_HOURS if pd.notna(new_row.get(f"HE{h}"))]
        allh = [new_row[f"HE{h}"] for h in range(1, 25) if pd.notna(new_row.get(f"HE{h}"))]
        new_row["OnPeak"] = np.mean(onpk) if onpk else np.nan
        new_row["OffPeak"] = np.mean(offpk) if offpk else np.nan
        new_row["Flat"] = np.mean(allh) if allh else np.nan
        adj_q_rows.append(new_row)

    adj_quantiles = pd.DataFrame(adj_q_rows)

    # 7. Adjust df_forecast (hour-level detail used by views)
    adj_df_forecast = df_forecast.copy()
    for idx, row in adj_df_forecast.iterrows():
        h = int(row["hour_ending"])
        delta = hour_deltas.get(h, 0)
        adj_df_forecast.at[idx, "point_forecast"] = row["point_forecast"] + delta
        for q in configs.QUANTILES:
            col = f"q_{q:.2f}"
            if col in adj_df_forecast.columns and pd.notna(row[col]):
                adj_df_forecast.at[idx, col] = row[col] + delta

    # 8. Print summary
    _print_adjustment(base_onpeak, base_offpeak, market_onpeak,
                      market_offpeak or (base_offpeak + offpeak_delta),
                      onpeak_delta, offpeak_delta, adjusted_hourly)

    return {
        **base,
        "output_table": adj_output,
        "quantiles_table": adj_quantiles,
        "df_forecast": adj_df_forecast,
        "adjustment": {
            "market_onpeak": market_onpeak,
            "market_offpeak": market_offpeak or (base_offpeak + offpeak_delta),
            "base_onpeak": base_onpeak,
            "base_offpeak": base_offpeak,
            "onpeak_delta": onpeak_delta,
            "offpeak_delta": offpeak_delta,
        },
        "base_output_table": output_table,
        "base_quantiles_table": quantiles_table,
        "base_df_forecast": df_forecast,
    }


def _print_adjustment(
    base_onpk: float, base_offpk: float,
    mkt_onpk: float, mkt_offpk: float,
    delta_on: float, delta_off: float,
    adjusted_hourly: dict,
) -> None:
    """Print a concise adjustment summary."""
    print("\n" + "=" * 70)
    print("  MARKET-ADJUSTED FORECAST")
    print("=" * 70)
    print(f"  {'':20s} {'Model':>10s} {'Market':>10s} {'Delta':>10s}")
    print(f"  {'On-Peak ($/MWh)':20s} {base_onpk:>10.2f} {mkt_onpk:>10.2f} {delta_on:>+10.2f}")
    print(f"  {'Off-Peak ($/MWh)':20s} {base_offpk:>10.2f} {mkt_offpk:>10.2f} {delta_off:>+10.2f}")

    flat_vals = [adjusted_hourly[h] for h in range(1, 25) if h in adjusted_hourly]
    adj_flat = np.mean(flat_vals) if flat_vals else 0
    print(f"  {'Adjusted Flat':20s} {adj_flat:>10.2f}")

    # Peak hour
    if adjusted_hourly:
        peak_he = max(adjusted_hourly, key=adjusted_hourly.get)
        print(f"  {'Peak Hour':20s} {'HE' + str(peak_he):>10s} {adjusted_hourly[peak_he]:>10.2f}")

    print("=" * 70 + "\n")


def main():
    """Entry point — example with ICE anchor."""
    import src.like_day_forecast.settings

    result = run(market_onpeak=60.80)

    if "error" not in result:
        adj = result["adjustment"]
        print(f"On-Peak: ${adj['base_onpeak']:.2f} → ${adj['market_onpeak']:.2f}")
        print(f"Off-Peak: ${adj['base_offpeak']:.2f} → ${adj['market_offpeak']:.2f}")


if __name__ == "__main__":
    main()
