"""Multi-day strip forecast — predict DA LMP for D+1 through D+N.

Uses a single reference date (today) for analog matching, then collects each
analog's day+1, day+2, ... day+N actual DA LMP profiles to build independent
forecasts per horizon date.

Output: one forecast row per date, stacked into a strip table.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
from tabulate import tabulate

from src.like_day_forecast import configs
from src.like_day_forecast.features.builder import build_daily_features
from src.like_day_forecast.similarity.engine import find_analogs
from src.data import lmps_hourly
from src.like_day_forecast.pipelines.forecast import (
    weighted_quantile,
    _add_summary_cols,
    ONPEAK_HOURS,
    OFFPEAK_HOURS,
)

logger = logging.getLogger(__name__)

DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def run_strip(
    horizon: int = 3,
    reference_date: str | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    weight_method: str = "inverse_distance",
    config: configs.ScenarioConfig | None = None,
) -> dict:
    """Run a multi-day strip forecast from a single set of analogs.

    Approach:
    1. Build daily feature matrix once
    2. Find analogs for the reference date (defaults to today)
    3. For each horizon day offset (1..N), collect each analog's day+offset
       actual DA LMP profile and build a weighted probabilistic forecast
    4. Stack all days into a combined strip table

    Args:
        horizon: Number of days ahead to forecast (e.g. 3 → D+1, D+2, D+3).
        reference_date: Anchor date for analog matching (YYYY-MM-DD).
                        Defaults to today.
        n_analogs: Number of analog days to find.
        weight_method: Weighting method for analogs.
        config: Optional ScenarioConfig (overrides n_analogs / weight_method).

    Returns:
        Dict with strip_table, quantiles_table, analogs, per_day results.
    """
    if config is None:
        config = configs.ScenarioConfig(
            n_analogs=n_analogs,
            weight_method=weight_method,
        )

    if reference_date is not None:
        ref_date = pd.to_datetime(reference_date).date()
    else:
        ref_date = date.today()

    forecast_dates = [ref_date + timedelta(days=d) for d in range(1, horizon + 1)]

    logger.info("=" * 70)
    logger.info(f"Strip Forecast: {len(forecast_dates)} days — {config.hub}")
    logger.info(f"Reference: {ref_date} ({DAY_ABBR[ref_date.weekday()]})")
    for fd in forecast_dates:
        logger.info(f"  D+{(fd - ref_date).days}: {fd} ({DAY_ABBR[fd.weekday()]})")
    logger.info("=" * 70)

    # 1. Build daily feature matrix (once)
    logger.info("Building daily feature matrix...")
    df_features = build_daily_features(schema=config.schema, hub=config.hub)

    available_dates = sorted(df_features["date"].unique())
    logger.info(f"Feature matrix: {len(available_dates):,} days "
                f"({available_dates[0]} to {available_dates[-1]})")

    if ref_date not in available_dates:
        logger.error(f"Reference date {ref_date} not in feature matrix. "
                     f"Latest available: {available_dates[-1]}")
        return {"error": f"Reference date {ref_date} not available"}

    # 2. Find analogs (once, against the reference date)
    logger.info(f"Finding {config.n_analogs} analogs for {ref_date}...")
    analogs_df = find_analogs(
        target_date=ref_date,
        df_features=df_features,
        n_analogs=config.n_analogs,
        feature_weights=config.resolved_weights(),
        apply_calendar_filter=config.apply_calendar_filter,
        apply_regime_filter=config.apply_regime_filter,
        season_window_days=config.season_window_days,
        same_dow_group=config.same_dow_group,
        weight_method=config.weight_method,
        adaptive_filter_enabled=config.adaptive_filter_enabled,
        adaptive_extreme_threshold_std=config.adaptive_extreme_threshold_std,
        adaptive_season_window_days=config.adaptive_season_window_days,
        adaptive_same_dow_group=config.adaptive_same_dow_group,
        adaptive_lmp_tolerance_std=config.adaptive_lmp_tolerance_std,
        adaptive_gas_tolerance_std=config.adaptive_gas_tolerance_std,
        adaptive_n_analogs=config.adaptive_n_analogs,
        adaptive_weight_method=config.adaptive_weight_method,
        adaptive_softmax_temperature=config.adaptive_softmax_temperature,
    )

    # 3. Pull ALL hourly DA LMP data once
    logger.info("Pulling hourly DA LMP data...")
    df_lmp_all = lmps_hourly.pull(schema=config.schema, hub=config.hub, market="da")

    # 4. Build forecast for each horizon day
    strip_rows = []
    all_quantile_rows = []
    per_day = {}

    for offset, target_date in enumerate(forecast_dates, start=1):
        logger.info(f"--- D+{offset}: {target_date} ({DAY_ABBR[target_date.weekday()]}) ---")

        # For each analog, get the day+offset LMP profile
        analog_offset_dates = [d + timedelta(days=offset) for d in analogs_df["date"]]
        df_offset_lmps = df_lmp_all[df_lmp_all["date"].isin(analog_offset_dates)].copy()

        # Map back to analog date for weight lookup
        df_offset_lmps["analog_date"] = df_offset_lmps["date"] - timedelta(days=offset)
        df_offset_lmps = df_offset_lmps.merge(
            analogs_df[["date", "weight", "rank", "distance"]],
            left_on="analog_date", right_on="date", suffixes=("", "_analog"),
        )

        n_with_data = df_offset_lmps["analog_date"].nunique()
        logger.info(f"  {n_with_data}/{len(analogs_df)} analogs have day+{offset} LMP data")

        if n_with_data == 0:
            logger.warning(f"  No day+{offset} LMP data — skipping {target_date}")
            continue

        # Build probabilistic forecast for this day
        forecast_rows = []
        for h in configs.HOURS:
            hour_data = df_offset_lmps[df_offset_lmps["hour_ending"] == h]
            if len(hour_data) == 0:
                continue

            values = hour_data["lmp_total"].values
            weights = hour_data["weight"].values
            weights = weights / weights.sum()

            point = np.average(values, weights=weights)
            row = {"hour_ending": h, "point_forecast": point}

            for q in config.quantiles:
                row[f"q_{q:.2f}"] = weighted_quantile(values, weights, q)

            forecast_rows.append(row)

        df_forecast = pd.DataFrame(forecast_rows)
        forecast_hourly = dict(
            zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"])
        )

        # Check for actuals
        df_actuals = df_lmp_all[df_lmp_all["date"] == target_date].sort_values("hour_ending")
        has_actuals = len(df_actuals) >= 24
        actuals_hourly = None
        if has_actuals:
            actuals_hourly = dict(
                zip(df_actuals["hour_ending"].astype(int), df_actuals["lmp_total"])
            )

        # Forecast row for strip table
        fc_row = {"Date": target_date, "Type": "Forecast"}
        for h in range(1, 25):
            fc_row[f"HE{h}"] = forecast_hourly.get(h)
        fc_row = _add_summary_cols(fc_row)
        strip_rows.append(fc_row)

        # Actual row (if available)
        if has_actuals:
            act_row = {"Date": target_date, "Type": "Actual"}
            for h in range(1, 25):
                act_row[f"HE{h}"] = actuals_hourly.get(h)
            act_row = _add_summary_cols(act_row)
            strip_rows.append(act_row)

        # Quantile rows
        for q_val in config.quantiles:
            col = f"q_{q_val:.2f}"
            if col in df_forecast.columns:
                q_row = {"Date": target_date, "Type": f"P{int(q_val * 100):02d}"}
                q_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast[col]))
                for h in range(1, 25):
                    q_row[f"HE{h}"] = q_hourly.get(h)
                q_row = _add_summary_cols(q_row)
                all_quantile_rows.append(q_row)

        per_day[str(target_date)] = {
            "df_forecast": df_forecast,
            "has_actuals": has_actuals,
            "n_analogs_used": n_with_data,
            "offset": offset,
        }

    # 5. Assemble output tables
    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    strip_table = pd.DataFrame(strip_rows, columns=cols)
    quantiles_table = pd.DataFrame(all_quantile_rows, columns=cols)

    # 6. Print results
    _print_strip_analogs(analogs_df, ref_date, forecast_dates, config.hub)
    _print_strip_table(strip_table, config.hub)
    _print_strip_quantiles(quantiles_table)

    return {
        "strip_table": strip_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs_df,
        "reference_date": str(ref_date),
        "forecast_dates": [str(d) for d in forecast_dates],
        "per_day": per_day,
    }


# ── Display helpers ──────────────────────────────────────────────────────


def _print_strip_analogs(
    analogs_df: pd.DataFrame,
    ref_date: date,
    forecast_dates: list[date],
    hub: str,
) -> None:
    """Print analog days used for the strip."""
    dates_str = ", ".join(
        f"{d} ({DAY_ABBR[d.weekday()]})" for d in forecast_dates
    )
    print("\n" + "=" * 100)
    print("  LIKE-DAY STRIP FORECAST — ANALOG DAYS")
    print(f"  Reference: {ref_date} ({DAY_ABBR[ref_date.weekday()]})  |  Hub: {hub}")
    print(f"  Forecast dates: {dates_str}")
    print("=" * 100)

    display = analogs_df.head(configs.DEFAULT_N_DISPLAY).copy()
    display["date"] = display["date"].astype(str)
    display["distance"] = display["distance"].map("{:.4f}".format)
    display["similarity"] = display["similarity"].map("{:.2%}".format)
    display["weight"] = display["weight"].map("{:.4f}".format)

    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))
    print(f"\n  Total analogs: {len(analogs_df)} | "
          f"Top-5 weight sum: {analogs_df.head(5)['weight'].sum():.2%} | "
          f"Distance range: {analogs_df['distance'].min():.4f} — "
          f"{analogs_df['distance'].max():.4f}")


def _print_strip_table(table: pd.DataFrame, hub: str) -> None:
    """Print the combined strip forecast table."""
    print("\n" + "=" * 130)
    print(f"  DA LMP LIKE-DAY STRIP FORECAST — {hub} ($/MWh)")
    print("=" * 130)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    prev_date = None
    for _, row in table.iterrows():
        # Separator between dates
        if prev_date is not None and row["Date"] != prev_date:
            print("-" * len(header))
        prev_date = row["Date"]

        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        print(line)

    print("=" * 130 + "\n")


def _print_strip_quantiles(table: pd.DataFrame) -> None:
    """Print quantile bands for all strip days."""
    if table.empty:
        return

    print("  Quantile Bands ($/MWh)")
    print("-" * 100)

    header = f"{'Date':<12} {'Band':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    prev_date = None
    for _, row in table.iterrows():
        if prev_date is not None and row["Date"] != prev_date:
            print("-" * len(header))
        prev_date = row["Date"]

        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        print(line)

    print("-" * len(header) + "\n")


def main():
    """Entry point — initialize settings and run 3-day strip forecast."""
    import src.like_day_forecast.settings
    run_strip(horizon=3)


if __name__ == "__main__":
    main()
