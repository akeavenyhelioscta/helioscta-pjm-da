"""Multi-day strip forecast — predict DA LMP for D+1 through D+N.

Uses per-day analog selection with synthetic reference rows: each target day
gets its own set of analogs matched to a reference row that reflects the
correct DOW, target weather, renewables, and outage forecasts.  Analog D+1
LMP profiles are always used (offset=1), eliminating the stale-offset drift
of the original approach.

Output: one forecast row per date, stacked into a strip table.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
from tabulate import tabulate

from src.like_day_forecast import configs
from src.like_day_forecast.features.builder import (
    build_daily_features,
    build_synthetic_reference_row,
)
from src.like_day_forecast.similarity.engine import find_analogs
from src.data import (
    lmps_hourly,
    weather_hourly,
    pjm_solar_forecast_hourly,
    pjm_wind_forecast_hourly,
    solar_forecast_vintages,
    wind_forecast_vintages,
    outages_forecast_daily,
    pjm_load_forecast_hourly,
)
from src.like_day_forecast.pipelines.forecast import (
    weighted_quantile,
    _add_summary_cols,
    ONPEAK_HOURS,
    OFFPEAK_HOURS,
)
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def run_strip(
    horizon: int = 3,
    reference_date: str | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    weight_method: str = "inverse_distance",
    config: configs.ScenarioConfig | None = None,
    cache_dir=configs.CACHE_DIR,
    cache_enabled=configs.CACHE_ENABLED,
    cache_ttl_hours=configs.CACHE_TTL_HOURS,
    force_refresh=configs.FORCE_CACHE_REFRESH,
) -> dict:
    """Run a multi-day strip forecast with per-day analog selection.

    For each target day:
      1. Build a synthetic reference row (today's features + correct calendar
         + correct target-date forecasts)
      2. Find independent analogs against the synthetic reference
      3. Collect each analog's *next-day* (D+1) actual DA LMP profile
      4. Build a weighted probabilistic forecast

    Args:
        horizon: Number of days ahead to forecast (e.g. 4 → D+1 … D+4).
        reference_date: Anchor date for the base feature row (YYYY-MM-DD).
                        Defaults to today.
        n_analogs: Number of analog days to find per target day.
        weight_method: Weighting method for analogs.
        config: Optional ScenarioConfig (overrides n_analogs / weight_method).

    Returns:
        Dict with strip_table, quantiles_table, per-day analogs and results.
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
    logger.info(f"Strip Forecast (rolling-reference): {len(forecast_dates)} days — {config.hub}")
    logger.info(f"Base reference: {ref_date} ({DAY_ABBR[ref_date.weekday()]})")
    for fd in forecast_dates:
        logger.info(f"  D+{(fd - ref_date).days}: {fd} ({DAY_ABBR[fd.weekday()]})")
    logger.info("=" * 70)

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── 1. Build daily feature matrix (once) ────────────────────────
    logger.info("Building daily feature matrix...")
    df_features = build_daily_features(
        schema=config.schema,
        hub=config.hub,
        renewable_mode=config.resolved_renewable_mode(),
        renewable_region=config.renewable_forecast_region,
        renewable_blend_pjm_weight=config.renewable_blend_weight(offset=1),
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    available_dates = sorted(df_features["date"].unique())
    logger.info(f"Feature matrix: {len(available_dates):,} days "
                f"({available_dates[0]} to {available_dates[-1]})")

    if ref_date not in available_dates:
        logger.error(f"Reference date {ref_date} not in feature matrix. "
                     f"Latest available: {available_dates[-1]}")
        return {"error": f"Reference date {ref_date} not available"}

    # ── 2. Pull ALL hourly DA LMP data (once) ───────────────────────
    logger.info("Pulling hourly DA LMP data...")
    df_lmp_all = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": config.schema, "market": "da"},
        **cache_kwargs,
    )
    df_lmp_all = df_lmp_all[df_lmp_all["hub"] == config.hub].copy()
    df_lmp_all["date"] = pd.to_datetime(df_lmp_all["date"]).dt.date

    # ── 3. Pull multi-day forecast data for synthetic rows ──────────
    logger.info("Pulling multi-day forecast data for synthetic references...")
    renewable_mode = config.resolved_renewable_mode()
    logger.info(
        "Renewable forecast mode=%s (region=%s, blend D+1=%.2f D+4=%.2f)",
        renewable_mode,
        config.renewable_forecast_region,
        config.renewable_blend_weight(offset=1),
        config.renewable_blend_weight(offset=4),
    )

    df_weather_all = None
    try:
        df_weather_all = pull_with_cache(
            source_name="wsi_weather_hourly",
            pull_fn=weather_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Weather forecast pull failed: {e}")

    df_solar_multi = None
    try:
        df_solar_multi = pull_with_cache(
            source_name="pjm_solar_forecast_rto",
            pull_fn=pjm_solar_forecast_hourly.pull,
            pull_kwargs={"timezone": "America/New_York"},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"PJM solar multi-day forecast pull failed: {e}")

    df_wind_multi = None
    try:
        df_wind_multi = pull_with_cache(
            source_name="pjm_wind_forecast_rto",
            pull_fn=pjm_wind_forecast_hourly.pull,
            pull_kwargs={"timezone": "America/New_York"},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"PJM wind multi-day forecast pull failed: {e}")

    df_meteo_solar_multi = None
    try:
        df_solar_vintages = pull_with_cache(
            source_name="meteologica_solar_forecast_vintages",
            pull_fn=solar_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kwargs,
        )
        df_meteo_solar_multi = df_solar_vintages[
            (df_solar_vintages["vintage_label"] == "Latest")
            & (df_solar_vintages["region"] == config.renewable_forecast_region)
        ].copy()
        df_meteo_solar_multi = df_meteo_solar_multi.rename(columns={"forecast_mw": "forecast_generation_mw"})
    except Exception as e:
        logger.warning(f"Meteologica solar multi-day forecast pull failed: {e}")

    df_meteo_wind_multi = None
    try:
        df_wind_vintages = pull_with_cache(
            source_name="meteologica_wind_forecast_vintages",
            pull_fn=wind_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kwargs,
        )
        df_meteo_wind_multi = df_wind_vintages[
            (df_wind_vintages["vintage_label"] == "Latest")
            & (df_wind_vintages["region"] == config.renewable_forecast_region)
        ].copy()
        df_meteo_wind_multi = df_meteo_wind_multi.rename(columns={"forecast_mw": "forecast_generation_mw"})
    except Exception as e:
        logger.warning(f"Meteologica wind multi-day forecast pull failed: {e}")

    df_outage_multi = None
    try:
        df_outage_multi = pull_with_cache(
            source_name="pjm_outages_forecast_daily",
            pull_fn=outages_forecast_daily.pull,
            pull_kwargs={"lookback_days": 14},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Outage forecast pull failed: {e}")

    df_load_forecast_multi = None
    try:
        df_load_forecast_multi = pull_with_cache(
            source_name="pjm_load_forecast_latest",
            pull_fn=pjm_load_forecast_hourly.pull,
            pull_kwargs={"region": configs.LOAD_REGION},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Load forecast pull failed: {e}")

    # ── 4. Per-day analog selection + forecast ──────────────────────
    analog_kwargs = dict(
        n_analogs=config.n_analogs,
        feature_weights=config.resolved_weights(),
        apply_calendar_filter=config.apply_calendar_filter,
        apply_regime_filter=config.apply_regime_filter,
        apply_outage_regime_filter=config.apply_outage_regime_filter,
        outage_tolerance_std=config.outage_tolerance_std,
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

    strip_rows = []
    all_quantile_rows = []
    per_day = {}
    per_day_analogs: dict[str, pd.DataFrame] = {}

    for offset, target_date in enumerate(forecast_dates, start=1):
        synthetic_ref_date = target_date - timedelta(days=1)
        renewable_blend_weight = config.renewable_blend_weight(offset=offset)
        logger.info(f"--- D+{offset}: {target_date} ({DAY_ABBR[target_date.weekday()]}) "
                     f"| ref={synthetic_ref_date} ---")

        # ── Find analogs for this day ───────────────────────────────
        if synthetic_ref_date == ref_date:
            # D+1: today's row already exists — use standard find_analogs
            analogs_df = find_analogs(
                target_date=ref_date,
                df_features=df_features,
                **analog_kwargs,
            )
        else:
            # D+2+: build synthetic row, inject, find analogs
            synthetic_row = build_synthetic_reference_row(
                df_features=df_features,
                today=ref_date,
                target_date=target_date,
                df_weather=df_weather_all,
                df_pjm_solar_forecast=df_solar_multi,
                df_pjm_wind_forecast=df_wind_multi,
                df_meteo_solar_forecast=df_meteo_solar_multi,
                df_meteo_wind_forecast=df_meteo_wind_multi,
                renewable_mode=renewable_mode,
                renewable_blend_weight_pjm=renewable_blend_weight,
                df_outage_forecast=df_outage_multi,
                df_load_forecast=df_load_forecast_multi,
            )

            # Remove today (ref_date) and any pre-existing row at synthetic_ref_date.
            # Today must be excluded because the synthetic row clones today's
            # reference features — without this, today would match against
            # itself on all high-weight groups and dominate the analog list.
            df_augmented = df_features[
                ~df_features["date"].isin([ref_date, synthetic_ref_date])
            ].copy()
            df_augmented = pd.concat(
                [df_augmented, synthetic_row.to_frame().T],
                ignore_index=True,
            )

            analogs_df = find_analogs(
                target_date=synthetic_ref_date,
                df_features=df_augmented,
                **analog_kwargs,
            )

        per_day_analogs[str(target_date)] = analogs_df

        # ── Collect analog D+1 LMP profiles (always offset=1) ──────
        analog_next_dates = [d + timedelta(days=1) for d in analogs_df["date"]]
        df_next_lmps = df_lmp_all[df_lmp_all["date"].isin(analog_next_dates)].copy()

        # Map next-day back to analog date for weight lookup
        df_next_lmps["analog_date"] = df_next_lmps["date"] - timedelta(days=1)
        df_next_lmps = df_next_lmps.merge(
            analogs_df[["date", "weight", "rank", "distance"]],
            left_on="analog_date", right_on="date", suffixes=("", "_analog"),
        )

        n_with_data = df_next_lmps["analog_date"].nunique()
        logger.info(f"  {n_with_data}/{len(analogs_df)} analogs have D+1 LMP data")

        if n_with_data == 0:
            logger.warning(f"  No D+1 LMP data — skipping {target_date}")
            continue

        # ── Build probabilistic forecast ────────────────────────────
        forecast_rows = []
        for h in configs.HOURS:
            hour_data = df_next_lmps[df_next_lmps["hour_ending"] == h]
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
            "analogs": analogs_df,
        }

    # ── 5. Assemble output tables ───────────────────────────────────
    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    strip_table = pd.DataFrame(strip_rows, columns=cols)
    quantiles_table = pd.DataFrame(all_quantile_rows, columns=cols)

    # ── 6. Print results ────────────────────────────────────────────
    _print_strip_analogs(per_day_analogs, ref_date, forecast_dates, config.hub)
    _print_strip_table(strip_table, config.hub)
    _print_strip_quantiles(quantiles_table)

    # Use D+1 analogs as top-level default for backward compatibility
    first_day_key = str(forecast_dates[0]) if forecast_dates else None
    top_level_analogs = per_day_analogs.get(first_day_key, pd.DataFrame())

    return {
        "strip_table": strip_table,
        "quantiles_table": quantiles_table,
        "analogs": top_level_analogs,
        "per_day_analogs": per_day_analogs,
        "reference_date": str(ref_date),
        "forecast_dates": [str(d) for d in forecast_dates],
        "per_day": per_day,
    }


# ── Display helpers ──────────────────────────────────────────────────────


def _print_strip_analogs(
    per_day_analogs: dict[str, pd.DataFrame],
    ref_date: date,
    forecast_dates: list[date],
    hub: str,
) -> None:
    """Print analog days used for each strip day."""
    dates_str = ", ".join(
        f"{d} ({DAY_ABBR[d.weekday()]})" for d in forecast_dates
    )
    print("\n" + "=" * 100)
    print("  LIKE-DAY STRIP FORECAST — ROLLING-REFERENCE ANALOGS")
    print(f"  Base reference: {ref_date} ({DAY_ABBR[ref_date.weekday()]})  |  Hub: {hub}")
    print(f"  Forecast dates: {dates_str}")
    print("=" * 100)

    for fd in forecast_dates:
        key = str(fd)
        analogs_df = per_day_analogs.get(key)
        if analogs_df is None or analogs_df.empty:
            continue

        offset = (fd - ref_date).days
        ref_day = fd - timedelta(days=1)
        print(f"\n  D+{offset}: {fd} ({DAY_ABBR[fd.weekday()]})  "
              f"| ref={ref_day} ({DAY_ABBR[ref_day.weekday()]})")
        print("-" * 90)

        display = analogs_df.head(configs.DEFAULT_N_DISPLAY).copy()
        display["date"] = display["date"].astype(str)
        display["distance"] = display["distance"].map("{:.4f}".format)
        display["similarity"] = display["similarity"].map("{:.2%}".format)
        display["weight"] = display["weight"].map("{:.4f}".format)
        print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))

        print(f"  Total: {len(analogs_df)} | "
              f"Top-5 wt: {analogs_df.head(5)['weight'].sum():.2%} | "
              f"Dist: {analogs_df['distance'].min():.4f}–{analogs_df['distance'].max():.4f}")


def _print_strip_table(table: pd.DataFrame, hub: str) -> None:
    """Print the combined strip forecast table."""
    print("\n" + "=" * 130)
    print(f"  DA LMP LIKE-DAY STRIP FORECAST (ROLLING-REF) — {hub} ($/MWh)")
    print("=" * 130)

    header = f"{'Date':<12} {'Type':<10}"
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
    """Entry point — initialize settings and run strip forecast through Friday."""
    import src.like_day_forecast.settings

    today = date.today()
    wd = today.weekday()
    if wd <= 3:
        horizon = 4 - wd
    elif wd == 4:
        horizon = 5
    else:
        horizon = 5

    run_strip(horizon=horizon)


if __name__ == "__main__":
    main()
