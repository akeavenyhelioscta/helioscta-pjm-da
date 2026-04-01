"""Refresh the local data cache — pull all sources fresh and write to parquet.

Usage:
    python -m src.scripts.refresh_cache              # refresh all sources
    python -m src.scripts.refresh_cache --ttl 1      # set TTL to 1 hour
    python -m src.scripts.refresh_cache --only lmps_hourly_da gas_prices
    python -m src.scripts.refresh_cache --workers 20 # parallel workers (default: 16)
    python -m src.scripts.refresh_cache --purge              # delete files >24h old
    python -m src.scripts.refresh_cache --purge --purge-age 8  # delete files >8h old
    python -m src.scripts.refresh_cache --purge --dry-run    # preview without deleting
"""
import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_BACKEND = str(Path(__file__).resolve().parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from src.like_day_forecast import configs
from src.utils.cache_utils import pull_with_cache, purge_stale
from src.data import (
    dates,
    fuel_mix_hourly,
    gas_prices,
    ice_power_intraday,
    lmps_hourly,
    load_rt_metered_hourly,
    meteologica_da_price_forecast,
    meteologica_generation_forecast_hourly,
    outages_actual_daily,
    outages_forecast_daily,
    pjm_load_forecast_hourly,
    pjm_solar_forecast_hourly,
    pjm_wind_forecast_hourly,
    tie_flows_hourly,
    transmission_outages,
    weather_hourly,
)
from src.data import load_forecast_vintages, solar_forecast_vintages, wind_forecast_vintages
from src.data.load_forecast_vintages import pull_combined_vintages as _pull_load_vintages

logger = logging.getLogger(__name__)

# ── Source registry ──────────────────────────────────────────────────
# Each entry: (source_name, pull_fn, pull_kwargs, unique_key)
# source_name is the cache key — one parquet file per source_name.

# Each entry: (source_name, pull_fn, pull_kwargs, unique_key, refresh_lookback_days)
# refresh_lookback_days: on refresh of an existing cache, only pull
# the last N days from the DB instead of full history. None = always full pull
# (use None for sources whose pull functions don't support sql_overrides start_date).

SOURCE_REGISTRY: list[tuple[str, callable, dict, list[str], int | None]] = [
    ("ice_gas_prices",                          gas_prices.pull,                                     {},                                             ["date"],                                                                       7),
    ("ice_power_intraday",                      ice_power_intraday.pull_intraday,                    {"lookback_days": 3},                           ["trade_date", "symbol", "snapshot_at"],                                         None),
    ("ice_power_settles",                       ice_power_intraday.pull_settles,                     {"lookback_days": 30},                          ["trade_date", "symbol"],                                                       None),
    ("meteologica_da_price_forecast",           meteologica_da_price_forecast.pull,                  {},                                             ["forecast_date", "hour_ending", "hub"],                                        None),
    ("meteologica_da_price_vintages",           meteologica_da_price_forecast.pull_da_cutoff_vintages, {},                                           ["forecast_date", "hour_ending", "hub", "vintage_label"],                       None),
    ("meteologica_load_forecast_vintages",      _pull_load_vintages,                                {"source": "meteologica"},                      ["forecast_date", "hour_ending", "source", "region", "vintage_label"],           None),
    ("meteologica_solar_forecast_vintages",     solar_forecast_vintages.pull_meteologica_vintages,   {},                                             ["source", "region", "forecast_date", "hour_ending", "vintage_label"],           None),
    ("meteologica_wind_forecast_vintages",      wind_forecast_vintages.pull_meteologica_vintages,    {},                                             ["source", "region", "forecast_date", "hour_ending", "vintage_label"],           None),
    ("pjm_dates_daily",                         dates.pull_daily,                                   {"schema": configs.SCHEMA},                     ["date"],                                                                       7),
    ("pjm_fuel_mix_hourly",                     fuel_mix_hourly.pull,                                {},                                             ["date", "hour_ending"],                                                        7),
    ("pjm_lmps_hourly_da",                      lmps_hourly.pull,                                   {"schema": configs.SCHEMA, "market": "da"},     ["date", "hour_ending", "hub"],                                                 7),
    ("pjm_lmps_hourly_rt",                      lmps_hourly.pull,                                   {"schema": configs.SCHEMA, "market": "rt"},     ["date", "hour_ending", "hub"],                                                 7),
    ("pjm_load_forecast_latest",                pjm_load_forecast_hourly.pull,                      {"region": configs.LOAD_REGION},                ["forecast_date", "hour_ending"],                                               None),
    ("pjm_load_forecast_vintages",              _pull_load_vintages,                                {"source": "pjm"},                              ["forecast_date", "hour_ending", "source", "region", "vintage_label"],           None),
    ("pjm_load_rt_metered_hourly",              load_rt_metered_hourly.pull,                         {},                                             ["date", "hour_ending", "region"],                                              7),
    ("pjm_outages_actual_daily",                outages_actual_daily.pull,                           {"schema": configs.SCHEMA},                     ["date", "region"],                                                             7),
    ("pjm_outages_forecast_daily",              outages_forecast_daily.pull,                         {"lookback_days": 14},                          ["forecast_execution_date", "forecast_date", "region", "forecast_rank"],         None),
    ("pjm_solar_forecast_rto",                  pjm_solar_forecast_hourly.pull,                     {"timezone": "America/New_York"},                ["forecast_date", "hour_ending"],                                               None),
    ("pjm_solar_forecast_vintages",             solar_forecast_vintages.pull_pjm_vintages,          {},                                             ["source", "region", "forecast_date", "hour_ending", "vintage_label"],           None),
    ("pjm_tie_flows_hourly",                    tie_flows_hourly.pull,                               {},                                             ["datetime_beginning_ept"],                                                     7),
    ("pjm_transmission_outages",                transmission_outages.pull,                           {},                                             ["outage_id"],                                                                  None),
    ("pjm_wind_forecast_rto",                   pjm_wind_forecast_hourly.pull,                      {"timezone": "America/New_York"},                ["forecast_date", "hour_ending"],                                               None),
    ("pjm_wind_forecast_vintages",              wind_forecast_vintages.pull_pjm_vintages,           {},                                             ["source", "region", "forecast_date", "hour_ending", "vintage_label"],           None),
    ("wsi_weather_hourly",                      weather_hourly.pull,                                 {},                                             ["date", "hour_ending", "station_name"],                                        7),
]


def _pull_one(
    source_name: str,
    pull_fn: callable,
    pull_kwargs: dict,
    unique_key: list[str],
    refresh_lookback_days: int | None,
    cache_dir: Path,
    ttl_hours: float,
) -> tuple[str, int]:
    """Pull a single source into cache. Returns (source_name, row_count)."""
    df = pull_with_cache(
        source_name=source_name,
        pull_fn=pull_fn,
        pull_kwargs=pull_kwargs,
        cache_dir=cache_dir,
        cache_enabled=True,
        ttl_hours=ttl_hours,
        force_refresh=True,
        unique_key=unique_key,
        refresh_lookback_days=refresh_lookback_days,
    )
    return source_name, len(df)


def refresh(
    cache_dir: Path = configs.CACHE_DIR,
    ttl_hours: float = configs.CACHE_TTL_HOURS,
    only: list[str] | None = None,
    max_workers: int = 16,
) -> dict[str, int]:
    """Pull all (or selected) data sources and write to cache.

    Returns dict mapping source_name to row count.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, int] = {}

    sources = SOURCE_REGISTRY
    if only:
        sources = [(n, fn, kw, uk, lb) for n, fn, kw, uk, lb in sources if n in only]
        unknown = set(only) - {n for n, _, _, _, _ in sources}
        if unknown:
            logger.warning(f"Unknown sources (skipped): {unknown}")
            logger.info(f"Available: {[n for n, _, _, _, _ in SOURCE_REGISTRY]}")

    logger.info(f"Refreshing {len(sources)} data sources → {cache_dir} (workers={max_workers})")
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_pull_one, name, fn, kw, uk, lb, cache_dir, ttl_hours): name
            for name, fn, kw, uk, lb in sources
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                _, rows = future.result()
                results[name] = rows
                logger.info(f"  {name}: {rows:,} rows")
            except Exception as e:
                logger.error(f"  {name}: FAILED — {e}")
                results[name] = -1

    elapsed = time.time() - t0
    ok = sum(1 for v in results.values() if v >= 0)
    failed = sum(1 for v in results.values() if v < 0)
    logger.info(f"Done in {elapsed:.1f}s — {ok} succeeded, {failed} failed")

    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh the local data cache")
    parser.add_argument(
        "--ttl", type=float, default=configs.CACHE_TTL_HOURS,
        help=f"Cache TTL in hours (default: {configs.CACHE_TTL_HOURS})",
    )
    parser.add_argument(
        "--only", nargs="+", default=None,
        help="Only refresh these source names (space-separated)",
    )
    parser.add_argument(
        "--workers", type=int, default=16,
        help="Max parallel workers (default: 16)",
    )
    parser.add_argument(
        "--purge", action="store_true",
        help="Delete stale cache files (older than --purge-age hours) and exit",
    )
    parser.add_argument(
        "--purge-age", type=float, default=24.0,
        help="Max age in hours for --purge (default: 24)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="With --purge, show what would be deleted without removing anything",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_sources",
        help="List all available source names and exit",
    )
    return parser.parse_args()


def main():
    import src.like_day_forecast.settings  # noqa: F401 — loads .env

    args = _parse_args()

    if args.list_sources:
        print(f"\n  {'SOURCE':<45s} {'REFRESH':>12s}  {'UNIQUE KEY':<55s}  PULL KWARGS")
        print(f"  {'-' * 45} {'-' * 12}  {'-' * 55}  {'-' * 30}")
        for name, _, kwargs, uk, lb in SOURCE_REGISTRY:
            lb_str = f"lookback={lb}d" if lb else "full"
            uk_str = ", ".join(uk) if uk else "-"
            kw_str = str(kwargs) if kwargs else "-"
            print(f"  {name:<45s} {lb_str:>12s}  {uk_str:<55s}  {kw_str}")
        print(f"\n  {len(SOURCE_REGISTRY)} sources registered\n")
        return

    if args.purge:
        purge_stale(
            cache_dir=configs.CACHE_DIR,
            max_age_hours=args.purge_age,
            dry_run=args.dry_run,
        )
        return

    refresh(ttl_hours=args.ttl, only=args.only, max_workers=args.workers)


if __name__ == "__main__":
    main()
