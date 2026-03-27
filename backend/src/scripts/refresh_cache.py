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
    lmps_hourly,
    load_rt_metered_hourly,
    meteologica_euro_ens_forecast,
    meteologica_load_forecast_hourly,
    outages_actual_daily,
    outages_forecast_daily,
    pjm_load_forecast_hourly,
    solar_forecast_hourly,
    weather_hourly,
    wind_forecast_hourly,
)
from src.reporting.fragments.load_forecast_vintage_combined import (
    _pull_source_vintages as _pull_combined_source_vintages,
)
from src.reporting.fragments.load_forecast_changes_rto import (
    _pull_source_vintages as _pull_changes_source_vintages,
)
from src.reporting.fragments.meteologica_vintage_table import (
    _pull_vintages as _pull_meteologica_vintages_table,
)

logger = logging.getLogger(__name__)

# ── Source registry ──────────────────────────────────────────────────
# Each entry: (source_name, pull_fn, pull_kwargs)
# source_name must match the key used by feature builder / report fragments
# so the same cached file is reused downstream.

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]

SOURCE_REGISTRY: list[tuple[str, callable, dict]] = [
    # Feature builder sources
    ("lmps_hourly_da", lmps_hourly.pull, {"schema": configs.SCHEMA, "hub": configs.HUB, "market": "da"}),
    ("lmps_hourly_rt", lmps_hourly.pull, {"schema": configs.SCHEMA, "hub": configs.HUB, "market": "rt"}),
    ("gas_prices", gas_prices.pull, {}),
    ("dates_daily", dates.pull_daily, {"schema": configs.SCHEMA}),
    ("load_rt_metered_hourly", load_rt_metered_hourly.pull, {"schema": configs.SCHEMA}),
    ("weather_hourly", weather_hourly.pull, {}),
    ("fuel_mix_hourly", fuel_mix_hourly.pull, {}),
    ("outages_actual_daily", outages_actual_daily.pull, {"schema": configs.SCHEMA}),
    ("solar_forecast_hourly", solar_forecast_hourly.pull, {}),
    ("wind_forecast_hourly", wind_forecast_hourly.pull, {}),

    # rt_load_metered_rto fragment
    ("load_rt_metered_hourly", load_rt_metered_hourly.pull, {"schema": configs.SCHEMA, "region": configs.LOAD_REGION}),

    # Outage fragments + API/view callsites
    ("outages_forecast_daily", outages_forecast_daily.pull, {"lookback_days": 14}),
    ("outages_actual_daily_history", outages_actual_daily.pull, {"sql_overrides": {"start_date": "2023-01-01"}}),

    # load_forecast_rto + regional forecast fragments
    ("pjm_load_strip", pjm_load_forecast_hourly.pull_strip, {}),
    ("meteologica_load_strip_v2_current_hour_filter", meteologica_load_forecast_hourly.pull_strip, {}),
    ("meteologica_euro_ens_strip_v2_current_hour_filter", meteologica_euro_ens_forecast.pull_strip, {}),
    *[(f"pjm_load_strip_{r.lower()}", pjm_load_forecast_hourly.pull_strip, {"region": r})
      for r in ["WEST", "MIDATL", "SOUTH"]],
    *[(f"meteologica_load_strip_v2_current_hour_filter_{r.lower()}", meteologica_load_forecast_hourly.pull_strip, {"region": r})
      for r in ["WEST", "MIDATL", "SOUTH"]],
    *[(f"meteologica_euro_ens_strip_v2_current_hour_filter_{r.lower()}", meteologica_euro_ens_forecast.pull_strip, {"region": r})
      for r in ["WEST", "MIDATL", "SOUTH"]],

    # load_forecast_vintage_combined fragment
    *[(f"forecast_vintage_{src}_{r.lower()}", _pull_combined_source_vintages, {"source": src, "region": r})
      for src in ("pjm", "meteologica") for r in REGIONS],
    *[(f"meteologica_euro_ens_vintage_{r.lower()}", meteologica_euro_ens_forecast.pull_strip, {"region": r})
      for r in REGIONS],

    # load_forecast_changes_rto fragment
    ("forecast_evolution_pjm_latest_da_cutoff_v2", _pull_changes_source_vintages, {"source": "pjm", "region": "RTO"}),
    ("forecast_evolution_meteologica_latest_da_cutoff_v2", _pull_changes_source_vintages, {"source": "meteologica", "region": "RTO"}),

    # meteologica_vintage_table fragment
    ("meteo_vintage_table_v1", _pull_meteologica_vintages_table, {}),

]


def _pull_one(
    source_name: str,
    pull_fn: callable,
    pull_kwargs: dict,
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
        sources = [(n, fn, kw) for n, fn, kw in sources if n in only]
        unknown = set(only) - {n for n, _, _ in sources}
        if unknown:
            logger.warning(f"Unknown sources (skipped): {unknown}")
            logger.info(f"Available: {[n for n, _, _ in SOURCE_REGISTRY]}")

    logger.info(f"Refreshing {len(sources)} data sources → {cache_dir} (workers={max_workers})")
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_pull_one, name, fn, kw, cache_dir, ttl_hours): name
            for name, fn, kw in sources
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
        for name, _, kwargs in SOURCE_REGISTRY:
            print(f"  {name:45s} {kwargs or ''}")
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
