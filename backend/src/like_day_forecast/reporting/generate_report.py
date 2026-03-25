"""Generate all input data validation HTML reports.

Run this script to produce one HTML report per data source.
Reports are saved to reporting/output/.

Usage:
    python generate_report.py                          # normal (uses cache)
    python generate_report.py --force-refresh           # bypass cache
    python generate_report.py --cache-ttl 1             # 1-hour TTL
    python generate_report.py --no-cache                # disable caching entirely

Environment overrides (applied before CLI args):
    CACHE_ENABLED=false
    CACHE_TTL_HOURS=2
    FORCE_CACHE_REFRESH=true
"""
import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Allow running as `python generate_report.py` from any directory
_BACKEND = str(Path(__file__).resolve().parent.parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from src.like_day_forecast import configs
from src.like_day_forecast.utils.html_dashboard import HTMLDashboardBuilder
from src.like_day_forecast.utils.logging_utils import get_pipeline_logger
from src.like_day_forecast.reporting.fragments import rt_load_metered_rto as rt_load_metered_rto_fragments
from src.like_day_forecast.reporting.fragments import load_forecast_rto as load_forecast_rto_fragments
from src.like_day_forecast.reporting.fragments import load_forecast_changes_rto as load_forecast_changes_rto_fragments
from src.like_day_forecast.reporting.master_report import build_master

logger = logging.getLogger(__name__)

REPORT_OUTPUT_DIR = Path(__file__).parent / "output"

# Register fragment builders here — add new sources as they're implemented.
FRAGMENT_REGISTRY = {
    "rt_load_metered_rto": ("RT Load Metered RTO", rt_load_metered_rto_fragments.build_fragments),
    "load_forecast_rto": ("Load Forecasts RTO", load_forecast_rto_fragments.build_fragments),
    "forecast_evolution": ("Load Forecast Changes RTO (PJM + Meteologica)", load_forecast_changes_rto_fragments.build_fragments),
    # "lmp": ("LMP Data", lmp_fragments.build_fragments),
    # "gas": ("Gas Prices", gas_fragments.build_fragments),
    # "weather": ("Weather", weather_fragments.build_fragments),
}


def generate(
    schema: str = configs.SCHEMA,
    output_dir: Path | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> dict[str, Path]:
    """Generate individual + combined validation reports.

    Produces one HTML per source, plus a combined report that joins
    all sources into a single scrollable dashboard.

    Returns:
        Dict mapping report key to output file path.
    """
    pl = get_pipeline_logger()
    output_dir = output_dir or REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Header ────────────────────────────────────────────────────
    if pl:
        pl.header("Like-Day Validation Reports")
    else:
        logger.info("Like-Day Validation Reports")

    # ── Cache config ──────────────────────────────────────────────
    if pl:
        pl.section("Cache Configuration")
    if cache_enabled:
        logger.info(f"Cache: ON | dir={cache_dir} | TTL={cache_ttl_hours}h | force_refresh={force_refresh}")
    else:
        logger.info("Cache: OFF — all pulls will hit the database")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    results: dict[str, Path] = {}
    all_fragments: list[tuple[str, list]] = []  # (label, fragments) per source

    # ── Individual reports ────────────────────────────────────────
    if pl:
        pl.section("Building Individual Reports")

    for source_key, (label, build_fn) in FRAGMENT_REGISTRY.items():
        logger.info(f"Generating {label} validation report...")

        if pl:
            with pl.timer(f"{label} report"):
                fragments = build_fn(schema=schema, **cache_kwargs)
        else:
            fragments = build_fn(schema=schema, **cache_kwargs)

        all_fragments.append((label, fragments))

        builder = HTMLDashboardBuilder(
            title=f"{label} Validation — {date.today().isoformat()}",
            theme="dark",
        )
        _feed_fragments(builder, fragments)

        filename = f"validation_{source_key}_{date.today().isoformat()}.html"
        output_path = output_dir / filename
        builder.save(str(output_path))

        if pl:
            pl.success(f"Saved: {output_path}")
        else:
            logger.info(f"Saved: {output_path}")
        results[source_key] = output_path

    # ── Master report ────────────────────────────────────────────
    if len(results) >= 1:
        if pl:
            pl.section("Building Master Report")
        master_path = build_master(results, output_dir)
        if pl:
            pl.success(f"Saved: {master_path}")
        else:
            logger.info(f"Saved: {master_path}")
        results["master"] = master_path

    # ── Summary ───────────────────────────────────────────────────
    if pl:
        pl.section("Summary")
        for source, path in results.items():
            pl.info(f"  {source}: {path}")
        pl.success(f"{len(results)} reports generated")

    return results


def _feed_fragments(builder: HTMLDashboardBuilder, fragments: list) -> None:
    """Feed a fragment list (dividers + sections) into a builder."""
    for item in fragments:
        if isinstance(item, str):
            builder.add_divider(item)
        else:
            name, content, icon = item
            builder.add_content(name, content, icon=icon)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Like-Day validation reports")
    parser.add_argument(
        "--force-refresh", action="store_true", default=configs.FORCE_CACHE_REFRESH,
        help="Bypass cache and pull fresh data from the database",
    )
    parser.add_argument(
        "--cache-ttl", type=float, default=configs.CACHE_TTL_HOURS,
        help=f"Cache TTL in hours (default: {configs.CACHE_TTL_HOURS})",
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="Disable caching entirely for this run",
    )
    return parser.parse_args()


def main():
    """Entry point — initialize settings, parse args, generate all reports."""
    import src.like_day_forecast.settings

    args = _parse_args()

    results = generate(
        cache_enabled=configs.CACHE_ENABLED and not args.no_cache,
        cache_ttl_hours=args.cache_ttl,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()
