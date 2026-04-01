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
_BACKEND = str(Path(__file__).resolve().parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from src.like_day_forecast import configs
from src.reporting.html_dashboard import HTMLDashboardBuilder
from src.like_day_forecast.utils.logging_utils import get_pipeline_logger
from src.reporting.fragments import rt_load_metered_rto as rt_load_metered_rto_fragments
from src.reporting.fragments import load_forecast_vintage_combined as load_forecast_vintage_combined_fragments
from src.reporting.fragments import solar_forecast_vintage_combined as solar_forecast_vintage_combined_fragments
from src.reporting.fragments import wind_forecast_vintage_combined as wind_forecast_vintage_combined_fragments
from src.reporting.fragments import net_load_forecast_vintage_combined as net_load_forecast_vintage_combined_fragments
from src.reporting.fragments import forecast_results as forecast_results_fragments
from src.reporting.fragments import market_adjusted_forecast as market_adjusted_forecast_fragments
from src.reporting.fragments import regression_adjusted_forecast as regression_adjusted_forecast_fragments
from src.reporting.fragments import fuel_mix as fuel_mix_fragments
from src.reporting.fragments import lmp_history as lmp_history_fragments
from src.reporting.fragments import outages_forecast_rto as outages_forecast_rto_fragments
from src.reporting.fragments import outages_term_bible as outages_term_bible_fragments
from src.reporting.fragments import outages_seasonal as outages_seasonal_fragments
from src.reporting.fragments import meteologica_da_price_forecast as meteologica_da_price_forecast_fragments
from src.reporting.fragments import strip_forecast as strip_forecast_fragments
from src.reporting.fragments import data_inspection as data_inspection_fragments
from src.reporting.fragments import balday as balday_fragments
from src.reporting.fragments import regional_spark_spreads as regional_spark_spreads_fragments
from src.reporting.fragments import tie_flows as tie_flows_fragments
from src.reporting.master_report import build_master
from src.utils.azure_blob_storage_utils import AzureBlobStorageClient

logger = logging.getLogger(__name__)

BLOB_PREFIX = "pjm-da/reports"

REPORT_OUTPUT_DIR = Path(__file__).parent / "output"

# Register fragment builders here — add new sources as they're implemented.
FRAGMENT_REGISTRY = {
    "forecast_results": ("Like Day Model", forecast_results_fragments.build_fragments),
    "market_adjusted_forecast": ("Market-Adjusted Forecast", market_adjusted_forecast_fragments.build_fragments),
    "regression_adjusted_forecast": ("Regression-Adjusted Forecast", regression_adjusted_forecast_fragments.build_fragments),
    "meteologica_da_price_forecast": ("Meteologica DA Price Forecast", meteologica_da_price_forecast_fragments.build_fragments),
    "strip_forecast": ("Strip Forecast", strip_forecast_fragments.build_fragments),
    "balday": ("Bal Day", balday_fragments.build_fragments),
    "rt_load_metered_rto": ("RT Load Metered RTO", rt_load_metered_rto_fragments.build_fragments),
    "load_forecast_vintage_combined": ("Load Forecast Vintages", load_forecast_vintage_combined_fragments.build_fragments),
    "solar_forecast_vintage_combined": ("Solar Forecast Vintages", solar_forecast_vintage_combined_fragments.build_fragments),
    "wind_forecast_vintage_combined": ("Wind Forecast Vintages", wind_forecast_vintage_combined_fragments.build_fragments),
    "net_load_forecast_vintage_combined": ("Net Load Forecast Vintages", net_load_forecast_vintage_combined_fragments.build_fragments),
    "regional_spark_spreads": ("Regional Spark Spreads", regional_spark_spreads_fragments.build_fragments),
    "fuel_mix": ("Fuel Mix", fuel_mix_fragments.build_fragments),
    "lmp_history": ("LMP History", lmp_history_fragments.build_fragments),
    "outages_forecast_rto": ("Forecast Outages RTO", outages_forecast_rto_fragments.build_fragments),
    "outages_term_bible": ("Outages Term Bible", outages_term_bible_fragments.build_fragments),
    "outages_seasonal": ("Seasonal Outages RTO", outages_seasonal_fragments.build_fragments),
    "tie_flows": ("Tie Flows", tie_flows_fragments.build_fragments),
    "data_inspection": ("Data Inspection", data_inspection_fragments.build_fragments),
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
    upload: bool = False,
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
        pl.header("DA Model Reports")
    else:
        logger.info("DA Model Reports")

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

    # ── Upload to Azure Blob Storage ─────────────────────────────
    if upload:
        import os
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            logger.warning("AZURE_STORAGE_CONNECTION_STRING not set — skipping upload")
        else:
            if pl:
                pl.section("Uploading to Azure Blob Storage")

            blob_client = AzureBlobStorageClient()
            blob_prefix = f"{BLOB_PREFIX}/{date.today().isoformat()}"
            blob_urls: dict[str, str] = {}

            # Upload individual sub-reports
            for key, path in results.items():
                if key == "master":
                    continue
                blob_name = f"{blob_prefix}/{path.name}"
                url = blob_client.upload_file(
                    file_path=path,
                    blob_name=blob_name,
                    content_type="text/html",
                )
                blob_urls[key] = url
                logger.info(f"Uploaded {key}: {url}")

            # Rewrite master report iframe src paths to blob URLs, then upload
            if "master" in results:
                master_html = results["master"].read_text(encoding="utf-8")
                for key, url in blob_urls.items():
                    local_filename = results[key].name
                    master_html = master_html.replace(
                        f'src="{local_filename}"', f'src="{url}"'
                    )
                    master_html = master_html.replace(
                        f'value="{local_filename}"', f'value="{url}"'
                    )

                master_blob_name = f"{blob_prefix}/master_report.html"
                master_url = blob_client.upload_html(
                    html_content=master_html,
                    blob_name=master_blob_name,
                )
                blob_urls["master"] = master_url
                logger.info(f"Uploaded master: {master_url}")

            results["blob_urls"] = blob_urls
            if pl:
                pl.success(f"Uploaded {len(blob_urls)} reports to Azure Blob Storage")

    # ── Summary ───────────────────────────────────────────────────
    if pl:
        pl.section("Summary")
        for source, path in results.items():
            if source == "blob_urls":
                continue
            pl.info(f"  {source}: {path}")
        if "blob_urls" in results:
            for source, url in results["blob_urls"].items():
                pl.info(f"  {source} (blob): {url}")
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
    parser.add_argument(
        "--upload", action="store_true",
        help="Upload reports to Azure Blob Storage after generation",
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
        upload=args.upload,
    )


if __name__ == "__main__":
    main()
