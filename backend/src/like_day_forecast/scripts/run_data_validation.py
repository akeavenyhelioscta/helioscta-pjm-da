#!/usr/bin/env python3
"""Runner script for data validation: Great Expectations + ydata-profiling.

Run from backend/:
  cd backend

Modes:
  1. Full validation (GX + profiling):
       python -m src.like_day_forecast.scripts.run_data_validation

  2. GX only (fast, seconds):
       python -m src.like_day_forecast.scripts.run_data_validation --skip-profiling

  3. Profiling only:
       python -m src.like_day_forecast.scripts.run_data_validation --profile-only

  4. Skip GX:
       python -m src.like_day_forecast.scripts.run_data_validation --skip-ge
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

# Add backend/ to sys.path
_BACKEND = str(Path(__file__).resolve().parent.parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import src.like_day_forecast.settings  # noqa: E402 — loads .env, init logging

import pandas as pd  # noqa: E402

from src.like_day_forecast import configs  # noqa: E402
from src.like_day_forecast.utils.logging_utils import get_logger  # noqa: E402

logger = get_logger()

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"


# ─── Data pull ───────────────────────────────────────────────────────────


def _pull_all_sources(schema: str) -> dict[str, pd.DataFrame]:
    """Pull all raw data sources used by the like-day forecast pipeline."""
    from src.like_day_forecast.data import (
        lmps_hourly,
        load_da_hourly,
        load_rt_metered_hourly,
        gas_prices,
        weather_hourly,
        dates,
    )

    sources: dict[str, pd.DataFrame] = {}

    logger.info("Pulling LMP hourly (DA)...")
    sources["lmp_hourly"] = lmps_hourly.pull(
        schema=schema, hub=configs.HUB, market="da"
    )

    logger.info("Pulling Load DA hourly...")
    try:
        sources["load_da_hourly"] = load_da_hourly.pull(schema=schema)
    except Exception as e:
        logger.warning(f"DA load pull failed: {e}")
        sources["load_da_hourly"] = pd.DataFrame()

    logger.info("Pulling Load RT metered hourly...")
    sources["load_rt_metered_hourly"] = load_rt_metered_hourly.pull(schema=schema)

    logger.info("Pulling gas prices...")
    sources["gas_prices"] = gas_prices.pull()

    logger.info("Pulling weather hourly...")
    try:
        sources["weather_hourly"] = weather_hourly.pull()
    except Exception as e:
        logger.warning(f"Weather pull failed: {e}")
        sources["weather_hourly"] = pd.DataFrame()

    logger.info("Pulling dates daily...")
    sources["dates_daily"] = dates.pull_daily(schema=schema)

    return sources


# ─── GX validation ───────────────────────────────────────────────────────


def _extract_failures(checkpoint_result) -> list[dict]:
    """Extract per-expectation failure details from a GX CheckpointResult."""
    failures = []
    try:
        for run_result in checkpoint_result.run_results.values():
            for exp_result in run_result.results:
                if not exp_result.success:
                    exp_config = exp_result.expectation_config
                    detail = {
                        "expectation": exp_config.type,
                        "kwargs": {
                            k: v
                            for k, v in exp_config.kwargs.items()
                            if k != "batch_id"
                        },
                    }
                    # Include result details (unexpected counts, observed value, etc.)
                    if exp_result.result:
                        for key in (
                            "observed_value",
                            "element_count",
                            "unexpected_count",
                            "unexpected_percent",
                            "missing_count",
                            "missing_percent",
                            "partial_unexpected_list",
                        ):
                            if key in exp_result.result:
                                detail[key] = exp_result.result[key]
                    failures.append(detail)
    except Exception as e:
        failures.append({"extraction_error": str(e)})
    return failures


def _run_ge_validation(
    sources: dict[str, pd.DataFrame],
    df_features: pd.DataFrame,
) -> dict:
    """Run all GX expectation suites and return a JSON-serializable summary."""
    from src.like_day_forecast.validation import expectations as gx_suites

    suite_map = {
        "lmp_hourly": gx_suites.validate_lmp_hourly,
        "load_da_hourly": gx_suites.validate_load_da_hourly,
        "load_rt_metered_hourly": gx_suites.validate_load_rt_metered_hourly,
        "gas_prices": gx_suites.validate_gas_prices,
        "weather_hourly": gx_suites.validate_weather_hourly,
        "dates_daily": gx_suites.validate_dates_daily,
    }

    results: dict = {}

    for source_key, validate_fn in suite_map.items():
        df = sources.get(source_key, pd.DataFrame())
        if df.empty:
            results[source_key] = {"success": False, "error": "No data available"}
            continue

        try:
            freshness = gx_suites.check_freshness(df)
            checkpoint_result = validate_fn(df)
            entry: dict = {
                "success": checkpoint_result.success,
                "freshness": freshness,
            }
            if not checkpoint_result.success:
                entry["failures"] = _extract_failures(checkpoint_result)
            results[source_key] = entry
        except Exception as e:
            logger.error(f"GX suite failed for {source_key}: {e}")
            results[source_key] = {"success": False, "error": str(e)}

    # Feature matrix
    if not df_features.empty:
        try:
            checkpoint_result = gx_suites.validate_feature_matrix(df_features)
            entry = {"success": checkpoint_result.success}
            if not checkpoint_result.success:
                entry["failures"] = _extract_failures(checkpoint_result)
            results["feature_matrix"] = entry
        except Exception as e:
            logger.error(f"GX suite failed for feature_matrix: {e}")
            results["feature_matrix"] = {"success": False, "error": str(e)}
    else:
        results["feature_matrix"] = {"success": False, "error": "Empty feature matrix"}

    return results


# ─── Profiling ───────────────────────────────────────────────────────────


def _run_profiling(
    sources: dict[str, pd.DataFrame],
    df_features: pd.DataFrame,
) -> list[str]:
    """Run ydata-profiling reports for all sources and feature matrix."""
    from src.like_day_forecast.validation.profiling import (
        profile_source,
        profile_feature_matrix,
        profile_feature_matrix_comparison,
    )

    report_paths: list[str] = []

    for source_name, df in sources.items():
        if df.empty:
            logger.warning(f"Skipping profiling for {source_name}: no data")
            continue
        try:
            path = profile_source(df, source_name)
            report_paths.append(str(path))
        except Exception as e:
            logger.error(f"Profiling failed for {source_name}: {e}")

    if not df_features.empty:
        try:
            path = profile_feature_matrix(df_features)
            report_paths.append(str(path))
        except Exception as e:
            logger.error(f"Feature matrix profiling failed: {e}")

        try:
            path = profile_feature_matrix_comparison(df_features)
            report_paths.append(str(path))
        except Exception as e:
            logger.error(f"Feature matrix comparison profiling failed: {e}")

    return report_paths


# ─── CLI ─────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run data validation: Great Expectations + ydata-profiling",
    )
    p.add_argument(
        "--skip-profiling",
        action="store_true",
        help="Skip ydata-profiling (GX only, fast).",
    )
    p.add_argument(
        "--skip-ge",
        action="store_true",
        help="Skip Great Expectations (profiling only).",
    )
    p.add_argument(
        "--profile-only",
        action="store_true",
        help="Alias for --skip-ge.",
    )
    p.add_argument(
        "--schema",
        default=configs.SCHEMA,
        help="Database schema (default: %(default)s).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    run_ge = not args.skip_ge and not args.profile_only
    run_profiling = not args.skip_profiling

    logger.info("=" * 60)
    logger.info("DATA VALIDATION (GX + ydata-profiling)")
    logger.info("=" * 60)
    logger.info(
        f"GX: {'ON' if run_ge else 'OFF'} | "
        f"Profiling: {'ON' if run_profiling else 'OFF'}"
    )

    # ── 1. Pull data ──
    logger.info("")
    logger.info("─" * 10 + " Pulling data sources " + "─" * 10)
    sources = _pull_all_sources(args.schema)

    # ── 2. Build feature matrix ──
    logger.info("")
    logger.info("─" * 10 + " Building feature matrix " + "─" * 10)
    from src.like_day_forecast.features.builder import build_daily_features

    df_features = build_daily_features(schema=args.schema)

    overall_success = True
    ge_passed = True

    # ── 3. GX validation ──
    if run_ge:
        logger.info("")
        logger.info("─" * 10 + " Great Expectations validation " + "─" * 10)
        ge_results = _run_ge_validation(sources, df_features)

        # Save JSON
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ge_output_path = (
            REPORTS_DIR / f"ge_validation_{date.today().isoformat()}.json"
        )
        with open(ge_output_path, "w") as f:
            json.dump(ge_results, f, indent=2, default=str)
        logger.info(f"GX results saved: {ge_output_path}")

        # Print summary
        for label, result in ge_results.items():
            status = "PASS" if result.get("success") else "FAIL"
            logger.info(f"  [{status}] {label}")
            if "error" in result:
                logger.error(f"         Error: {result['error']}")
            if "freshness" in result:
                fr = result["freshness"]
                fresh_status = "FRESH" if fr["fresh"] else "STALE"
                logger.info(
                    f"         Freshness: {fresh_status} "
                    f"(max_date={fr['max_date']}, {fr['stale_days']}d ago)"
                )
            if "failures" in result:
                for fail in result["failures"]:
                    exp = fail.get("expectation", "unknown")
                    kwargs = fail.get("kwargs", {})
                    col = kwargs.get("column", "")
                    observed = fail.get("observed_value")
                    unexpected_pct = fail.get("unexpected_percent")
                    detail_parts = []
                    if col:
                        detail_parts.append(f"column={col}")
                    if observed is not None:
                        detail_parts.append(f"observed={observed}")
                    if unexpected_pct is not None:
                        detail_parts.append(f"unexpected={unexpected_pct:.2f}%")
                    detail_str = ", ".join(detail_parts)
                    logger.warning(f"         FAIL: {exp} ({detail_str})")

        ge_passed = all(r.get("success", False) for r in ge_results.values())
        if not ge_passed:
            overall_success = False

    # ── 4. Profiling ──
    report_paths: list[str] = []
    if run_profiling:
        logger.info("")
        logger.info("─" * 10 + " ydata-profiling reports " + "─" * 10)
        report_paths = _run_profiling(sources, df_features)
        logger.info(f"Generated {len(report_paths)} profiling reports")
        for p in report_paths:
            logger.info(f"  {p}")

    # ── 5. Summary ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("VALIDATION COMPLETE")
    logger.info("=" * 60)
    if run_ge:
        logger.info(f"GX overall: {'PASS' if ge_passed else 'FAIL'}")
    if run_profiling:
        logger.info(f"Profiling reports: {len(report_paths)} generated")
    logger.info(f"Overall: {'PASS' if overall_success else 'FAIL'}")

    return 0 if overall_success else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logger.error(f"Fatal error: {exc}")
        raise SystemExit(1) from exc
