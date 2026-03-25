#!/usr/bin/env python3
"""W&B sweep runner for like-day forecast scenarios.

Run from backend/:
  cd backend

Modes:
  1. Single run — test one config, log to W&B:
       python -m src.like_day_forecast.scripts.run_sweep --single --forecast-date 2026-03-12

  2. Launch a W&B sweep from YAML:
       python -m src.like_day_forecast.scripts.run_sweep --sweep-config src/like_day_forecast/sweeps/grid_search.yaml --count 50

  3. Attach to an existing sweep:
       python -m src.like_day_forecast.scripts.run_sweep --sweep-id ENTITY/PROJECT/SWEEP_ID --count 50

  4. Backtest — run a date range with fixed config:
       python -m src.like_day_forecast.scripts.run_sweep --backtest --start-date 2026-02-15 --end-date 2026-03-12

  5. Backtest via sweep (for parallel agents):
       python -m src.like_day_forecast.scripts.run_sweep --sweep-config src/like_day_forecast/sweeps/backtest.yaml --count 26

  6. Validate data before a sweep:
       python -m src.like_day_forecast.scripts.run_sweep --sweep-config src/like_day_forecast/sweeps/grid_search.yaml --validate --count 224

  7. Run validation only (no sweep):
       python -m src.like_day_forecast.scripts.run_sweep --validate-only
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import date, timedelta
from pathlib import Path

# Add backend/ to sys.path so `from src.like_day_forecast import ...` works
# scripts/ lives at backend/src/like_day_forecast/scripts/ → go up 3 levels to backend/
_BACKEND = str(Path(__file__).resolve().parent.parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import src.like_day_forecast.settings  # noqa: E402 — loads .env, init logging
from src.like_day_forecast.utils.logging_utils import get_logger  # noqa: E402
from src.like_day_forecast.configs import (  # noqa: E402
    ScenarioConfig,
    FEATURE_GROUP_WEIGHTS,
)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import wandb  # noqa: E402

WANDB_PROJECT = "pjm-like-day-forecast"
logger = get_logger()


# ─── Helpers ────────────────────────────────────────────────────────────


def _config_hash(cfg: ScenarioConfig) -> str:
    """Short hash of the scenario config for dedup/naming."""
    flat = cfg.to_flat_dict()
    raw = str(sorted(flat.items()))
    return hashlib.sha256(raw.encode()).hexdigest()[:8]


def _build_config_from_wandb(wbc: wandb.Config, base: ScenarioConfig) -> ScenarioConfig:
    """Overlay W&B sweep config onto a base ScenarioConfig."""
    base.forecast_date = wbc.get("forecast_date", base.forecast_date)
    base.n_analogs = int(wbc.get("n_analogs", base.n_analogs))
    base.weight_method = wbc.get("weight_method", base.weight_method)
    base.season_window_days = int(wbc.get("season_window_days", base.season_window_days))
    base.same_dow_group = bool(wbc.get("same_dow_group", base.same_dow_group))
    base.apply_calendar_filter = bool(wbc.get("apply_calendar_filter", base.apply_calendar_filter))
    base.apply_regime_filter = bool(wbc.get("apply_regime_filter", base.apply_regime_filter))

    # Feature weight overrides: sweep params like w_lmp_profile → rebuild dict
    weight_overrides = {
        k[2:]: float(v) for k, v in dict(wbc).items() if k.startswith("w_")
    }
    if weight_overrides:
        merged = dict(FEATURE_GROUP_WEIGHTS)
        merged.update(weight_overrides)
        base.feature_group_weights = merged

    # Generate a descriptive name
    base.name = (
        f"{base.forecast_date}_n{base.n_analogs}_{base.weight_method}"
        f"_sw{base.season_window_days}"
    )
    return base


def _log_hourly_profile(result: dict) -> None:
    """Log the hourly forecast profile as a wandb.Table for rich comparison."""
    df_fc = result.get("df_forecast")
    if df_fc is None or df_fc.empty:
        return

    # Build table with hour, forecast, and quantile columns
    columns = ["hour_ending", "point_forecast"]
    q_cols = [c for c in df_fc.columns if c.startswith("q_")]
    columns.extend(q_cols)

    data = []
    for _, row in df_fc.iterrows():
        data.append([row.get(c) for c in columns])

    # Add actuals if available
    output_table = result.get("output_table")
    if output_table is not None and not output_table.empty:
        actual_row = output_table[output_table["Type"] == "Actual"]
        if not actual_row.empty:
            columns.append("actual")
            for i, (_, row) in enumerate(df_fc.iterrows()):
                h = int(row["hour_ending"])
                he_col = f"HE{h}"
                actual_val = actual_row[he_col].values[0] if he_col in actual_row.columns else None
                data[i].append(actual_val)

    wandb.log({"hourly_forecast": wandb.Table(columns=columns, data=data)})


def _log_step_metrics(result: dict) -> None:
    """Log hourly forecast/actual as step metrics for W&B native charts."""
    df_fc = result.get("df_forecast")
    if df_fc is None or df_fc.empty:
        return

    output_table = result.get("output_table")
    actual_row = None
    if output_table is not None and not output_table.empty:
        actual_rows = output_table[output_table["Type"] == "Actual"]
        if not actual_rows.empty:
            actual_row = actual_rows.iloc[0]

    for _, row in df_fc.iterrows():
        h = int(row["hour_ending"])
        step_data = {"forecast_lmp": row["point_forecast"]}
        if actual_row is not None:
            he_col = f"HE{h}"
            if he_col in actual_row.index and pd.notna(actual_row[he_col]):
                step_data["actual_lmp"] = float(actual_row[he_col])
                step_data["error"] = row["point_forecast"] - float(actual_row[he_col])
        # Log q05/q95 bands
        if "q_0.05" in row.index:
            step_data["q05"] = row["q_0.05"]
        if "q_0.95" in row.index:
            step_data["q95"] = row["q_0.95"]
        wandb.log(step_data, step=h)


def _log_analogs(result: dict) -> None:
    """Log analog table as a wandb.Table."""
    analogs_df = result.get("analogs")
    if analogs_df is None or analogs_df.empty:
        return
    top = analogs_df.head(30).copy()
    top["date"] = top["date"].astype(str)
    wandb.log({"analogs": wandb.Table(dataframe=top)})


# ─── Core run function ──────────────────────────────────────────────────


def run_one(config: ScenarioConfig) -> dict:
    """Execute a single forecast run and log everything to W&B."""
    from src.like_day_forecast.pipelines.forecast import run

    logger.info(f"Running scenario: {config.name}")
    result = run(config=config)

    if "error" in result:
        logger.error(f"Pipeline returned error: {result['error']}")
        wandb.log({"error": result["error"]})
        return result

    # ── Scalar metrics ──
    metrics = result.get("metrics")
    if metrics:
        scalar_metrics = {
            k: float(v) for k, v in metrics.items()
            if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool)
        }
        wandb.log(scalar_metrics)

    wandb.log({
        "n_analogs_used": result.get("n_analogs_used", 0),
        "has_actuals": int(result.get("has_actuals", False)),
    })

    # ── Rich tables ──
    _log_hourly_profile(result)
    _log_step_metrics(result)
    _log_analogs(result)

    # ── Summary tags ──
    wandb.run.tags = wandb.run.tags + (
        config.weight_method,
        f"n{config.n_analogs}",
        f"sw{config.season_window_days}",
    )
    if config.forecast_date:
        wandb.run.tags = wandb.run.tags + (config.forecast_date,)

    logger.info(f"Completed: {config.name} — "
                f"MAE={metrics.get('mae', 'N/A') if metrics else 'N/A'}")
    return result


# ─── Sweep agent entry point ────────────────────────────────────────────


def sweep_fn() -> None:
    """Called by wandb.agent() for each sweep trial."""
    with wandb.init(project=WANDB_PROJECT, reinit=True):
        config = _build_config_from_wandb(wandb.config, ScenarioConfig())
        wandb.run.name = config.name
        # Log full flat config
        wandb.config.update(config.to_flat_dict(), allow_val_change=True)
        run_one(config)


# ─── Backtest mode ──────────────────────────────────────────────────────


def run_backtest(
    start_date: str,
    end_date: str,
    base_config: ScenarioConfig,
) -> list[dict]:
    """Run the model across a date range, one W&B run per date."""
    start = pd.to_datetime(start_date).date()
    end = pd.to_datetime(end_date).date()
    dates = pd.date_range(start, end, freq="D")

    logger.info(f"Backtest: {len(dates)} dates from {start} to {end}")

    results = []
    for dt in dates:
        forecast_date = dt.strftime("%Y-%m-%d")
        config = ScenarioConfig(
            name=f"backtest_{forecast_date}_n{base_config.n_analogs}_{base_config.weight_method}",
            forecast_date=forecast_date,
            n_analogs=base_config.n_analogs,
            weight_method=base_config.weight_method,
            feature_group_weights=base_config.feature_group_weights,
            season_window_days=base_config.season_window_days,
            same_dow_group=base_config.same_dow_group,
            apply_calendar_filter=base_config.apply_calendar_filter,
            apply_regime_filter=base_config.apply_regime_filter,
            schema=base_config.schema,
            hub=base_config.hub,
        )

        with wandb.init(
            project=WANDB_PROJECT,
            config=config.to_flat_dict(),
            name=config.name,
            tags=["backtest", config.weight_method, f"n{config.n_analogs}"],
            group=f"backtest_{start_date}_to_{end_date}",
            reinit=True,
        ):
            result = run_one(config)
            results.append(result)

    return results


# ─── CLI ─────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="W&B sweep runner for like-day forecast scenarios.",
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--single", action="store_true",
                      help="Run a single scenario and log to W&B.")
    mode.add_argument("--sweep-config", type=str,
                      help="Path to sweep YAML config. Creates sweep and runs agent.")
    mode.add_argument("--sweep-id", type=str,
                      help="Existing W&B sweep ID to attach agent to.")
    mode.add_argument("--backtest", action="store_true",
                      help="Run backtest over a date range.")
    mode.add_argument("--validate-only", action="store_true",
                      help="Run data validation only (no sweep).")

    # Single-run params
    p.add_argument("--forecast-date", type=str, default=None)
    p.add_argument("--n-analogs", type=int, default=30)
    p.add_argument("--weight-method", type=str, default="inverse_distance")
    p.add_argument("--season-window-days", type=int, default=30)
    p.add_argument("--same-dow-group", type=bool, default=True)
    p.add_argument("--name", type=str, default=None)

    # Sweep params
    p.add_argument("--count", type=int, default=None,
                   help="Max number of sweep runs to execute.")
    p.add_argument("--entity", type=str, default=None,
                   help="W&B entity (team/user).")

    # Backtest params
    p.add_argument("--start-date", type=str, default=None)
    p.add_argument("--end-date", type=str, default=None)

    # Validation params
    p.add_argument("--validate", action="store_true",
                   help="Run Evidently data validation before sweep/backtest. "
                        "Aborts if validation fails.")
    p.add_argument("--skip-validation", action="store_true",
                   help="Explicitly skip validation (overrides --validate).")
    p.add_argument("--validation-report-dir", type=str, default=None,
                   help="Directory to save Evidently HTML report. "
                        "Defaults to src/like_day_forecast/validation/reports/")
    p.add_argument("--drift-threshold", type=float, default=0.25,
                   help="Max share of drifted feature columns (default: 0.25).")

    return p.parse_args()


def _run_validation(args: argparse.Namespace) -> bool:
    """Run Evidently preflight validation. Returns True if passed."""
    from src.like_day_forecast.validation.preflight import run_preflight

    report_dir = args.validation_report_dir or str(
        Path(__file__).resolve().parent.parent / "validation" / "reports"
    )

    logger.info("Running Evidently preflight validation...")
    result = run_preflight(
        output_dir=report_dir,
        drift_threshold=args.drift_threshold,
    )

    print("\n" + result.summary + "\n")

    # Log validation report to W&B as artifact (if in a wandb context)
    if result.report_path:
        try:
            artifact = wandb.Artifact(
                name="preflight-validation",
                type="validation-report",
                metadata={
                    "passed": result.passed,
                    "drift_share": result.drift_share,
                    "source_checks": len(result.source_checks),
                },
            )
            artifact.add_file(result.report_path)
            wandb.log_artifact(artifact)
            logger.info("Validation report logged as W&B artifact")
        except Exception:
            # Not in a wandb run context — that's fine for validate-only mode
            pass

    return result.passed


def main() -> int:
    args = parse_args()

    # ── Validate-only mode ──
    if args.validate_only:
        # Init a short-lived W&B run to log the artifact
        with wandb.init(
            project=WANDB_PROJECT,
            name=f"validation_{date.today().isoformat()}",
            tags=["validation"],
            job_type="validation",
        ):
            passed = _run_validation(args)
        return 0 if passed else 1

    # ── Pre-sweep validation gate ──
    if args.validate and not args.skip_validation:
        passed = _run_validation(args)
        if not passed:
            logger.error(
                "Preflight validation FAILED — aborting sweep. "
                "Use --skip-validation to override."
            )
            return 1
        logger.info("Preflight validation PASSED — proceeding with sweep.")

    if args.single:
        config = ScenarioConfig(
            name=args.name or f"single_{args.forecast_date or 'tomorrow'}",
            forecast_date=args.forecast_date,
            n_analogs=args.n_analogs,
            weight_method=args.weight_method,
            season_window_days=args.season_window_days,
            same_dow_group=args.same_dow_group,
        )
        with wandb.init(
            project=WANDB_PROJECT,
            config=config.to_flat_dict(),
            name=config.name,
            tags=["single", config.weight_method, f"n{config.n_analogs}"],
        ):
            result = run_one(config)
        if "error" in result:
            return 1
        return 0

    elif args.sweep_config:
        sweep_path = Path(args.sweep_config)
        if not sweep_path.exists():
            logger.error(f"Sweep config not found: {sweep_path}")
            return 1

        import yaml
        with open(sweep_path) as f:
            sweep_cfg = yaml.safe_load(f)

        sweep_id = wandb.sweep(
            sweep=sweep_cfg,
            project=WANDB_PROJECT,
            entity=args.entity,
        )
        logger.info(f"Created sweep: {sweep_id}")
        wandb.agent(sweep_id, function=sweep_fn, count=args.count)
        return 0

    elif args.sweep_id:
        wandb.agent(args.sweep_id, function=sweep_fn, count=args.count)
        return 0

    elif args.backtest:
        if not args.start_date or not args.end_date:
            logger.error("--backtest requires --start-date and --end-date")
            return 1

        base_config = ScenarioConfig(
            n_analogs=args.n_analogs,
            weight_method=args.weight_method,
            season_window_days=args.season_window_days,
            same_dow_group=args.same_dow_group,
        )
        run_backtest(args.start_date, args.end_date, base_config)
        return 0

    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logger.error(f"Fatal error: {exc}")
        raise SystemExit(1) from exc
