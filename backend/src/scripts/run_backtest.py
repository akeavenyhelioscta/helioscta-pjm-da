"""CLI: run shared walk-forward backtest across like-day and LASSO QR models."""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from src.backtesting.config import BacktestConfig
from src.backtesting.engine import run_backtest
from src.backtesting.io import save_backtest_outputs, write_markdown_summary

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run walk-forward forecast backtest")
    parser.add_argument("--start", required=True, help="Start delivery date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End delivery date YYYY-MM-DD")
    parser.add_argument(
        "--models",
        default="like_day,lasso_qr",
        help="Comma-separated models (like_day,lasso_qr)",
    )
    parser.add_argument(
        "--retrain-every",
        type=int,
        default=1,
        help="LASSO retrain cadence in days (1=strict walk-forward)",
    )
    parser.add_argument(
        "--outdir",
        default=None,
        help="Output directory (default: backend/output/backtests/<timestamp>)",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=None,
        help="Optional cap on number of forecast days",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Reserved flag; current implementation runs sequentially",
    )
    parser.add_argument(
        "--keep-incomplete-days",
        action="store_true",
        help="Keep dates with partial/missing actuals in outputs",
    )
    parser.add_argument(
        "--weekdays-only",
        action="store_true",
        help="Exclude weekends (Sat/Sun) from backtest date range",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Generate an HTML dashboard report alongside parquet + markdown",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()

    if args.parallel:
        logger.warning("--parallel flag is reserved; running sequentially")

    model_list = [m.strip().lower() for m in args.models.split(",") if m.strip()]
    if not model_list:
        raise ValueError("No models provided")

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        outdir = Path("backend/output/backtests") / stamp

    cfg = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        models=model_list,
        retrain_every_n_days=max(args.retrain_every, 1),
        output_dir=outdir,
        max_days=args.max_days,
        drop_incomplete_days=not args.keep_incomplete_days,
        weekdays_only=args.weekdays_only,
    )
    logger.info(
        "Running backtest start=%s end=%s models=%s retrain_every=%s weekdays_only=%s",
        cfg.start_date,
        cfg.end_date,
        ",".join(cfg.models),
        cfg.retrain_every_n_days,
        cfg.weekdays_only,
    )

    result = run_backtest(cfg)
    paths = save_backtest_outputs(result, cfg.output_dir)
    md_path = write_markdown_summary(result, cfg.output_dir / "comparison.md")

    if args.html:
        from src.backtesting.report import write_backtest_report

        html_path = write_backtest_report(
            result,
            cfg.output_dir / "backtest_report.html",
            title=f"Backtest {cfg.start_date} to {cfg.end_date}",
        )
        logger.info("html_report: %s", html_path)

    logger.info("Backtest complete")
    for key, path in paths.items():
        logger.info("%s: %s", key, path)
    logger.info("summary: %s", md_path)


if __name__ == "__main__":
    main()
