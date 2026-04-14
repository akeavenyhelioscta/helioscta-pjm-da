"""Rebuild HTML report from existing backtest parquet files."""
import sys
from pathlib import Path

import pandas as pd

from src.backtesting.engine import BacktestResult
from src.backtesting.report import write_backtest_report

base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("backend/output/backtests/redesigned_3day")

result = BacktestResult(
    hourly_predictions=pd.read_parquet(base / "hourly_predictions.parquet"),
    daily_metrics=pd.read_parquet(base / "daily_metrics.parquet"),
    aggregate_metrics=pd.read_parquet(base / "aggregate_metrics.parquet"),
    run_metadata=pd.read_parquet(base / "run_metadata.parquet"),
)

out = write_backtest_report(result, base / "backtest_report.html", title="Backtest Report")
print(f"Report written: {out}")
