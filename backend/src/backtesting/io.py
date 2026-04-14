"""Backtest result persistence and markdown summaries."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.backtesting.engine import BacktestResult


def save_backtest_outputs(
    result: BacktestResult,
    output_dir: Path,
) -> dict[str, Path]:
    """Write core backtest artifacts to parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "hourly_predictions": output_dir / "hourly_predictions.parquet",
        "daily_metrics": output_dir / "daily_metrics.parquet",
        "aggregate_metrics": output_dir / "aggregate_metrics.parquet",
        "run_metadata": output_dir / "run_metadata.parquet",
    }
    result.hourly_predictions.to_parquet(paths["hourly_predictions"], index=False)
    result.daily_metrics.to_parquet(paths["daily_metrics"], index=False)
    result.aggregate_metrics.to_parquet(paths["aggregate_metrics"], index=False)
    result.run_metadata.to_parquet(paths["run_metadata"], index=False)
    return paths


def build_markdown_summary(result: BacktestResult) -> str:
    """Create a concise markdown leaderboard from aggregate metrics."""
    parts: list[str] = []
    parts.append("# Backtest Summary")

    if len(result.run_metadata) > 0:
        ok = (result.run_metadata["status"] == "ok").sum()
        total = len(result.run_metadata)
        parts.append(f"\nRuns: {ok}/{total} successful")

    if len(result.aggregate_metrics) == 0:
        parts.append("\nNo aggregate metrics available.")
        return "\n".join(parts)

    df = result.aggregate_metrics.copy()
    show_cols = [
        c for c in [
            "model", "period", "mae", "rmse", "mape", "bias",
            "mean_pinball", "crps", "coverage_80pct", "n_hours",
        ]
        if c in df.columns
    ]
    df = df[show_cols]
    parts.append("\n## Aggregate Metrics")
    parts.append(df.to_markdown(index=False))
    return "\n".join(parts)


def write_markdown_summary(result: BacktestResult, path: Path) -> Path:
    """Write markdown summary file and return its path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_markdown_summary(result), encoding="utf-8")
    return path
