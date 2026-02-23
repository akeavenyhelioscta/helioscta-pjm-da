"""
Quick diagnostic script for the like-day pipeline.
Run inside the backend container:
    docker compose exec backend python test_pipeline.py

Or locally (with .env vars set):
    cd backend && python test_pipeline.py
"""
from datetime import date
import numpy as np
import pandas as pd
from tabulate import tabulate

from src.pjm_like_day import configs
from src.pjm_like_day.data import lmps
from src.pjm_like_day.like_day import find_like_days
from src.pjm_like_day.pipeline import run


TARGET = date(2026, 2, 23)
HUB = "WESTERN HUB"
MARKET = "da"


def test_data_shape():
    """Check raw data coming from DB."""
    print("=" * 60)
    print("1. RAW DATA CHECK")
    print("=" * 60)

    df = lmps.pull(hub=HUB, market=MARKET)
    df[configs.DATE_COL] = pd.to_datetime(df[configs.DATE_COL]).dt.date

    print(f"Total rows: {len(df):,}")
    print(f"Date range: {df[configs.DATE_COL].min()} to {df[configs.DATE_COL].max()}")
    print(f"Unique dates: {df[configs.DATE_COL].nunique()}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nFeature stats (all dates):")
    print(df[configs.FEATURE_COLS].describe().round(2).to_string())

    # Target date
    df_target = df[df[configs.DATE_COL] == TARGET]
    print(f"\nTarget date ({TARGET}) rows: {len(df_target)}")
    if len(df_target) > 0:
        print(f"Target lmp_total range: {df_target['lmp_total'].min():.2f} to {df_target['lmp_total'].max():.2f}")
        print(f"Target lmp_total mean: {df_target['lmp_total'].mean():.2f}")


def test_metrics():
    """Compare results across different metrics."""
    print("\n" + "=" * 60)
    print("2. METRIC COMPARISON (lmp_total only)")
    print("=" * 60)

    for metric in ["mae", "rmse", "euclidean", "cosine"]:
        out = run(
            target_date=TARGET, hub=HUB, market=MARKET,
            n_neighbors=5, metric=metric,
            feature_cols=["lmp_total"],
        )
        print(f"\n{metric.upper()}:")
        print(tabulate(out["like_days"], headers="keys", tablefmt="psql", showindex=False))


def test_profile_comparison():
    """Print hourly profiles for target + top like days to visually compare."""
    print("\n" + "=" * 60)
    print("3. HOURLY PROFILE COMPARISON (MAE, lmp_total)")
    print("=" * 60)

    out = run(
        target_date=TARGET, hub=HUB, market=MARKET,
        n_neighbors=3, metric="mae",
        feature_cols=["lmp_total"],
    )

    profiles = out["hourly_profiles"]
    target_profile = profiles[profiles[configs.DATE_COL] == TARGET].sort_values(configs.HOUR_ENDING_COL)

    for i, row in out["like_days"].iterrows():
        like_date = row[configs.DATE_COL]
        like_profile = profiles[profiles[configs.DATE_COL] == like_date].sort_values(configs.HOUR_ENDING_COL)

        comparison = pd.DataFrame({
            "hour": target_profile[configs.HOUR_ENDING_COL].values,
            f"target ({TARGET})": target_profile["lmp_total"].values,
            f"like{row['rank']} ({like_date})": like_profile["lmp_total"].values,
        })
        comparison["diff"] = comparison.iloc[:, 2] - comparison.iloc[:, 1]

        print(f"\n--- Rank {int(row['rank'])}: {like_date} (dist={row['distance']:.2f}) ---")
        print(tabulate(comparison, headers="keys", tablefmt="psql", showindex=False, floatfmt=".2f"))
        print(f"MAE: {comparison['diff'].abs().mean():.2f}")
        print(f"Correlation: {np.corrcoef(comparison.iloc[:, 1], comparison.iloc[:, 2])[0, 1]:.4f}")


if __name__ == "__main__":
    test_data_shape()
    test_metrics()
    test_profile_comparison()
