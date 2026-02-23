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
from src.pjm_like_day.like_day import reshape_to_daily, find_like_days
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


def test_reshape():
    """Check that reshape produces correct vectors."""
    print("\n" + "=" * 60)
    print("2. RESHAPE CHECK")
    print("=" * 60)

    df = lmps.pull(hub=HUB, market=MARKET)
    df[configs.DATE_COL] = pd.to_datetime(df[configs.DATE_COL]).dt.date

    # Single feature (lmp_total only)
    X_single, dates_single = reshape_to_daily(df, feature_cols=["lmp_total"])
    print(f"Single feature (lmp_total) shape: {X_single.shape}")
    print(f"  Expected: ({dates_single.shape[0]}, 24)")

    # All features
    X_all, dates_all = reshape_to_daily(df, feature_cols=configs.FEATURE_COLS)
    print(f"All features shape: {X_all.shape}")
    print(f"  Expected: ({dates_all.shape[0]}, {24 * len(configs.FEATURE_COLS)})")

    # Check a target day vector
    target_idx = dates_single[dates_single == TARGET].index
    if len(target_idx) > 0:
        idx = dates_single.index.get_loc(target_idx[0])
        print(f"\nTarget day vector (lmp_total, first 6 hours): {X_single[idx, :6].round(2)}")


def test_similarity_single_vs_multi():
    """Compare results using 1 feature vs all 4."""
    print("\n" + "=" * 60)
    print("3. SIMILARITY: SINGLE FEATURE vs ALL FEATURES")
    print("=" * 60)

    # Run with all 4 features
    out_all = run(
        target_date=TARGET, hub=HUB, market=MARKET,
        n_neighbors=5, metric="cosine",
        feature_cols=configs.FEATURE_COLS,
    )
    print("All 4 features (cosine):")
    print(tabulate(out_all["like_days"], headers="keys", tablefmt="psql", showindex=False))

    # Run with just lmp_total
    out_single = run(
        target_date=TARGET, hub=HUB, market=MARKET,
        n_neighbors=5, metric="cosine",
        feature_cols=["lmp_total"],
    )
    print("\nlmp_total only (cosine):")
    print(tabulate(out_single["like_days"], headers="keys", tablefmt="psql", showindex=False))

    # Run with just lmp_total + euclidean
    out_euc = run(
        target_date=TARGET, hub=HUB, market=MARKET,
        n_neighbors=5, metric="euclidean",
        feature_cols=["lmp_total"],
    )
    print("\nlmp_total only (euclidean):")
    print(tabulate(out_euc["like_days"], headers="keys", tablefmt="psql", showindex=False))


def test_profile_comparison():
    """Print hourly profiles for target + top like day to visually compare."""
    print("\n" + "=" * 60)
    print("4. HOURLY PROFILE COMPARISON")
    print("=" * 60)

    out = run(
        target_date=TARGET, hub=HUB, market=MARKET,
        n_neighbors=3, metric="cosine",
        feature_cols=["lmp_total"],
    )

    profiles = out["hourly_profiles"]
    target_profile = profiles[profiles[configs.DATE_COL] == TARGET].sort_values(configs.HOUR_ENDING_COL)
    like1_date = out["like_days"].iloc[0][configs.DATE_COL]
    like1_profile = profiles[profiles[configs.DATE_COL] == like1_date].sort_values(configs.HOUR_ENDING_COL)

    comparison = pd.DataFrame({
        "hour": target_profile[configs.HOUR_ENDING_COL].values,
        f"target ({TARGET})": target_profile["lmp_total"].values,
        f"like1 ({like1_date})": like1_profile["lmp_total"].values,
    })
    comparison["diff"] = comparison.iloc[:, 2] - comparison.iloc[:, 1]

    print(tabulate(comparison, headers="keys", tablefmt="psql", showindex=False, floatfmt=".2f"))
    print(f"\nMean absolute difference: {comparison['diff'].abs().mean():.2f}")
    print(f"Correlation: {np.corrcoef(comparison.iloc[:, 1], comparison.iloc[:, 2])[0, 1]:.4f}")


if __name__ == "__main__":
    test_data_shape()
    test_reshape()
    test_similarity_single_vs_multi()
    test_profile_comparison()
