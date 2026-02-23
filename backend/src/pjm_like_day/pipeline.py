from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from tabulate import tabulate

from src.pjm_like_day import configs
from src.pjm_like_day.data import lmps
from src.pjm_like_day.like_day import find_like_days

import logging
logging.basicConfig(level=logging.DEBUG)


LMP_COLS = configs.FEATURE_COLS  # ["lmp_total", "lmp_system_energy_price", ...]


def _pull_and_prefix(hub: str, market: str) -> pd.DataFrame:
    """Pull LMP data for a market and prefix columns with market name."""
    df = lmps.pull(hub=hub, market=market)
    df[configs.DATE_COL] = pd.to_datetime(df[configs.DATE_COL]).dt.date

    # Prefix LMP columns: lmp_total → da_lmp_total
    rename_map = {col: f"{market}_{col}" for col in LMP_COLS}
    df = df.rename(columns=rename_map)

    # Keep only date, hour_ending, and prefixed columns
    keep = [configs.DATE_COL, configs.HOUR_ENDING_COL] + list(rename_map.values())
    return df[[c for c in keep if c in df.columns]]


def run(
        target_date: date = configs.TARGET_DATE,
        hub: str = configs.HUB,
        features: list[dict] | None = None,
        n_neighbors: int = 5,
        metric: str = "cosine",
        hist_start: Optional[date] = None,
        hist_end: Optional[date] = None,
        hours: Optional[list[int]] = None,
        days_of_week: Optional[list[int]] = None,
        months: Optional[list[int]] = None,
    ) -> dict:
    """
    Run the like-day pipeline with cross-market feature support.

    Args:
        target_date: The date to find like days for.
        hub: PJM pricing hub.
        features: List of {"market": str, "column": str, "weight": float} dicts.
                  Defaults to [{"market": "da", "column": "lmp_total", "weight": 1.0}].
        n_neighbors: Number of like days to return.
        metric: Distance metric (mae, rmse, euclidean, cosine).
        hist_start: Start of historical window (inclusive). None = no lower bound.
        hist_end: End of historical window (inclusive). None = no upper bound.
        hours: List of hours (1-24) to include in feature vectors. None = all 24.
        days_of_week: List of day-of-week (0=Sun..6=Sat) to include. None = all.
        months: List of months (1-12) to include. None = all.

    Returns:
        dict with keys:
            - like_days: DataFrame with date, rank, distance, similarity
            - hourly_profiles: DataFrame with hourly LMP data for target + like days
            - target_date: the target date used
    """
    if features is None:
        features = [{"market": "da", "column": "lmp_total", "weight": 1.0}]

    # 1. Determine unique markets needed
    unique_markets = sorted(set(f["market"] for f in features))
    logging.info(f"Markets to pull: {unique_markets}")

    # 2. Pull and join data from each market (prefixed columns)
    dfs = []
    for mkt in unique_markets:
        df_mkt = _pull_and_prefix(hub=hub, market=mkt)
        dfs.append(df_mkt)

    # Join on (date, hour_ending) — inner join keeps only dates present in ALL markets
    df = dfs[0]
    for df_mkt in dfs[1:]:
        df = df.merge(df_mkt, on=[configs.DATE_COL, configs.HOUR_ENDING_COL], how="inner")

    logging.info(f"Joined data: {len(df)} rows, {df[configs.DATE_COL].nunique()} dates")

    # 3. Filter hours (applied to both target and historical for consistent feature vectors)
    if hours is not None:
        df = df[df[configs.HOUR_ENDING_COL].isin(hours)]
        logging.info(f"Filtered to {len(hours)} hours: {hours}")

    # 4. Split target vs historicals
    df_target = df[df[configs.DATE_COL] == target_date]
    df_hist = df[df[configs.DATE_COL] < target_date]

    # 5. Filter historical pool by date range
    if hist_start is not None:
        df_hist = df_hist[df_hist[configs.DATE_COL] >= hist_start]
        logging.info(f"Historical start filter: >= {hist_start}")
    if hist_end is not None:
        df_hist = df_hist[df_hist[configs.DATE_COL] <= hist_end]
        logging.info(f"Historical end filter: <= {hist_end}")

    # 6. Filter historical pool by day of week (0=Sun..6=Sat)
    if days_of_week is not None:
        df_hist["_dow"] = pd.to_datetime(df_hist[configs.DATE_COL]).dt.dayofweek
        # pandas dayofweek: 0=Mon..6=Sun → convert to 0=Sun..6=Sat
        df_hist["_dow"] = (df_hist["_dow"] + 1) % 7
        df_hist = df_hist[df_hist["_dow"].isin(days_of_week)]
        df_hist = df_hist.drop(columns=["_dow"])
        logging.info(f"Filtered to days of week: {days_of_week}")

    # 7. Filter historical pool by month
    if months is not None:
        df_hist["_month"] = pd.to_datetime(df_hist[configs.DATE_COL]).dt.month
        df_hist = df_hist[df_hist["_month"].isin(months)]
        df_hist = df_hist.drop(columns=["_month"])
        logging.info(f"Filtered to months: {months}")

    logging.info(f"Target date: {target_date}  ({len(df_target)} rows)")
    logging.info(f"Historical dates: {df_hist[configs.DATE_COL].nunique()} days")

    # 8. Build feature_weights dict with prefixed column names
    feature_weights = {
        f"{f['market']}_{f['column']}": f["weight"]
        for f in features
    }
    logging.info(f"Feature weights: {feature_weights}")

    # 9. Find like days
    results = find_like_days(
        df_target=df_target,
        df_hist=df_hist,
        feature_weights=feature_weights,
        n_neighbors=n_neighbors,
        metric=metric,
    )

    # 10. Gather hourly profiles for like days + target (ALL hours, ALL referenced markets)
    like_dates = results[configs.DATE_COL].tolist()
    all_dates = like_dates + [target_date]

    profiles = []
    for mkt in unique_markets:
        df_full = lmps.pull(hub=hub, market=mkt)
        df_full[configs.DATE_COL] = pd.to_datetime(df_full[configs.DATE_COL]).dt.date
        df_full = df_full[df_full[configs.DATE_COL].isin(all_dates)].copy()
        df_full["market"] = mkt
        profiles.append(df_full)

    hourly_profiles = pd.concat(profiles, ignore_index=True) if profiles else pd.DataFrame()

    return {
        "like_days": results,
        "hourly_profiles": hourly_profiles,
        "target_date": target_date,
    }


if __name__ == "__main__":

    output = run()
    results = output["like_days"]

    print("\n" + "=" * 60)
    print(f"Like Days for {configs.TARGET_DATE}")
    print("=" * 60)
    print(tabulate(results, headers="keys", tablefmt="psql", showindex=False))
