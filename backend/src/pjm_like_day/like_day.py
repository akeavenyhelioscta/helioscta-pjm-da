import numpy as np
import pandas as pd

from src.pjm_like_day import configs

import logging
logging.basicConfig(level=logging.DEBUG)


def _compute_metric(diff: np.ndarray, metric: str,
                    target_flat: np.ndarray | None = None,
                    hist_flat: np.ndarray | None = None) -> float:
    """Compute a single distance value from a diff array (or flat vectors for cosine)."""
    if metric == "mae":
        return float(np.abs(diff).mean())
    elif metric == "rmse":
        return float(np.sqrt((diff ** 2).mean()))
    elif metric == "euclidean":
        return float(np.sqrt((diff ** 2).sum()))
    elif metric == "cosine":
        if target_flat is None or hist_flat is None:
            return float(np.abs(diff).mean())
        denom = np.linalg.norm(target_flat) * np.linalg.norm(hist_flat)
        if denom == 0:
            return 1.0
        return float(1.0 - np.dot(target_flat, hist_flat) / denom)
    else:
        return float(np.abs(diff).mean())


def find_like_days(
        df_target: pd.DataFrame,
        df_hist: pd.DataFrame,
        feature_weights: dict[str, float] | None = None,
        feature_cols: list[str] = configs.FEATURE_COLS,
        n_neighbors: int = 5,
        metric: str = "mae",
        date_col: str = configs.DATE_COL,
        hour_col: str = configs.HOUR_ENDING_COL,
    ) -> pd.DataFrame:
    """
    Compare hourly profiles between target day and each historical day.

    Supports multi-feature weighted ranking: computes per-feature distances,
    z-score normalizes across the historical pool, then blends with weights.

    Args:
        df_target: Hourly rows for the target date.
        df_hist: Hourly rows for all historical dates.
        feature_weights: Dict of {column: weight}. If provided, takes precedence
                         over feature_cols. E.g. {"lmp_total": 1.0, "lmp_congestion_price": 0.5}
        feature_cols: LMP columns to compare (used when feature_weights is None).
        n_neighbors: Number of like days to return.
        metric: Distance metric — mae, rmse, euclidean, cosine.
        date_col: Name of the date column.
        hour_col: Name of the hour column.

    Returns:
        DataFrame with columns: date, rank, distance, similarity
    """
    # Resolve features and weights
    if feature_weights is not None and len(feature_weights) > 0:
        feature_cols = list(feature_weights.keys())
        weights = feature_weights
    else:
        weights = {col: 1.0 for col in feature_cols}

    # Target hourly values as array (sorted by hour)
    target_sorted = df_target.sort_values(hour_col)
    n_hours = len(target_sorted)

    if n_hours == 0:
        raise ValueError("Target date has no hourly data")

    # Pre-compute target arrays per feature
    target_per_feat = {}
    for col in feature_cols:
        target_per_feat[col] = target_sorted[col].values

    # --- Pass 1: compute raw per-feature distances for every historical day ---
    raw_results = []  # list of {date_col: dt, feat1_dist: ..., feat2_dist: ..., ...}
    for dt, group in df_hist.groupby(date_col):
        hist_sorted = group.sort_values(hour_col)

        # Only compare days with the exact same number of hours
        if len(hist_sorted) != n_hours:
            continue

        row = {date_col: dt}
        for col in feature_cols:
            t_vals = target_per_feat[col]
            h_vals = hist_sorted[col].values
            diff = t_vals - h_vals
            row[col] = _compute_metric(diff, metric, t_vals, h_vals)

        raw_results.append(row)

    if not raw_results:
        raise ValueError("No historical days matched the target hours")

    raw_df = pd.DataFrame(raw_results)

    # --- Pass 2: z-score normalize per-feature distances, then weighted blend ---
    weight_sum = sum(weights[col] for col in feature_cols)

    # Compute mean/std for each feature's distances across the full historical pool
    feat_stats = {}
    for col in feature_cols:
        mean = raw_df[col].mean()
        std = raw_df[col].std()
        if std == 0 or np.isnan(std):
            std = 1.0  # avoid division by zero when all distances are identical
        feat_stats[col] = (mean, std)

    # Compute weighted normalized distance
    raw_df["distance"] = 0.0
    for col in feature_cols:
        mean, std = feat_stats[col]
        normalized = (raw_df[col] - mean) / std
        raw_df["distance"] += weights[col] * normalized
    raw_df["distance"] /= weight_sum

    # Select top-N
    results_df = raw_df.nsmallest(n_neighbors, "distance").reset_index(drop=True)
    results_df["rank"] = np.arange(1, len(results_df) + 1)

    # Similarity: normalise relative to the worst of the N neighbors
    max_dist = results_df["distance"].max()
    min_dist = results_df["distance"].min()
    dist_range = max_dist - min_dist
    if dist_range > 0:
        results_df["similarity"] = 1.0 - (results_df["distance"] - min_dist) / dist_range
    else:
        results_df["similarity"] = 1.0

    return results_df[[date_col, "rank", "distance", "similarity"]]
