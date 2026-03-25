"""Main similarity engine: orchestrates filter -> compute -> rank.

Finds the N most similar historical dates to a target date based on
multi-feature weighted distance, with pre-filtering and configurable metrics.
"""
import pandas as pd
import numpy as np
import logging
from datetime import date

from src.like_day_forecast import configs
from src.like_day_forecast.similarity import metrics, filtering

logger = logging.getLogger(__name__)

# Feature groups define which columns map to which similarity group
FEATURE_GROUPS = {
    "lmp_profile": {
        "columns_pattern": "lmp_profile_h",
        "default_metric": "euclidean",
    },
    "lmp_level": {
        "columns": ["lmp_daily_flat", "lmp_onpeak_avg", "lmp_offpeak_avg",
                     "lmp_daily_max", "lmp_daily_min"],
        "default_metric": "euclidean",
    },
    "lmp_volatility": {
        "columns": ["lmp_intraday_std", "lmp_intraday_range", "lmp_peak_ratio",
                     "lmp_7d_rolling_std"],
        "default_metric": "euclidean",
    },
    "load_level": {
        "columns": ["load_daily_avg", "load_daily_peak", "load_daily_valley",
                     "load_7d_rolling_mean"],
        "default_metric": "euclidean",
    },
    "load_shape": {
        "columns": ["load_peak_ratio", "load_ramp_max"],
        "default_metric": "euclidean",
    },
    "gas_price": {
        "columns": ["gas_m3_price", "gas_hh_price", "gas_m3_hh_spread"],
        "default_metric": "euclidean",
    },
    "gas_momentum": {
        "columns": ["gas_m3_7d_change", "gas_m3_30d_mean"],
        "default_metric": "euclidean",
    },
    "calendar_dow": {
        "columns": ["dow_sin", "dow_cos"],
        "default_metric": "euclidean",
    },
    "calendar_season": {
        "columns": ["day_of_year_sin", "day_of_year_cos", "month_sin", "month_cos"],
        "default_metric": "euclidean",
    },
    "weather_level": {
        "columns": ["temp_daily_avg", "temp_daily_max", "temp_daily_min",
                     "temp_intraday_range", "feels_like_daily_avg",
                     "temp_7d_rolling_mean", "temp_daily_change"],
        "default_metric": "euclidean",
    },
    "weather_hdd_cdd": {
        "columns": ["hdd", "cdd"],
        "default_metric": "euclidean",
    },
    "weather_wind": {
        "columns": ["wind_speed_daily_avg"],
        "default_metric": "euclidean",
    },
    "composite_heat_rate": {
        "columns": ["implied_heat_rate", "lmp_per_load"],
        "default_metric": "euclidean",
    },
    "renewable_level": {
        "columns": ["solar_daily_avg", "wind_daily_avg", "renewable_daily_avg",
                     "renewable_daily_max", "renewable_7d_rolling_mean"],
        "default_metric": "euclidean",
    },
    "renewable_shape": {
        "columns": ["solar_daily_max", "wind_daily_max",
                     "solar_peak_concentration", "wind_intraday_std"],
        "default_metric": "euclidean",
    },
    "outage_level": {
        "columns": ["outage_total_mw", "outage_total_7d_mean",
                     "outage_total_daily_change"],
        "default_metric": "euclidean",
    },
    "outage_composition": {
        "columns": ["outage_planned_mw", "outage_forced_mw",
                     "outage_forced_share"],
        "default_metric": "euclidean",
    },
    "target_renewable_level": {
        "columns": ["tgt_solar_daily_avg", "tgt_wind_daily_avg",
                     "tgt_renewable_daily_avg", "tgt_renewable_change_vs_ref"],
        "default_metric": "euclidean",
    },
    "target_outage_level": {
        "columns": ["tgt_outage_total_mw", "tgt_outage_forced_mw",
                     "tgt_outage_change_vs_ref"],
        "default_metric": "euclidean",
    },
    "target_weather_level": {
        "columns": ["tgt_temp_daily_avg", "tgt_temp_daily_max", "tgt_temp_daily_min",
                     "tgt_feels_like_daily_avg", "tgt_temp_change_vs_ref"],
        "default_metric": "euclidean",
    },
    "target_weather_hdd_cdd": {
        "columns": ["tgt_hdd", "tgt_cdd"],
        "default_metric": "euclidean",
    },
}


def _resolve_group_columns(df: pd.DataFrame, group_def: dict) -> list[str]:
    """Resolve which columns in df belong to a feature group."""
    if "columns_pattern" in group_def:
        pattern = group_def["columns_pattern"]
        return [c for c in df.columns if c.startswith(pattern)]
    elif "columns" in group_def:
        return [c for c in group_def["columns"] if c in df.columns]
    return []


def _extract_feature_vector(row: pd.Series, columns: list[str]) -> np.ndarray:
    """Extract a feature vector from a DataFrame row, replacing NaN with 0."""
    values = row[columns].values.astype(float)
    return np.nan_to_num(values, nan=0.0)


def _normalize_features(
    df: pd.DataFrame,
    feature_columns: dict[str, list[str]],
) -> tuple[pd.DataFrame, dict[str, tuple[np.ndarray, np.ndarray]]]:
    """Z-score normalize features per group across the pool.

    Returns:
        Normalized DataFrame and dict of {group: (means, stds)} for each group.
    """
    df_norm = df.copy()
    stats = {}

    for group_name, cols in feature_columns.items():
        if not cols:
            continue
        vals = df_norm[cols].values.astype(float)
        means = np.nanmean(vals, axis=0)
        stds = np.nanstd(vals, axis=0)
        stds[stds == 0] = 1.0  # avoid division by zero
        df_norm[cols] = (vals - means) / stds
        stats[group_name] = (means, stds)

    return df_norm, stats


def compute_analog_weights(
    distances: np.ndarray,
    method: str = "inverse_distance",
    temperature: float = 1.0,
) -> np.ndarray:
    """Convert distances to analog weights for probabilistic estimation.

    Args:
        distances: Array of distances (lower = more similar).
        method: "inverse_distance", "softmax", "rank", or "uniform".
        temperature: Controls concentration for softmax (lower = more concentrated).

    Returns:
        Array of weights summing to 1.0.
    """
    n = len(distances)
    if n == 0:
        return np.array([])

    if method == "inverse_distance":
        # w_i = 1 / (d_i + epsilon)^2
        epsilon = 1e-8
        weights = 1.0 / (distances + epsilon) ** 2
    elif method == "softmax":
        # w_i = exp(-d_i / T) / sum(exp(-d_j / T))
        scaled = -distances / max(temperature, 1e-8)
        scaled -= scaled.max()  # numerical stability
        weights = np.exp(scaled)
    elif method == "rank":
        # w_i = (N - rank_i + 1)
        ranks = np.argsort(np.argsort(distances))  # 0-indexed ranks
        weights = (n - ranks).astype(float)
    else:  # uniform
        weights = np.ones(n)

    # Normalize to sum to 1
    total = weights.sum()
    if total > 0:
        weights /= total

    return weights


def find_analogs(
    target_date: date,
    df_features: pd.DataFrame,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    feature_weights: dict[str, float] | None = None,
    apply_calendar_filter: bool = True,
    apply_regime_filter: bool = True,
    season_window_days: int = configs.FILTER_SEASON_WINDOW_DAYS,
    same_dow_group: bool = configs.FILTER_SAME_DOW_GROUP,
    weight_method: str = "inverse_distance",
    adaptive_filter_enabled: bool = configs.ADAPTIVE_FILTER_ENABLED,
    adaptive_extreme_threshold_std: float = configs.ADAPTIVE_EXTREME_THRESHOLD_STD,
    adaptive_season_window_days: int = configs.ADAPTIVE_SEASON_WINDOW_DAYS,
    adaptive_same_dow_group: bool = configs.ADAPTIVE_SAME_DOW_GROUP,
    adaptive_lmp_tolerance_std: float = configs.ADAPTIVE_LMP_TOLERANCE_STD,
    adaptive_gas_tolerance_std: float = configs.ADAPTIVE_GAS_TOLERANCE_STD,
    adaptive_n_analogs: int = configs.ADAPTIVE_N_ANALOGS,
    adaptive_weight_method: str = configs.ADAPTIVE_WEIGHT_METHOD,
    adaptive_softmax_temperature: float = configs.ADAPTIVE_SOFTMAX_TEMPERATURE,
) -> pd.DataFrame:
    """Find the N most similar historical dates to the target.

    Pipeline:
    1. Extract target date feature vector
    2. Pre-filter historical pool (calendar + regime)
    3. Z-score normalize features across filtered pool
    4. Compute per-group distances with configurable metrics
    5. Weighted blend across groups
    6. Rank by distance, return top-N with weights

    Args:
        target_date: The date to find analogs for.
        df_features: Full daily feature matrix (from builder.build_daily_features).
        n_analogs: Number of analogs to return.
        feature_weights: Dict of {group_name: weight}. Defaults to configs.
        apply_calendar_filter: Whether to apply DOW/season filtering.
        apply_regime_filter: Whether to apply LMP/gas regime filtering.
        season_window_days: Season proximity window in days.
        same_dow_group: Whether to enforce same DOW group.
        weight_method: Method for computing analog weights.

    Returns:
        DataFrame with columns: date, rank, distance, similarity, weight
    """
    if feature_weights is None:
        feature_weights = configs.FEATURE_GROUP_WEIGHTS.copy()

    # Ensure target date exists in features
    target_mask = df_features["date"] == target_date
    if not target_mask.any():
        raise ValueError(f"Target date {target_date} not found in feature matrix")

    # --- 0. Adaptive regime detection ---
    # When the reference date is in an extreme LMP regime, widen the season
    # window and relax DOW matching so that extreme-regime analogs from other
    # seasons (e.g., Jan cold snap for a March spike) can enter the pool.
    is_extreme = False
    if adaptive_filter_enabled:
        is_extreme = filtering.detect_extreme_regime(
            df_features, target_date,
            threshold_std=adaptive_extreme_threshold_std,
        )
        if is_extreme:
            season_window_days = adaptive_season_window_days
            same_dow_group = adaptive_same_dow_group
            n_analogs = adaptive_n_analogs
            weight_method = adaptive_weight_method
            logger.info(f"Adaptive filter: season_window={season_window_days}d, "
                        f"same_dow_group={same_dow_group}, n_analogs={n_analogs}, "
                        f"weight_method={weight_method}")

    # --- 1. Pre-filter ---
    pool = df_features.copy()

    if apply_calendar_filter:
        pool = filtering.calendar_filter(
            pool, target_date,
            same_dow_group=same_dow_group,
            season_window_days=season_window_days,
        )

    if apply_regime_filter:
        lmp_tol = adaptive_lmp_tolerance_std if is_extreme else 1.5
        gas_tol = adaptive_gas_tolerance_std if is_extreme else 1.5
        pool = filtering.regime_filter(
            pool, target_date, df_full=df_features,
            lmp_tolerance_std=lmp_tol, gas_tolerance_std=gas_tol,
        )

    # Ensure minimum pool size
    pool = filtering.ensure_minimum_pool(pool, df_features, target_date)

    if len(pool) == 0:
        raise ValueError("No candidates remaining after filtering")

    # --- 2. Resolve feature groups ---
    target_row = df_features[target_mask].iloc[0]

    group_columns = {}
    active_groups = {}
    for group_name, group_def in FEATURE_GROUPS.items():
        if group_name not in feature_weights:
            continue
        cols = _resolve_group_columns(df_features, group_def)
        if not cols:
            continue
        # NaN-group exclusion: skip groups where the target row has all NaN.
        # This handles production cases where D+1 observed weather isn't
        # available yet — those groups silently drop out.
        target_vals = target_row[cols].values.astype(float)
        if np.all(np.isnan(target_vals)):
            logger.info(f"Skipping group '{group_name}' — target has all NaN")
            continue
        group_columns[group_name] = cols
        active_groups[group_name] = group_def

    if not group_columns:
        raise ValueError("No feature groups matched available columns")

    # --- 3. Normalize features across the pool + target ---
    all_dates = pd.concat([df_features[target_mask], pool]).drop_duplicates(subset=["date"])
    df_norm, stats = _normalize_features(all_dates, group_columns)

    # Extract normalized target row
    target_norm = df_norm[df_norm["date"] == target_date].iloc[0]

    # Extract normalized pool
    pool_norm = df_norm[df_norm["date"] != target_date]

    # --- 4. Compute distances ---
    distances = []
    pool_dates = []

    for _, row in pool_norm.iterrows():
        target_groups = {}
        candidate_groups = {}

        for group_name, cols in group_columns.items():
            target_groups[group_name] = _extract_feature_vector(target_norm, cols)
            candidate_groups[group_name] = _extract_feature_vector(row, cols)

        group_metrics = {g: active_groups[g].get("default_metric", "euclidean")
                        for g in active_groups}

        dist = metrics.combined_distance(
            target_groups, candidate_groups,
            group_metrics=group_metrics,
            group_weights=feature_weights,
        )

        distances.append(dist)
        pool_dates.append(row["date"])

    distances = np.array(distances)
    pool_dates = np.array(pool_dates)

    # --- 5. Rank and select top-N ---
    n_select = min(n_analogs, len(distances))
    top_indices = np.argsort(distances)[:n_select]

    result_dates = pool_dates[top_indices]
    result_distances = distances[top_indices]

    # Compute analog weights
    softmax_temp = adaptive_softmax_temperature if is_extreme else 1.0
    result_weights = compute_analog_weights(
        result_distances, method=weight_method, temperature=softmax_temp,
    )

    # Similarity score: relative to worst of the N analogs
    max_dist = result_distances.max()
    min_dist = result_distances.min()
    dist_range = max_dist - min_dist
    if dist_range > 0:
        similarities = 1.0 - (result_distances - min_dist) / dist_range
    else:
        similarities = np.ones(n_select)

    results_df = pd.DataFrame({
        "date": result_dates,
        "rank": np.arange(1, n_select + 1),
        "distance": result_distances,
        "similarity": similarities,
        "weight": result_weights,
    })

    logger.info(f"Found {n_select} analogs for {target_date} "
                f"(pool: {len(pool):,}, groups: {len(group_columns)})")

    return results_df
