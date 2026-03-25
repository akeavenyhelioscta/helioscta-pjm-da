"""Distance/similarity functions for comparing daily feature vectors.

Supports multiple metrics that capture different aspects of similarity:
- Euclidean: overall magnitude difference
- Cosine: directional similarity (shape)
- Pattern: shape-only (level-invariant)
- Combined: per-feature-group metrics with weights
"""
import numpy as np
import logging

logger = logging.getLogger(__name__)


def euclidean_distance(a: np.ndarray, b: np.ndarray, weights: np.ndarray | None = None) -> float:
    """Weighted Euclidean distance between two feature vectors.

    Args:
        a, b: Feature vectors of same length.
        weights: Per-element weights (default: uniform).

    Returns:
        Weighted Euclidean distance (>= 0).
    """
    diff = a - b
    if weights is not None:
        return float(np.sqrt(np.sum(weights * diff ** 2)))
    return float(np.sqrt(np.sum(diff ** 2)))


def cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance: 1 - cosine_similarity.

    Returns 0 for identical directions, 1 for orthogonal, 2 for opposite.
    """
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 1.0
    return float(1.0 - np.dot(a, b) / (norm_a * norm_b))


def mae_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Mean Absolute Error distance."""
    return float(np.mean(np.abs(a - b)))


def pattern_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Pattern-based distance: compares normalized shapes.

    Normalizes each vector to zero-mean, unit-variance before computing
    Euclidean distance. This separates shape from level — a $30 avg day
    and a $60 avg day can have identical patterns.
    """
    a_std = np.std(a)
    b_std = np.std(b)

    if a_std == 0 or b_std == 0:
        # Flat profiles — distance is 0 if both flat, else penalize
        if a_std == 0 and b_std == 0:
            return 0.0
        return 1.0

    a_norm = (a - np.mean(a)) / a_std
    b_norm = (b - np.mean(b)) / b_std

    return float(np.sqrt(np.mean((a_norm - b_norm) ** 2)))


def combined_distance(
    target_features: dict[str, np.ndarray],
    candidate_features: dict[str, np.ndarray],
    group_metrics: dict[str, str] | None = None,
    group_weights: dict[str, float] | None = None,
) -> float:
    """Compute combined distance across feature groups using different metrics.

    Each feature group can use a different distance metric and weight.

    Args:
        target_features: Dict of {group_name: feature_array} for target day.
        candidate_features: Dict of {group_name: feature_array} for candidate day.
        group_metrics: Dict of {group_name: metric_name}. Default: euclidean for all.
        group_weights: Dict of {group_name: weight}. Default: uniform.

    Returns:
        Weighted combined distance.
    """
    if group_metrics is None:
        group_metrics = {}
    if group_weights is None:
        group_weights = {}

    metric_funcs = {
        "euclidean": euclidean_distance,
        "cosine": cosine_distance,
        "mae": mae_distance,
        "pattern": pattern_distance,
    }

    total_distance = 0.0
    total_weight = 0.0

    for group_name in target_features:
        if group_name not in candidate_features:
            continue

        t = target_features[group_name]
        c = candidate_features[group_name]

        metric_name = group_metrics.get(group_name, "euclidean")
        weight = group_weights.get(group_name, 1.0)
        metric_fn = metric_funcs.get(metric_name, euclidean_distance)

        dist = metric_fn(t, c)
        total_distance += weight * dist
        total_weight += weight

    if total_weight == 0:
        return 0.0

    return total_distance / total_weight
