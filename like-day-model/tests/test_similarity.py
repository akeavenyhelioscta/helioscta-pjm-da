"""Unit tests for similarity metrics and engine."""
import numpy as np
import pandas as pd
import pytest
from datetime import date

from pjm_like_day_forecast.similarity import metrics


class TestEuclideanDistance:
    def test_identical_vectors(self):
        a = np.array([1.0, 2.0, 3.0])
        assert metrics.euclidean_distance(a, a) == 0.0

    def test_known_distance(self):
        a = np.array([0.0, 0.0])
        b = np.array([3.0, 4.0])
        assert metrics.euclidean_distance(a, b) == pytest.approx(5.0)

    def test_weighted(self):
        a = np.array([0.0, 0.0])
        b = np.array([1.0, 1.0])
        weights = np.array([4.0, 0.0])
        assert metrics.euclidean_distance(a, b, weights) == pytest.approx(2.0)


class TestCosineDistance:
    def test_identical_direction(self):
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([2.0, 4.0, 6.0])
        assert metrics.cosine_distance(a, b) == pytest.approx(0.0, abs=1e-10)

    def test_orthogonal(self):
        a = np.array([1.0, 0.0])
        b = np.array([0.0, 1.0])
        assert metrics.cosine_distance(a, b) == pytest.approx(1.0)

    def test_zero_vector(self):
        a = np.array([0.0, 0.0])
        b = np.array([1.0, 1.0])
        assert metrics.cosine_distance(a, b) == 1.0


class TestPatternDistance:
    def test_identical_pattern_different_level(self):
        """Profiles with same shape but different levels should have distance ~0."""
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([11.0, 12.0, 13.0, 14.0, 15.0])  # shifted by +10
        assert metrics.pattern_distance(a, b) == pytest.approx(0.0, abs=1e-10)

    def test_identical_pattern_different_scale(self):
        """Profiles with same shape but different scales should have distance ~0."""
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([2.0, 4.0, 6.0, 8.0, 10.0])  # scaled by 2x
        assert metrics.pattern_distance(a, b) == pytest.approx(0.0, abs=1e-10)

    def test_different_pattern(self):
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([5.0, 4.0, 3.0, 2.0, 1.0])  # reversed
        assert metrics.pattern_distance(a, b) > 0

    def test_flat_profiles(self):
        a = np.array([5.0, 5.0, 5.0])
        b = np.array([5.0, 5.0, 5.0])
        assert metrics.pattern_distance(a, b) == 0.0


class TestCombinedDistance:
    def test_single_group(self):
        target = {"lmp": np.array([1.0, 2.0, 3.0])}
        candidate = {"lmp": np.array([1.0, 2.0, 3.0])}
        assert metrics.combined_distance(target, candidate) == 0.0

    def test_weighted_groups(self):
        target = {
            "lmp": np.array([0.0, 0.0]),
            "gas": np.array([0.0]),
        }
        candidate = {
            "lmp": np.array([3.0, 4.0]),  # euclidean = 5.0
            "gas": np.array([10.0]),       # euclidean = 10.0
        }
        weights = {"lmp": 1.0, "gas": 1.0}
        dist = metrics.combined_distance(target, candidate, group_weights=weights)
        expected = (1.0 * 5.0 + 1.0 * 10.0) / 2.0  # 7.5
        assert dist == pytest.approx(expected)

    def test_missing_group_ignored(self):
        target = {"lmp": np.array([1.0, 2.0])}
        candidate = {"lmp": np.array([1.0, 2.0]), "gas": np.array([5.0])}
        dist = metrics.combined_distance(target, candidate)
        assert dist == 0.0  # only 'lmp' compared


class TestAnalogWeights:
    def test_inverse_distance_sums_to_one(self):
        from pjm_like_day_forecast.similarity.engine import compute_analog_weights
        distances = np.array([0.1, 0.5, 1.0, 2.0])
        weights = compute_analog_weights(distances, method="inverse_distance")
        assert weights.sum() == pytest.approx(1.0)
        # Closest should have highest weight
        assert weights[0] > weights[1] > weights[2] > weights[3]

    def test_softmax_sums_to_one(self):
        from pjm_like_day_forecast.similarity.engine import compute_analog_weights
        distances = np.array([0.1, 0.5, 1.0, 2.0])
        weights = compute_analog_weights(distances, method="softmax")
        assert weights.sum() == pytest.approx(1.0)

    def test_rank_sums_to_one(self):
        from pjm_like_day_forecast.similarity.engine import compute_analog_weights
        distances = np.array([0.1, 0.5, 1.0, 2.0])
        weights = compute_analog_weights(distances, method="rank")
        assert weights.sum() == pytest.approx(1.0)

    def test_uniform(self):
        from pjm_like_day_forecast.similarity.engine import compute_analog_weights
        distances = np.array([0.1, 0.5, 1.0])
        weights = compute_analog_weights(distances, method="uniform")
        assert all(w == pytest.approx(1/3) for w in weights)
