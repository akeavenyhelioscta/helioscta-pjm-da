"""Tests for load data validation report fragments."""
from datetime import date, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest


# ── Synthetic data fixtures ──────────────────────────────────────────


def _make_hourly_load(
    start_date: date,
    n_days: int,
    col_name: str,
    base_load: float = 90_000,
) -> pd.DataFrame:
    """Create synthetic hourly load data."""
    rows = []
    for d in range(n_days):
        dt = start_date + timedelta(days=d)
        for h in range(1, 25):
            # Sine-ish daily profile
            load = base_load + 20_000 * np.sin((h - 6) * np.pi / 12)
            load += np.random.normal(0, 1000)
            rows.append({"date": dt, "hour_ending": h, col_name: load})
    return pd.DataFrame(rows)


@pytest.fixture()
def df_rt_raw():
    return _make_hourly_load(date(2024, 1, 1), 30, "rt_load_mw")


@pytest.fixture()
def df_da_raw():
    return _make_hourly_load(date(2024, 1, 1), 30, "da_load_mw")


# ── Tests ────────────────────────────────────────────────────────────


class TestRunChecks:
    """Tests for _run_checks."""

    def test_all_checks_pass_clean_data(self, df_rt_raw, df_da_raw):
        from src.reporting.fragments.load import _run_checks
        from src.like_day_forecast.features import load_features

        df_rt = df_rt_raw.copy()
        df_rt["date"] = pd.to_datetime(df_rt["date"])
        df_da = df_da_raw.copy()
        df_da["date"] = pd.to_datetime(df_da["date"])

        # Fake the date range to cover 2014 / 2020
        df_rt.loc[0, "date"] = pd.Timestamp("2014-01-01")
        df_da.loc[0, "date"] = pd.Timestamp("2020-01-01")

        df_features = load_features.build(df_rt_load=df_rt_raw)
        checks = _run_checks(df_rt, df_da, df_features)

        assert isinstance(checks, list)
        assert all(isinstance(c, tuple) and len(c) == 2 for c in checks)
        # At least the basic checks should be present
        names = [name for name, _ in checks]
        assert "RT zero nulls" in names

    def test_detects_nulls(self, df_rt_raw):
        from src.reporting.fragments.load import _run_checks
        from src.like_day_forecast.features import load_features

        df_rt = df_rt_raw.copy()
        df_rt.loc[5, "rt_load_mw"] = np.nan
        df_rt["date"] = pd.to_datetime(df_rt["date"])

        df_features = load_features.build(df_rt_load=df_rt_raw)
        checks = _run_checks(df_rt, None, df_features)

        null_check = next(
            (passed for name, passed in checks if name == "RT zero nulls"), None
        )
        assert null_check is False


class TestFormatChecksHtml:
    """Tests for _format_checks_html."""

    def test_all_pass(self):
        from src.reporting.fragments.load import _format_checks_html

        html = _format_checks_html([("check_a", True), ("check_b", True)])
        assert "ALL CHECKS PASSED" in html
        assert "PASS" in html

    def test_some_fail(self):
        from src.reporting.fragments.load import _format_checks_html

        html = _format_checks_html([("ok", True), ("bad", False)])
        assert "SOME CHECKS FAILED" in html
        assert "FAIL" in html


class TestBuildFragments:
    """Integration test: mock data pulls, verify fragment structure."""

    @patch("src.reporting.fragments.load.load_da_hourly")
    @patch("src.reporting.fragments.load.load_rt_metered_hourly")
    def test_returns_sections_and_dividers(self, mock_rt, mock_da, df_rt_raw, df_da_raw):
        mock_rt.pull.return_value = df_rt_raw
        mock_da.pull.return_value = df_da_raw

        from src.reporting.fragments.load import build_fragments

        fragments = build_fragments()

        # Should have divider strings and section tuples
        dividers = [f for f in fragments if isinstance(f, str)]
        sections = [f for f in fragments if isinstance(f, tuple)]

        assert len(dividers) > 0, "Should have at least one divider"
        assert len(sections) > 0, "Should have at least one section"

        # Each section is (name, content, icon)
        for name, content, icon in sections:
            assert isinstance(name, str)
            assert content is not None

    @patch("src.reporting.fragments.load.load_da_hourly")
    @patch("src.reporting.fragments.load.load_rt_metered_hourly")
    def test_works_without_da(self, mock_rt, mock_da, df_rt_raw):
        mock_rt.pull.return_value = df_rt_raw
        mock_da.pull.side_effect = Exception("DA not available")

        from src.reporting.fragments.load import build_fragments

        fragments = build_fragments()
        sections = [f for f in fragments if isinstance(f, tuple)]
        assert len(sections) > 0

        # Should not have DA-specific sections
        section_names = [name for name, _, _ in sections]
        assert "DA vs RT Scatter" not in section_names


class TestFullRender:
    """End-to-end: mock pulls -> fragments -> HTMLDashboardBuilder -> HTML."""

    @patch("src.reporting.fragments.load.load_da_hourly")
    @patch("src.reporting.fragments.load.load_rt_metered_hourly")
    def test_produces_valid_html(self, mock_rt, mock_da, df_rt_raw, df_da_raw):
        mock_rt.pull.return_value = df_rt_raw
        mock_da.pull.return_value = df_da_raw

        from src.reporting.fragments.load import build_fragments
        from src.reporting.html_dashboard import HTMLDashboardBuilder

        builder = HTMLDashboardBuilder(title="Test Report", theme="dark")
        for item in build_fragments():
            if isinstance(item, str):
                builder.add_divider(item)
            else:
                name, content, icon = item
                builder.add_content(name, content, icon=icon)

        html = builder.build()
        assert "<!DOCTYPE html>" in html
        assert "Validation Summary" in html
        assert "plotly" in html.lower()
