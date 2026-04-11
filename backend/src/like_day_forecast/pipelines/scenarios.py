"""Scenario definitions for analog-pool and fundamental-delta overrides.

Each ScenarioSpec combines two modes:
  Mode 1 (pool_overrides): Change ScenarioConfig filtering parameters to alter
      which historical days are selected as analogs.
  Mode 2 (fundamental_overrides): Override target-day fundamental values to
      change the regression adjustment (e.g., "what if load is 5% lower?").

Both modes are composable within a single ScenarioSpec.

Usage:
    from src.like_day_forecast.pipelines.scenarios import NAMED_PRESETS, ScenarioSpec
    spec = NAMED_PRESETS["holiday"]
    custom = ScenarioSpec(name="My Scenario", fundamental_overrides={"load_mw": {"pct_change": -0.05}})
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ScenarioSpec:
    """A single scenario specification combining pool and fundamental overrides.

    Attributes:
        name: Human-readable label (appears in comparison table).
        description: One-line explanation for trader context.
        pool_overrides: Dict of ScenarioConfig field overrides (Mode 1).
            Keys must be valid ScenarioConfig field names.
        fundamental_overrides: Dict of fundamental value overrides (Mode 2).
            Keys are fundamental names matching FUNDAMENTAL_COLUMN_MAP.
            Values can be:
              - float: absolute MW or $/MWh value to use as today_value
              - dict with "pct_change": multiply current forecast by (1 + pct_change)
        sensitivity_overrides: Optional override of regression sensitivities.
    """

    name: str
    description: str = ""
    pool_overrides: dict = field(default_factory=dict)
    fundamental_overrides: dict = field(default_factory=dict)
    sensitivity_overrides: dict[str, dict[str, float]] | None = None


# ── Column mapping: override key → feature matrix column ──────────────
FUNDAMENTAL_COLUMN_MAP: dict[str, str] = {
    "load_mw": "tgt_load_daily_avg",
    "outage_total_mw": "tgt_outage_total_mw",
    "renewable_mw": "tgt_renewable_daily_avg",
    "nuclear_mw": "nuclear_daily_avg",
    "congestion_dollar": "congestion_onpeak_avg",
}


# ── Named presets ─────────────────────────────────────────────────────

NAMED_PRESETS: dict[str, ScenarioSpec] = {
    # ── Mode 1: Analog Pool Scenarios ─────────────────────────
    "holiday": ScenarioSpec(
        name="Holiday",
        description="Holiday/Sunday analog pool — no DOW matching, wider season window, no regime filter",
        pool_overrides={
            "same_dow_group": False,
            "season_window_days": 90,
            "apply_regime_filter": False,
            "exclude_holidays": False,
        },
    ),
    "high_outage_pool": ScenarioSpec(
        name="High Outage Pool",
        description="Strict outage-regime matching — only analogs with similar high outages",
        pool_overrides={
            "outage_tolerance_std": 0.5,
        },
    ),
    "wide_pool": ScenarioSpec(
        name="Wide Pool",
        description="Relaxed filtering — 180-day window, no regime filter",
        pool_overrides={
            "season_window_days": 180,
            "apply_regime_filter": False,
        },
    ),
    "weekday_only": ScenarioSpec(
        name="Weekday Only",
        description="Enforce weekday DOW group — excludes any weekend contamination",
        pool_overrides={
            "same_dow_group": True,
        },
    ),

    # ── Mode 2: Fundamental Delta Scenarios ───────────────────
    "good_friday_load": ScenarioSpec(
        name="Good Friday Load",
        description="Load forecast -5% — holiday demand suppression",
        fundamental_overrides={
            "load_mw": {"pct_change": -0.05},
        },
    ),
    "low_load": ScenarioSpec(
        name="Low Load",
        description="Load forecast -3% — mild demand underperformance",
        fundamental_overrides={
            "load_mw": {"pct_change": -0.03},
        },
    ),
    "high_load": ScenarioSpec(
        name="High Load",
        description="Load forecast +3% — demand overperformance",
        fundamental_overrides={
            "load_mw": {"pct_change": 0.03},
        },
    ),
    "low_wind": ScenarioSpec(
        name="Low Wind",
        description="Renewable output override to 1,500 MW — calm day",
        fundamental_overrides={
            "renewable_mw": 1500.0,
        },
    ),
    "high_wind": ScenarioSpec(
        name="High Wind",
        description="Renewable output override to 8,000 MW — strong front",
        fundamental_overrides={
            "renewable_mw": 8000.0,
        },
    ),
    "high_outage_stress": ScenarioSpec(
        name="High Outage Stress",
        description="Outages override to 60,000 MW — maintenance season peak",
        fundamental_overrides={
            "outage_total_mw": 60000.0,
        },
    ),
    "low_outage": ScenarioSpec(
        name="Low Outage",
        description="Outages override to 45,000 MW — below-normal outages",
        fundamental_overrides={
            "outage_total_mw": 45000.0,
        },
    ),

    # ── Combined Mode 1+2 ────────────────────────────────────
    "holiday_low_wind": ScenarioSpec(
        name="Holiday + Low Wind",
        description="Holiday pool with low renewables — worst case for long position",
        pool_overrides={
            "same_dow_group": False,
            "season_window_days": 90,
            "apply_regime_filter": False,
        },
        fundamental_overrides={
            "load_mw": {"pct_change": -0.05},
            "renewable_mw": 1500.0,
        },
    ),
    "bull": ScenarioSpec(
        name="Bull Case",
        description="Renewable -25%, outage +5%, load +3% — tighter conditions",
        fundamental_overrides={
            "renewable_mw": {"pct_change": -0.25},
            "outage_total_mw": {"pct_change": 0.05},
            "load_mw": {"pct_change": 0.03},
        },
    ),
    "bear": ScenarioSpec(
        name="Bear Case",
        description="Renewable +25%, outage -5%, load -3% — looser conditions",
        fundamental_overrides={
            "renewable_mw": {"pct_change": 0.25},
            "outage_total_mw": {"pct_change": -0.05},
            "load_mw": {"pct_change": -0.03},
        },
    ),
}
