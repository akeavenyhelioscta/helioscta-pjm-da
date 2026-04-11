"""Scenario forecast pipeline — run multiple what-if scenarios and compare.

Supports two composable modes:
  Mode 1 (Analog Pool): Override ScenarioConfig filtering parameters to change
      which historical days are selected as analogs.
  Mode 2 (Fundamental Delta): Override target-day fundamental values to change
      the regression adjustment (e.g., "what if load is 5% lower?").

Usage:
    from src.like_day_forecast.pipelines.scenario_forecast import run

    # Named presets
    result = run(scenarios=["base", "holiday", "high_outage_stress"])

    # Custom inline scenario
    result = run(scenarios=[
        "base",
        {"name": "Load -5%", "fundamental_overrides": {"load_mw": {"pct_change": -0.05}}},
    ])

    # ScenarioSpec objects
    from src.like_day_forecast.pipelines.scenarios import ScenarioSpec
    result = run(scenarios=[ScenarioSpec(name="Custom", fundamental_overrides={"renewable_mw": 1500.0})])
"""
import logging
from dataclasses import replace

import numpy as np
import pandas as pd

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_base_forecast
from src.like_day_forecast.pipelines.regression_adjusted_forecast import (
    build_features,
    compute_adjustment,
)
from src.like_day_forecast.pipelines.scenarios import (
    NAMED_PRESETS,
    ScenarioSpec,
)

logger = logging.getLogger(__name__)


# ── Public API ────────────────────────────────────────────────────────


def run(
    scenarios: list[str | dict | ScenarioSpec] | None = None,
    forecast_date: str | None = None,
    base_config: configs.ScenarioConfig | None = None,
    **kwargs,
) -> dict:
    """Run multiple scenarios and produce a comparison table.

    Args:
        scenarios: List of scenario specifications. Each element can be:
            - str: name of a preset from NAMED_PRESETS (e.g., "holiday")
            - dict: inline spec with keys matching ScenarioSpec fields
            - ScenarioSpec: fully constructed object
            If None, runs ["holiday", "high_outage_stress", "low_wind"].
        forecast_date: YYYY-MM-DD, defaults to tomorrow.
        base_config: Base ScenarioConfig to start from.
        **kwargs: Passed through to regression pipeline (cache settings, etc.).

    Returns:
        Dict with:
            comparison_table: pd.DataFrame — scenarios x period summary
            per_scenario: dict[str, dict] — full regression result per scenario
            scenarios_run: list[str] — scenario names in order
            forecast_date: str
    """
    if base_config is None:
        base_config = configs.ScenarioConfig(forecast_date=forecast_date)

    resolved = _resolve_scenarios(scenarios)

    # Always prepend "Base" if not present
    if not any(s.name.lower() == "base" for s in resolved):
        resolved.insert(0, ScenarioSpec(name="Base", description="No overrides"))

    # ── Compute base forecast and feature matrix ONCE ────────────
    logger.info("Building shared base forecast and feature matrix …")
    base_forecast = run_base_forecast(
        forecast_date=forecast_date, config=base_config, **kwargs,
    )
    df_features = build_features(
        config=base_config, forecast_date=forecast_date, **kwargs,
    )

    per_scenario: dict[str, dict] = {}
    comparison_rows: list[dict] = []

    for spec in resolved:
        logger.info(f"Running scenario: {spec.name} — {spec.description}")

        has_pool_overrides = bool(spec.pool_overrides)

        try:
            if has_pool_overrides:
                # Mode 1: different analog pool — must re-run base forecast
                scenario_config = _apply_pool_overrides(
                    base_config, spec, forecast_date,
                )
                scenario_base = run_base_forecast(
                    forecast_date=forecast_date,
                    config=scenario_config,
                    **kwargs,
                )
            else:
                # Reuse cached base forecast (no pool change)
                scenario_base = base_forecast

            result = compute_adjustment(
                base=scenario_base,
                df_features=df_features,
                sensitivities=spec.sensitivity_overrides,
                fundamental_overrides=spec.fundamental_overrides or None,
            )
        except Exception:
            logger.exception(f"Scenario '{spec.name}' failed")
            result = {"error": f"Scenario '{spec.name}' failed"}

        per_scenario[spec.name] = result
        comparison_rows.append(_extract_comparison_row(spec, result))

    # Build comparison DataFrame
    comparison_table = pd.DataFrame(comparison_rows)

    # Add delta-vs-base columns
    base_rows = comparison_table[comparison_table["Scenario"] == "Base"]
    if len(base_rows) > 0:
        base_row = base_rows.iloc[0]
        for col, label in [("OnPeak", "OnPk vs Base"), ("OffPeak", "OffPk vs Base")]:
            if pd.notna(base_row.get(col)):
                comparison_table[label] = comparison_table[col] - base_row[col]

    actual_date = forecast_date or base_forecast.get("forecast_date") or str(base_config.forecast_date)

    return {
        "comparison_table": comparison_table,
        "per_scenario": per_scenario,
        "scenarios_run": [s.name for s in resolved],
        "forecast_date": actual_date,
    }


# ── Helpers ───────────────────────────────────────────────────────────


def _resolve_scenarios(
    scenarios: list[str | dict | ScenarioSpec] | None,
) -> list[ScenarioSpec]:
    """Convert mixed input list into ScenarioSpec objects."""
    if scenarios is None:
        scenarios = ["holiday", "high_outage_stress", "low_wind"]

    resolved = []
    for s in scenarios:
        if isinstance(s, ScenarioSpec):
            resolved.append(s)
        elif isinstance(s, str):
            if s.lower() == "base":
                resolved.append(ScenarioSpec(name="Base", description="No overrides"))
            elif s in NAMED_PRESETS:
                resolved.append(NAMED_PRESETS[s])
            else:
                available = ", ".join(sorted(NAMED_PRESETS.keys()))
                raise ValueError(
                    f"Unknown scenario preset: '{s}'. Available: {available}"
                )
        elif isinstance(s, dict):
            resolved.append(ScenarioSpec(**s))
        else:
            raise TypeError(
                f"Scenario must be str, dict, or ScenarioSpec, got {type(s)}"
            )
    return resolved


def _apply_pool_overrides(
    base: configs.ScenarioConfig,
    spec: ScenarioSpec,
    forecast_date: str | None,
) -> configs.ScenarioConfig:
    """Create a new ScenarioConfig with pool_overrides applied."""
    overrides = {}
    if forecast_date:
        overrides["forecast_date"] = forecast_date
    overrides.update(spec.pool_overrides)

    if not overrides:
        return base

    return replace(base, **overrides)


def _extract_comparison_row(spec: ScenarioSpec, result: dict) -> dict:
    """Extract a single comparison row from a scenario result."""
    row: dict = {
        "Scenario": spec.name,
        "Description": spec.description,
    }

    if "error" in result:
        row.update({
            "Model OnPk": np.nan, "Model OffPk": np.nan,
            "OnPeak": np.nan, "OffPeak": np.nan, "Flat": np.nan,
            "Adj OnPk": np.nan, "Adj OffPk": np.nan, "N Analogs": None,
        })
        return row

    adj = result.get("adjustment", {})
    row["Model OnPk"] = adj.get("base_onpeak")
    row["Model OffPk"] = adj.get("base_offpeak")
    row["OnPeak"] = adj.get("adj_onpeak", adj.get("base_onpeak"))
    row["OffPeak"] = adj.get("adj_offpeak", adj.get("base_offpeak"))

    # Flat from the output table
    output_table = result.get("output_table")
    if output_table is not None:
        for type_label in ("Regression Adj", "Forecast"):
            match = output_table[output_table["Type"] == type_label]
            if len(match) > 0:
                flat_val = match.iloc[0].get("Flat")
                if pd.notna(flat_val):
                    row["Flat"] = float(flat_val)
                    break
    if "Flat" not in row:
        row["Flat"] = np.nan

    row["Adj OnPk"] = adj.get("total_onpeak", 0.0)
    row["Adj OffPk"] = adj.get("total_offpeak", 0.0)
    row["N Analogs"] = result.get("n_analogs_used")

    return row


def format_comparison_markdown(result: dict) -> str:
    """Format scenario comparison as markdown for API/MCP output."""
    lines = [
        f"# Scenario Forecast Comparison — {result.get('forecast_date', '?')}\n",
    ]

    table = result["comparison_table"]

    # Extract P25/P75 from base scenario quantile bands for context
    base_result = result["per_scenario"].get("Base", {})
    base_quantiles = base_result.get("base_quantiles_table")
    p25_onpk = p75_onpk = p25_offpk = p75_offpk = None
    if base_quantiles is not None and len(base_quantiles) > 0:
        band_col = "Band" if "Band" in base_quantiles.columns else "Type"
        p25_row = base_quantiles[base_quantiles[band_col] == "P25"]
        p75_row = base_quantiles[base_quantiles[band_col] == "P75"]
        if len(p25_row) > 0:
            p25_onpk = p25_row.iloc[0].get("OnPeak")
            p25_offpk = p25_row.iloc[0].get("OffPeak")
        if len(p75_row) > 0:
            p75_onpk = p75_row.iloc[0].get("OnPeak")
            p75_offpk = p75_row.iloc[0].get("OffPeak")

    # Summary table
    lines.append("## Comparison Table\n")
    display_cols = [
        "Scenario", "Model OnPk", "Model OffPk",
        "OnPeak", "OffPeak", "Flat",
        "Adj OnPk", "Adj OffPk",
    ]
    for extra in ("OnPk vs Base", "OffPk vs Base"):
        if extra in table.columns:
            display_cols.append(extra)

    header = "| " + " | ".join(display_cols) + " |"
    sep = "|" + "|".join(["--------"] * len(display_cols)) + "|"
    lines.append(header)
    lines.append(sep)

    for _, row in table.iterrows():
        cells = []
        for col in display_cols:
            val = row.get(col)
            if col == "Scenario":
                cells.append(str(val))
            elif col in ("Adj OnPk", "Adj OffPk", "OnPk vs Base", "OffPk vs Base"):
                cells.append(f"{val:+.2f}" if pd.notna(val) else "-")
            else:
                cells.append(f"${val:.2f}" if pd.notna(val) else "-")
        lines.append("| " + " | ".join(cells) + " |")

    # Add P25/P75 context
    if p25_onpk is not None and p75_onpk is not None:
        lines.append("")
        lines.append(
            f"*Like-day quantile bands — "
            f"On-Peak P25: ${p25_onpk:.2f}, P75: ${p75_onpk:.2f} | "
            f"Off-Peak P25: ${p25_offpk:.2f}, P75: ${p75_offpk:.2f}*"
        )

    lines.append("")

    # Per-scenario fundamental deltas
    for name, res in result["per_scenario"].items():
        deltas = res.get("deltas", [])
        overrides = res.get("fundamental_overrides_applied", {})
        if not deltas:
            continue

        lines.append(f"## {name} — Fundamental Deltas\n")

        if overrides:
            lines.append("**Overrides applied:**")
            for k, v in overrides.items():
                lines.append(
                    f"- {k}: {v['original']:,.0f} → {v['override']:,.0f}"
                )
            lines.append("")

        lines.append(
            "| Factor | Today | Analog Avg | Delta | Adj OnPk | Adj OffPk |"
        )
        lines.append("|--------|-------|-----------|-------|----------|----------|")
        for d in deltas:
            if d.unit == "MW":
                lines.append(
                    f"| {d.label} | {d.today_value:,.0f} MW | "
                    f"{d.analog_avg:,.0f} MW | {d.delta:+,.0f} | "
                    f"{d.adj_onpeak:+.2f} | {d.adj_offpeak:+.2f} |"
                )
            else:
                lines.append(
                    f"| {d.label} | ${d.today_value:.2f} | "
                    f"${d.analog_avg:.2f} | {d.delta:+.2f} | "
                    f"{d.adj_onpeak:+.2f} | {d.adj_offpeak:+.2f} |"
                )

        adj = res.get("adjustment", {})
        lines.append(
            f"| **Total** | | | | "
            f"**{adj.get('total_onpeak', 0):+.2f}** | "
            f"**{adj.get('total_offpeak', 0):+.2f}** |"
        )
        lines.append("")

    return "\n".join(lines)


def main():
    """Entry point — run default scenarios."""
    import src.like_day_forecast.settings

    result = run()
    print(format_comparison_markdown(result))
    print(f"\nScenarios run: {result['scenarios_run']}")


if __name__ == "__main__":
    main()
