"""Like-day forecast with ICE-informed analog selection.

Scenario: ``ice_analogs``
  ICE NxtDay DA settlement data is added to the daily feature matrix
  as a new feature group (``ice_forward_level``). The similarity engine
  prefers historical days where ICE was pricing similarly to today.

  The output is still a weighted average of historical DA LMP profiles.
  This does NOT scale the forecast level to match ICE; it only improves
  analog selection.

Usage:
    python -m src.like_day_forecast.pipelines.forecast_with_ice_analog_selection
"""
from __future__ import annotations

from src.like_day_forecast.configs import ScenarioConfig
from src.like_day_forecast.pipelines.forecast import run


def main(
    forecast_date: str | None = None,
    **kwargs,
) -> dict:
    """Run like-day forecast with ICE features in analog selection."""
    config = ScenarioConfig(
        name="ice_analogs",
        forecast_date=forecast_date,
        include_ice_forward=True,
        ice_level_adjustment=False,
        **kwargs,
    )
    return run(config=config)


if __name__ == "__main__":
    import src.like_day_forecast.settings  # noqa: F401

    main()
