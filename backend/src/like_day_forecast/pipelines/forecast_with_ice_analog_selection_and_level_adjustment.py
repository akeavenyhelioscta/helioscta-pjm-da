"""Like-day forecast with ICE analogs plus ICE level adjustment.

Scenario: ``ice_combined``
  Applies both ICE forward-price mechanisms:
  1) ICE settlement features in analog selection
  2) On-peak level scaling to ICE NxtDay DA settlement

Usage:
    python -m src.like_day_forecast.pipelines.forecast_with_ice_analog_selection_and_level_adjustment
"""
from __future__ import annotations

from src.like_day_forecast.configs import ScenarioConfig
from src.like_day_forecast.pipelines.forecast import run


def main(
    forecast_date: str | None = None,
    **kwargs,
) -> dict:
    """Run like-day forecast with ICE analog selection and level scaling."""
    config = ScenarioConfig(
        name="ice_combined",
        forecast_date=forecast_date,
        include_ice_forward=True,
        ice_level_adjustment=True,
        **kwargs,
    )
    return run(config=config)


if __name__ == "__main__":
    import src.like_day_forecast.settings  # noqa: F401

    main()
