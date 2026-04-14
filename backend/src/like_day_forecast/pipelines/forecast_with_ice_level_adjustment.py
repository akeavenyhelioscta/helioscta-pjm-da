"""Like-day forecast with ICE on-peak level adjustment.

Scenario: ``ice_level``
  Base like-day analog selection (no ICE features), then on-peak
  forecast output is scaled so on-peak hours match the ICE NxtDay DA
  settlement price.

  The hourly shape comes from analogs; the on-peak level comes from ICE.
  Off-peak hours are unchanged (no off-peak ICE product available).

Usage:
    python -m src.like_day_forecast.pipelines.forecast_with_ice_level_adjustment
"""
from __future__ import annotations

from src.like_day_forecast.configs import ScenarioConfig
from src.like_day_forecast.pipelines.forecast import run


def main(
    forecast_date: str | None = None,
    **kwargs,
) -> dict:
    """Run like-day forecast with ICE-based on-peak level scaling."""
    config = ScenarioConfig(
        name="ice_level",
        forecast_date=forecast_date,
        include_ice_forward=False,
        ice_level_adjustment=True,
        **kwargs,
    )
    return run(config=config)


if __name__ == "__main__":
    import src.like_day_forecast.settings  # noqa: F401

    main()
