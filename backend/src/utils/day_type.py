"""Utilities for classifying forecast targets by day type."""
from __future__ import annotations

from datetime import date


DAY_TYPE_WEEKDAY = "weekday"
DAY_TYPE_SATURDAY = "saturday"
DAY_TYPE_SUNDAY = "sunday"


def resolve_day_type(target_date: date) -> str:
    """Return day-type bucket for a target delivery date."""
    wd = target_date.weekday()
    if wd == 5:
        return DAY_TYPE_SATURDAY
    if wd == 6:
        return DAY_TYPE_SUNDAY
    return DAY_TYPE_WEEKDAY

