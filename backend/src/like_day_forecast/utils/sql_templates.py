"""Utilities for rendering SQL templates with optional overrides.

This renderer intentionally leaves missing template keys as empty strings so
SQL files can define defaults via `COALESCE(NULLIF('{key}', ''), <default>)`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


class _BlankDefaultDict(dict):
    """Return empty string for missing format keys."""

    def __missing__(self, key: str) -> str:
        return ""


def render_sql_template(sql_file: Path, overrides: dict[str, Any] | None = None) -> str:
    """Load and render a SQL template file using optional override values."""
    query = sql_file.read_text(encoding="utf-8")
    rendered = _BlankDefaultDict()

    if overrides:
        for key, value in overrides.items():
            rendered[key] = "" if value is None else str(value)

    return query.format_map(rendered)
