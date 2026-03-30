"""Utilities for rendering SQL templates with optional overrides.

This renderer intentionally leaves missing template keys as empty strings so
SQL files can define defaults via `COALESCE(NULLIF('{key}', ''), <default>)`.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any


class _BlankDefaultDict(dict):
    """Return empty string for missing format keys."""

    def __missing__(self, key: str) -> str:
        return ""


IDENTIFIER_KEYS = {
    "schema",
}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def render_sql_template(sql_file: Path, overrides: dict[str, Any] | None = None) -> str:
    """Load and render a SQL template file using optional override values."""
    query = sql_file.read_text(encoding="utf-8")
    rendered = _BlankDefaultDict()

    if overrides:
        for key, value in overrides.items():
            rendered_value = "" if value is None else str(value)
            if key in IDENTIFIER_KEYS and rendered_value:
                if not IDENTIFIER_RE.match(rendered_value):
                    raise ValueError(
                        f"Invalid SQL identifier override for '{key}': {rendered_value!r}"
                    )
            rendered[key] = rendered_value

    return query.format_map(rendered)
