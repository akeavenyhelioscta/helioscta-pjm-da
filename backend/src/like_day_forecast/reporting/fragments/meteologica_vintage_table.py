"""Meteologica forecast vintage comparison table — today + tomorrow.

Renders an HTML table with one row per forecast_datetime and one column per
vintage (Latest, DA Cutoff, DA -12h, DA -24h, DA -48h).  Each cell shows
forecast_load_mw; forecast_rank is shown on hover via a styled tooltip.
Text brightness encodes rank (higher rank = brighter).
"""
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.like_day_forecast import configs
from src.like_day_forecast.data import meteologica_load_forecast_hourly
from src.like_day_forecast.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

_VINTAGE_ORDER = ["Latest", "DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]
_VINTAGE_COLORS = {
    "Latest": "#60a5fa",
    "DA Cutoff": "#f87171",
    "DA -12h": "#a78bfa",
    "DA -24h": "#34d399",
    "DA -48h": "#fbbf24",
}

Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Build Meteologica vintage comparison table for today + tomorrow."""
    logger.info("Building Meteologica vintage table...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    df = _safe_pull(
        "meteo_vintage_table_v1",
        _pull_vintages,
        {},
        **cache_kwargs,
    )

    if df is None or len(df) == 0:
        return [("Meteologica Vintage Table", _empty("No Meteologica vintage data."), None)]

    today = date.today()
    tomorrow = today + timedelta(days=1)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"])
    mask = df["forecast_date"].dt.date.isin([today, tomorrow])
    df = df[mask].copy()

    if len(df) == 0:
        return [("Meteologica Vintage Table", _empty("No data for today/tomorrow."), None)]

    html = _render_table(df)
    return [("Meteologica Vintage Table", html, None)]


# ── Data helpers ─────────────────────────────────────────────────────


def _safe_pull(source_name, pull_fn, pull_kwargs, **cache_kwargs):
    try:
        return pull_with_cache(
            source_name=source_name,
            pull_fn=pull_fn,
            pull_kwargs=pull_kwargs,
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"{source_name} pull failed: {e}")
        return None


def _pull_vintages(region: str = "RTO") -> pd.DataFrame:
    """Pull Latest + DA cutoff vintages for Meteologica."""
    _cols = [
        "forecast_date", "hour_ending", "forecast_load_mw",
        "forecast_rank", "forecast_execution_datetime",
        "vintage_label",
    ]

    # Latest
    df_latest = meteologica_load_forecast_hourly.pull(region=region)
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest = df_latest[[c for c in _cols if c in df_latest.columns]]
    else:
        df_latest = pd.DataFrame(columns=_cols)

    # DA cutoff vintages
    df_da = meteologica_load_forecast_hourly.pull_da_cutoff_vintages(region=region)
    if df_da is not None and len(df_da) > 0:
        df_da["forecast_date"] = pd.to_datetime(df_da["forecast_date"])
        df_da = df_da[[c for c in _cols if c in df_da.columns]]
    else:
        df_da = pd.DataFrame(columns=_cols)

    df = pd.concat([df_latest, df_da], ignore_index=True)
    if len(df) == 0:
        return pd.DataFrame()

    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["forecast_rank"] = pd.to_numeric(df["forecast_rank"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["forecast_date", "hour_ending", "forecast_load_mw"]).copy()
    df["hour_ending"] = df["hour_ending"].astype(int)
    return df


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


# ── Table rendering ──────────────────────────────────────────────────


def _rank_opacity(rank, max_rank):
    """Map forecast_rank to text opacity (higher rank = brighter)."""
    if pd.isna(rank) or pd.isna(max_rank) or max_rank <= 1:
        return 0.9
    return 0.4 + 0.6 * ((rank - 1) / (max_rank - 1))


def _render_table(df: pd.DataFrame) -> str:
    """Build the HTML table with vintage columns and rank-encoded brightness."""
    df["forecast_datetime"] = (
        pd.to_datetime(df["forecast_date"])
        + pd.to_timedelta(df["hour_ending"], unit="h")
    )

    # Compute max rank per vintage for opacity scaling
    max_ranks = df.groupby("vintage_label")["forecast_rank"].max().to_dict()

    # Pivot: rows = forecast_datetime, columns = vintage_label
    vintages_present = [v for v in _VINTAGE_ORDER if v in df["vintage_label"].unique()]

    rows_html = []
    for dt in sorted(df["forecast_datetime"].unique()):
        dt_ts = pd.Timestamp(dt)
        dt_label = dt_ts.strftime("%a %m/%d %H:%M")

        cells = f'<td class="vt-dt">{dt_label}</td>'
        for v in vintages_present:
            match = df[(df["forecast_datetime"] == dt) & (df["vintage_label"] == v)]
            if len(match) == 0:
                cells += '<td class="vt-cell vt-empty">&mdash;</td>'
                continue

            row = match.iloc[0]
            mw = row["forecast_load_mw"]
            rank = row["forecast_rank"]
            exec_dt = row.get("forecast_execution_datetime")

            mw_str = f"{mw:,.0f} MW"
            rank_str = f"Rank {int(rank)}" if pd.notna(rank) else ""
            exec_str = ""
            if pd.notna(exec_dt):
                exec_str = f" | Exec: {pd.Timestamp(exec_dt).strftime('%m/%d %I:%M %p')}"

            opacity = _rank_opacity(rank, max_ranks.get(v, 1))
            color = _VINTAGE_COLORS.get(v, "#e0e0e0")

            cells += (
                f'<td class="vt-cell" '
                f'style="opacity:{opacity:.2f};" '
                f'title="{rank_str}{exec_str}">'
                f'{mw_str}</td>'
            )

        rows_html.append(f"<tr>{cells}</tr>")

    # Header
    header_cells = '<th class="vt-hdr">Forecast Datetime</th>'
    for v in vintages_present:
        color = _VINTAGE_COLORS.get(v, "#e0e0e0")
        header_cells += f'<th class="vt-hdr" style="color:{color};">{v}</th>'

    table_html = f"""
<div class="vt-wrap">
<table class="vt-table">
<thead><tr>{header_cells}</tr></thead>
<tbody>
{"".join(rows_html)}
</tbody>
</table>
</div>
"""

    return _STYLE + table_html


# ── CSS ──────────────────────────────────────────────────────────────

_STYLE = """
<style>
.vt-wrap {
    overflow-x: auto;
    padding: 12px;
}
.vt-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 13px;
}
.vt-table th, .vt-table td {
    padding: 6px 14px;
    text-align: right;
    white-space: nowrap;
}
.vt-hdr {
    background: #141e30;
    color: #8db4e0;
    font-weight: 600;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    border-bottom: 2px solid #2a3f60;
    position: sticky;
    top: 0;
    z-index: 1;
}
.vt-dt {
    text-align: left;
    color: #9eb4d3;
    font-weight: 500;
    font-size: 12px;
    background: #0e1520;
    position: sticky;
    left: 0;
    z-index: 1;
}
.vt-cell {
    color: #e0e0e0;
    font-variant-numeric: tabular-nums;
    cursor: default;
}
.vt-empty {
    color: #4a5568;
    opacity: 0.5;
}
.vt-table tbody tr {
    border-bottom: 1px solid #1a2a42;
}
.vt-table tbody tr:hover {
    background: #1a2640;
}
.vt-table tbody tr:hover .vt-dt {
    background: #1a2640;
}
</style>
"""
