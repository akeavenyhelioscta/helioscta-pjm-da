"""View model: ICE PJM Power — daily settlements and intraday tape.

Combines the daily settlement history (lookback) with the current
session's intraday snapshot tape to show how DA/RT products are
settling and trading.

Consumed by:
  - API endpoints (JSON)
  - Markdown formatters (MD)
  - Agent (structured context for market price inspection)
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _delivery_date(trade_date: date, symbol: str) -> date | None:
    """Derive the delivery date from trade_date and ICE symbol tenor.

    D0 = same-day (BalDay), D1 = next business day (NxtDay).
    Weekly and other tenors return None (not a single-day product).
    """
    m = re.search(r"[A-Z]{3}\s+(D[01])", symbol)
    if not m:
        return None
    tenor = m.group(1)
    if tenor == "D0":
        return trade_date
    # D1: next business day (skip weekends)
    dt = trade_date + timedelta(days=1)
    while dt.weekday() >= 5:  # 5=Sat, 6=Sun
        dt += timedelta(days=1)
    return dt


def _sr(val, decimals: int = 2) -> float | None:
    """Safe round — return None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, decimals)
    except (TypeError, ValueError):
        return None


def build_view_model(
    df_settles: pd.DataFrame | None = None,
    df_intraday: pd.DataFrame | None = None,
) -> dict:
    """Build the ICE power intraday view model.

    Args:
        df_settles: Output of ``ice_power_intraday.pull_settles()``.
        df_intraday: Output of ``ice_power_intraday.pull_intraday()``.

    Returns:
        Structured dict with settlement history and intraday tape sections.
    """
    has_settles = df_settles is not None and len(df_settles) > 0
    has_intraday = df_intraday is not None and len(df_intraday) > 0

    if not has_settles and not has_intraday:
        return {"error": "No ICE power data available."}

    result: dict = {}

    if has_settles:
        result["settlements"] = _build_settlements_section(df_settles)

    if has_intraday:
        result["intraday"] = _build_intraday_section(df_intraday)

    return result


# ── Settlements ─────────────────────────────────────────────────


def _build_settlements_section(df: pd.DataFrame) -> dict:
    """Daily settlement table per product with day-over-day changes."""
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    dates = sorted(df["trade_date"].unique())
    products = sorted(df["product"].dropna().unique())

    # Per-product daily rows
    by_product: dict[str, list[dict]] = {}
    for product in products:
        pdf = df[df["product"] == product].sort_values("trade_date")
        rows = []
        for _, row in pdf.iterrows():
            dd = _delivery_date(row["trade_date"], row["symbol"])
            rows.append({
                "trade_date": str(row["trade_date"]),
                "delivery_date": str(dd) if dd else None,
                "symbol": row["symbol"],
                "peak_type": row.get("peak_type"),
                "settle": _sr(row.get("settle")),
                "prior_settle": _sr(row.get("prior_settle")),
                "settle_vs_prior": _sr(row.get("settle_vs_prior")),
                "vwap": _sr(row.get("vwap")),
                "high": _sr(row.get("high")),
                "low": _sr(row.get("low")),
                "volume": _sr(row.get("volume"), decimals=0),
            })
        by_product[product] = rows

    # Cross-product daily summary (settle per product per date)
    daily_matrix: list[dict] = []
    for d in dates:
        entry: dict = {"trade_date": str(d)}
        ddf = df[df["trade_date"] == d]
        for _, row in ddf.iterrows():
            p = row.get("product")
            if p:
                entry[p] = _sr(row.get("settle"))
                dd = _delivery_date(d, row["symbol"])
                if dd:
                    entry[f"{p}_delivery"] = str(dd)
        daily_matrix.append(entry)

    # Product-level metadata (peak_type, symbol)
    product_meta: dict[str, dict] = {}
    for product in products:
        pdf = df[df["product"] == product]
        row0 = pdf.iloc[0]
        product_meta[product] = {
            "symbol": row0["symbol"],
            "peak_type": row0.get("peak_type"),
        }

    return {
        "date_range": {
            "start": str(dates[0]) if dates else None,
            "end": str(dates[-1]) if dates else None,
        },
        "products": products,
        "product_meta": product_meta,
        "by_product": by_product,
        "daily_matrix": daily_matrix,
    }


# ── Intraday Tape ──────────────────────────────────────────────


def _build_intraday_section(df: pd.DataFrame) -> dict:
    """Intraday snapshot tape grouped by product and date."""
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["snapshot_at"] = pd.to_datetime(df["snapshot_at"])

    dates = sorted(df["trade_date"].unique())
    products = sorted(df["product"].dropna().unique())

    # Per-product, per-date tape
    by_product: dict[str, dict] = {}
    for product in products:
        pdf = df[df["product"] == product]
        by_date: dict[str, dict] = {}
        for d in dates:
            ddf = pdf[pdf["trade_date"] == d].sort_values("snapshot_at")
            if ddf.empty:
                continue

            snapshots = []
            for _, row in ddf.iterrows():
                snapshots.append({
                    "time_et": str(row["time_et"]),
                    "bid": _sr(row.get("bid")),
                    "ask": _sr(row.get("ask")),
                    "spread": _sr(row.get("spread")),
                    "last_px": _sr(row.get("last_px")),
                    "vwap": _sr(row.get("vwap")),
                    "volume": _sr(row.get("volume"), decimals=0),
                    "last_chg": _sr(row.get("last_chg")),
                })

            # Session summary
            last_row = ddf.iloc[-1]
            sym = ddf["symbol"].iloc[0]
            dd = _delivery_date(d, sym)
            by_date[str(d)] = {
                "delivery_date": str(dd) if dd else None,
                "n_snapshots": len(ddf),
                "session_open": _sr(ddf["open_px"].dropna().iloc[0]) if not ddf["open_px"].dropna().empty else None,
                "session_high": _sr(ddf["high"].max()),
                "session_low": _sr(ddf["low"].min()),
                "session_last": _sr(last_row.get("last_px")),
                "session_vwap": _sr(last_row.get("vwap")),
                "session_volume": _sr(last_row.get("volume"), decimals=0),
                "snapshots": snapshots,
            }

        by_product[product] = by_date

    # Product-level metadata
    product_meta: dict[str, dict] = {}
    for product in products:
        pdf = df[df["product"] == product]
        row0 = pdf.iloc[0]
        product_meta[product] = {
            "symbol": row0["symbol"],
            "peak_type": row0.get("peak_type"),
        }

    return {
        "date_range": {
            "start": str(dates[0]) if dates else None,
            "end": str(dates[-1]) if dates else None,
        },
        "products": products,
        "product_meta": product_meta,
        "by_product": by_product,
    }


if __name__ == "__main__":
    import json

    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.data import ice_power_intraday
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    df_settles = pull_with_cache(
        source_name="ice_power_settles",
        pull_fn=ice_power_intraday.pull_settles,
        pull_kwargs={"lookback_days": 30},
        **CACHE,
    )
    df_intraday = pull_with_cache(
        source_name="ice_power_intraday",
        pull_fn=ice_power_intraday.pull_intraday,
        pull_kwargs={"lookback_days": 3},
        **CACHE,
    )

    vm = build_view_model(df_settles, df_intraday)
    print(json.dumps(vm, indent=2, default=str))
