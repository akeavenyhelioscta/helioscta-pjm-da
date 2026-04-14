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


def _filter_by_products(df: pd.DataFrame, products: list[str] | None) -> pd.DataFrame:
    """Filter dataframe to only include rows matching the given product labels."""
    if not products:
        return df
    return df[df["product"].isin(products)]


def _filter_by_delivery_date(
    df: pd.DataFrame, delivery: date | None,
) -> pd.DataFrame:
    """Keep only rows whose derived delivery_date matches the target."""
    if delivery is None:
        return df
    mask = df.apply(
        lambda r: _delivery_date(
            pd.to_datetime(r["trade_date"]).date()
            if not isinstance(r["trade_date"], date) else r["trade_date"],
            r["symbol"],
        ) == delivery,
        axis=1,
    )
    return df[mask]


def _compress_snapshots(snapshots: list[dict]) -> list[dict]:
    """Collapse consecutive unchanged snapshots into single rows.

    Two snapshots are "unchanged" when bid, ask, spread, last_px, vwap,
    volume, and last_chg all match.  Collapsed rows get ``time_start`` /
    ``time_end`` instead of ``time_et``.
    """
    if not snapshots:
        return []

    compare_keys = ("bid", "ask", "spread", "last_px", "vwap", "volume", "last_chg")

    def _sig(snap: dict) -> tuple:
        return tuple(snap.get(k) for k in compare_keys)

    compressed: list[dict] = []
    run_start = snapshots[0]
    run_end = snapshots[0]

    for snap in snapshots[1:]:
        if _sig(snap) == _sig(run_start):
            run_end = snap
        else:
            # Flush the current run
            if run_start["time_et"] == run_end["time_et"]:
                compressed.append(run_start)
            else:
                entry = {k: run_start[k] for k in compare_keys}
                entry["time_start"] = run_start["time_et"]
                entry["time_end"] = run_end["time_et"]
                compressed.append(entry)
            run_start = snap
            run_end = snap

    # Flush last run
    if run_start["time_et"] == run_end["time_et"]:
        compressed.append(run_start)
    else:
        entry = {k: run_start[k] for k in compare_keys}
        entry["time_start"] = run_start["time_et"]
        entry["time_end"] = run_end["time_et"]
        compressed.append(entry)

    return compressed


def build_view_model(
    df_settles: pd.DataFrame | None = None,
    df_intraday: pd.DataFrame | None = None,
    *,
    products: list[str] | None = None,
    delivery_date: date | None = None,
    include_snapshots: bool = False,
) -> dict:
    """Build the ICE power intraday view model.

    Args:
        df_settles: Output of ``ice_power_intraday.pull_settles()``.
        df_intraday: Output of ``ice_power_intraday.pull_intraday()``.
        products: Filter to specific product labels
            (e.g. ``["NxtDay DA", "NxtDay RT"]``).  None = all products.
        delivery_date: Filter settlements and intraday to rows whose
            derived delivery date matches this date.  None = no filter.
        include_snapshots: If True, include the full (compressed)
            intraday snapshot tape.  Defaults to False — only session
            summaries are returned.

    Returns:
        Structured dict with settlement history, intraday tape, and
        cross-product session summary sections.
    """
    # Apply filters to copies of the dataframes
    if df_settles is not None and len(df_settles) > 0:
        df_settles = df_settles.copy()
        df_settles["trade_date"] = pd.to_datetime(df_settles["trade_date"]).dt.date
        df_settles = _filter_by_products(df_settles, products)
        df_settles = _filter_by_delivery_date(df_settles, delivery_date)

    if df_intraday is not None and len(df_intraday) > 0:
        df_intraday = df_intraday.copy()
        df_intraday["trade_date"] = pd.to_datetime(df_intraday["trade_date"]).dt.date
        df_intraday = _filter_by_products(df_intraday, products)
        df_intraday = _filter_by_delivery_date(df_intraday, delivery_date)

    has_settles = df_settles is not None and len(df_settles) > 0
    has_intraday = df_intraday is not None and len(df_intraday) > 0

    if not has_settles and not has_intraday:
        return {"error": "No ICE power data available."}

    result: dict = {}

    if has_settles:
        result["settlements"] = _build_settlements_section(df_settles)

    if has_intraday:
        result["intraday"] = _build_intraday_section(
            df_intraday, include_snapshots=include_snapshots,
        )

    # Always include a lightweight cross-product session summary
    if has_intraday:
        result["session_summary"] = _build_session_summary(df_intraday)

    return result


# ── Settlements ─────────────────────────────────────────────────


def _build_settlements_section(df: pd.DataFrame) -> dict:
    """Daily settlement table per product with day-over-day changes."""
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


def _build_intraday_section(
    df: pd.DataFrame,
    *,
    include_snapshots: bool = False,
) -> dict:
    """Intraday snapshot tape grouped by product and date.

    When ``include_snapshots`` is False (default), only session-level
    summaries are returned — the full tape is omitted.  When True, the
    tape is compressed to collapse consecutive unchanged rows.
    """
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

            # Session summary
            last_row = ddf.iloc[-1]
            sym = ddf["symbol"].iloc[0]
            dd = _delivery_date(d, sym)
            session: dict = {
                "delivery_date": str(dd) if dd else None,
                "n_snapshots": len(ddf),
                "session_open": _sr(ddf["open_px"].dropna().iloc[0]) if not ddf["open_px"].dropna().empty else None,
                "session_high": _sr(ddf["high"].max()),
                "session_low": _sr(ddf["low"].min()),
                "session_last": _sr(last_row.get("last_px")),
                "session_vwap": _sr(last_row.get("vwap")),
                "session_volume": _sr(last_row.get("volume"), decimals=0),
            }

            if include_snapshots:
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
                session["snapshots"] = _compress_snapshots(snapshots)

            by_date[str(d)] = session

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


# ── Session Summary ────────────────────────────────────────────


def _build_session_summary(df: pd.DataFrame) -> list[dict]:
    """Cross-product session summary keyed by delivery date.

    Returns a list of dicts, one per delivery date, each containing
    a per-product summary of intraday session stats.  This is the
    lightweight view the agent needs for quick cross-product comparison.
    """
    df["snapshot_at"] = pd.to_datetime(df["snapshot_at"])

    products = sorted(df["product"].dropna().unique())

    # Collect session stats keyed by (delivery_date, product)
    sessions: dict[str, dict[str, dict]] = {}
    for product in products:
        pdf = df[df["product"] == product]
        for d in sorted(pdf["trade_date"].unique()):
            ddf = pdf[pdf["trade_date"] == d].sort_values("snapshot_at")
            if ddf.empty:
                continue
            sym = ddf["symbol"].iloc[0]
            dd = _delivery_date(d, sym)
            if dd is None:
                continue
            dd_str = str(dd)

            last_row = ddf.iloc[-1]
            stats = {
                "settle": _sr(last_row.get("settle")) if "settle" in ddf.columns else None,
                "vwap": _sr(last_row.get("vwap")),
                "last": _sr(last_row.get("last_px")),
                "high": _sr(ddf["high"].max()),
                "low": _sr(ddf["low"].min()),
                "volume": _sr(last_row.get("volume"), decimals=0),
            }

            if dd_str not in sessions:
                sessions[dd_str] = {}
            sessions[dd_str][product] = stats

    return [
        {"delivery_date": dd, "products": prods}
        for dd, prods in sorted(sessions.items())
    ]


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

    vm = build_view_model(
        df_settles, df_intraday,
        include_snapshots=True,
    )
    print(json.dumps(vm, indent=2, default=str))
