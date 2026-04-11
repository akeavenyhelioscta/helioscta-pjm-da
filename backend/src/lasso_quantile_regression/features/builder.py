"""Build regression feature matrix from the shared like-day feature builder.

Reuses ``build_daily_features()`` from the like-day module, then adds:
  - Hourly LMP targets (Y variables) pivoted to wide format
  - Lagged LMP features (D-1, D-2, D-7 for each hour)
  - Interaction / nonlinear terms (net_load, reserve_margin, load^2, load x gas)
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.like_day_forecast import configs as ld_configs
from src.like_day_forecast.features.builder import build_daily_features
from src.lasso_quantile_regression.configs import (
    FUNDAMENTAL_COLS,
    HOURS,
    MINIMAL_COLS,
    LassoQRConfig,
)
from src.data import lmps_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

# Rough PJM installed capacity for reserve margin calc
_INSTALLED_CAPACITY_MW = 185_000


def build_regression_features(
    config: LassoQRConfig,
    df_features: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Build feature matrix suitable for quantile regression.

    Returns
    -------
    (df, feature_columns)
        *df* has columns: ``date``, ``target_HE1`` .. ``target_HE24`` (Y),
        plus all selected feature columns (X).
        *feature_columns* is the list of X column names to feed the model.
    """
    # Step 1: get shared feature matrix
    if df_features is None:
        df_features = build_daily_features(
            schema=config.schema,
            hub=config.hub,
            cache_dir=config.cache_dir,
            cache_enabled=config.cache_enabled,
            cache_ttl_hours=config.cache_ttl_hours,
            force_refresh=config.force_refresh,
        )

    df = df_features.copy()

    # Step 2: attach hourly LMP targets (Y variables)
    df_lmp_wide = _pull_hourly_lmp_wide(config)
    df = df.merge(df_lmp_wide, on="date", how="left")

    # Step 3: add lagged LMP features
    if config.include_lagged_lmp:
        for lag in [1, 2, 7]:
            for h in HOURS:
                src_col = f"target_HE{h}"
                if src_col in df.columns:
                    df[f"lmp_lag{lag}_HE{h}"] = df[src_col].shift(lag)

    # Step 4: add interaction / nonlinear terms
    if config.include_interaction_terms:
        _add_interaction_terms(df)

    # Step 5: select feature columns
    feature_cols = _select_feature_cols(df, config)

    return df, feature_cols


def _pull_hourly_lmp_wide(config: LassoQRConfig) -> pd.DataFrame:
    """Pull hourly DA LMP and pivot to ``date | target_HE1 .. target_HE24``.

    ``date`` is the reference date (D), while ``target_HE*`` are delivery-day
    prices for D+1. This aligns with shared ``tgt_*`` features that are shifted
    to reference-date indexing.
    """
    cache_kwargs = dict(
        cache_dir=config.cache_dir,
        cache_enabled=config.cache_enabled,
        ttl_hours=config.cache_ttl_hours,
        force_refresh=config.force_refresh,
    )
    df = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": config.schema, "market": "da"},
        **cache_kwargs,
    )
    df = df[df["hub"] == config.hub].copy()
    # Shift delivery date back one day so row date=D maps to delivery D+1.
    df["date"] = (pd.to_datetime(df["date"]) - pd.Timedelta(days=1)).dt.date
    df["hour_ending"] = df["hour_ending"].astype(int)

    wide = df.pivot_table(
        index="date", columns="hour_ending", values="lmp_total", aggfunc="first",
    )
    wide.columns = [f"target_HE{h}" for h in wide.columns]
    wide = wide.reset_index()
    return wide


def _add_interaction_terms(df: pd.DataFrame) -> None:
    """Add interaction and nonlinear terms in-place."""
    has_load = "tgt_load_daily_avg" in df.columns
    has_renewable = "tgt_renewable_daily_avg" in df.columns
    has_outage = "tgt_outage_total_mw" in df.columns
    has_gas = "gas_m3_daily_avg" in df.columns

    if has_load and has_renewable:
        df["net_load"] = df["tgt_load_daily_avg"] - df["tgt_renewable_daily_avg"]

    if has_load and has_outage:
        avail = _INSTALLED_CAPACITY_MW - df["tgt_outage_total_mw"]
        df["reserve_margin_pct"] = (
            (avail - df["tgt_load_daily_avg"]) / df["tgt_load_daily_avg"]
        )

    if has_load:
        df["load_squared"] = df["tgt_load_daily_avg"] ** 2

    if has_load and has_gas:
        df["load_x_gas"] = df["tgt_load_daily_avg"] * df["gas_m3_daily_avg"]

    if has_outage:
        df["outage_squared"] = df["tgt_outage_total_mw"] ** 2


def _select_feature_cols(df: pd.DataFrame, config: LassoQRConfig) -> list[str]:
    """Select feature columns based on ``config.feature_set``."""
    target_cols = {f"target_HE{h}" for h in HOURS}
    exclude = {"date"} | target_cols

    if config.feature_set == "minimal":
        base_cols = [c for c in MINIMAL_COLS if c in df.columns]
    elif config.feature_set == "fundamental":
        base_cols = [c for c in FUNDAMENTAL_COLS if c in df.columns]
    else:  # "full"
        base_cols = [
            c for c in df.select_dtypes(include=[np.number]).columns
            if c not in exclude
        ]

    feature_cols = list(base_cols)

    if config.include_lagged_lmp:
        feature_cols.extend(c for c in df.columns if c.startswith("lmp_lag"))

    if config.include_interaction_terms:
        for c in ["net_load", "reserve_margin_pct", "load_squared",
                   "load_x_gas", "outage_squared"]:
            if c in df.columns and c not in feature_cols:
                feature_cols.append(c)

    # Deduplicate preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for c in feature_cols:
        if c not in seen and c not in exclude:
            seen.add(c)
            deduped.append(c)

    drop_prefixes = tuple(config.drop_feature_prefixes or [])
    drop_names = set(config.drop_feature_names or [])

    pruned = [
        c for c in deduped
        if c not in drop_names and not any(c.startswith(p) for p in drop_prefixes)
    ]

    # Guarantee selected strategic signals (if available) remain in full feature mode.
    if config.feature_set == "full":
        for c in (config.force_include_features or []):
            if c in df.columns and c not in exclude and c not in pruned:
                pruned.append(c)

    return pruned
