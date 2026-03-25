"""Local DataFrame caching utility — parquet + metadata JSON.

Caches expensive DB pulls locally so repeated report runs are faster.
Follows the caching pattern from helioscta_python/pipelines/production,
extended with TTL-based freshness and metadata tracking.

Usage:
    from src.like_day_forecast.utils.cache_utils import pull_with_cache

    df = pull_with_cache(
        source_name="load_rt_metered_hourly",
        pull_fn=load_rt_metered_hourly.pull,
        pull_kwargs={"schema": "pjm_cleaned", "region": "RTO"},
        cache_dir=CACHE_DIR,
        ttl_hours=4,
    )
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


def _cache_key(source_name: str, pull_kwargs: dict[str, Any]) -> str:
    """Deterministic cache key from source name + pull parameters."""
    raw = json.dumps({"source": source_name, **pull_kwargs}, sort_keys=True, default=str)
    suffix = hashlib.md5(raw.encode()).hexdigest()[:10]
    return f"{source_name}_{suffix}"


def _meta_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(".meta.json")


def _write_metadata(parquet_path: Path, source_name: str, pull_kwargs: dict[str, Any], rows: int) -> None:
    meta = {
        "source_name": source_name,
        "pull_kwargs": {k: str(v) for k, v in pull_kwargs.items()},
        "rows": rows,
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "cached_at_epoch": time.time(),
    }
    _meta_path(parquet_path).write_text(json.dumps(meta, indent=2))


def _read_metadata(parquet_path: Path) -> dict | None:
    mp = _meta_path(parquet_path)
    if not mp.exists():
        return None
    try:
        return json.loads(mp.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _is_fresh(meta: dict, ttl_hours: float) -> bool:
    """Check if cached data is within TTL."""
    cached_epoch = meta.get("cached_at_epoch")
    if cached_epoch is None:
        return False
    age_hours = (time.time() - cached_epoch) / 3600
    return age_hours < ttl_hours


def get_cache(
    cache_dir: Path,
    cache_key: str,
    ttl_hours: float,
) -> pd.DataFrame | None:
    """Read cached parquet if it exists and is fresh. Returns None otherwise."""
    parquet_path = cache_dir / f"{cache_key}.parquet"

    if not parquet_path.exists():
        logger.info(f"Cache MISS (not found): {cache_key}")
        return None

    meta = _read_metadata(parquet_path)
    if meta is None:
        logger.info(f"Cache MISS (no metadata): {cache_key}")
        return None

    if not _is_fresh(meta, ttl_hours):
        age_hours = (time.time() - meta.get("cached_at_epoch", 0)) / 3600
        logger.info(f"Cache STALE ({age_hours:.1f}h old, TTL={ttl_hours}h): {cache_key}")
        return None

    try:
        df = pd.read_parquet(parquet_path)
        age_hours = (time.time() - meta["cached_at_epoch"]) / 3600
        logger.info(f"Cache HIT ({meta['rows']:,} rows, {age_hours:.1f}h old): {cache_key}")
        return df
    except Exception as e:
        logger.warning(f"Cache CORRUPT (falling back to fresh pull): {cache_key} — {e}")
        return None


def write_cache(
    df: pd.DataFrame,
    cache_dir: Path,
    cache_key: str,
    source_name: str,
    pull_kwargs: dict[str, Any],
) -> Path:
    """Write DataFrame to parquet + metadata JSON."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / f"{cache_key}.parquet"
    df.to_parquet(parquet_path, index=False)
    _write_metadata(parquet_path, source_name, pull_kwargs, len(df))
    logger.info(f"Cache WRITE ({len(df):,} rows): {parquet_path.name}")
    return parquet_path


def pull_with_cache(
    source_name: str,
    pull_fn: Callable[..., pd.DataFrame],
    pull_kwargs: dict[str, Any] | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    ttl_hours: float = 4.0,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Pull data, using local parquet cache when possible.

    Parameters
    ----------
    source_name : str
        Human-readable name for the data source (e.g. "load_rt_metered_hourly").
    pull_fn : Callable
        Function that returns a DataFrame (e.g. load_rt_metered_hourly.pull).
    pull_kwargs : dict, optional
        Keyword arguments forwarded to pull_fn.
    cache_dir : Path, optional
        Directory for parquet cache files. If None, caching is skipped.
    cache_enabled : bool
        Master switch. If False, always pulls fresh.
    ttl_hours : float
        Hours before cached data is considered stale.
    force_refresh : bool
        If True, bypass cache and pull fresh regardless of TTL.

    Returns
    -------
    pd.DataFrame
    """
    pull_kwargs = pull_kwargs or {}

    # Short-circuit: caching disabled or no cache dir
    if not cache_enabled or cache_dir is None:
        logger.info(f"Cache DISABLED — pulling fresh: {source_name}")
        return pull_fn(**pull_kwargs)

    key = _cache_key(source_name, pull_kwargs)

    # Try cache (unless force refresh)
    if not force_refresh:
        cached = get_cache(cache_dir, key, ttl_hours)
        if cached is not None:
            return cached
    else:
        logger.info(f"Cache FORCE REFRESH: {source_name}")

    # Fresh pull
    df = pull_fn(**pull_kwargs)

    # Write to cache
    write_cache(df, cache_dir, key, source_name, pull_kwargs)

    return df
