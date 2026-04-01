"""Local DataFrame caching utility — parquet + metadata JSON.

One parquet file per data source (keyed by source_name, no hash).
Supports append-only writes: when a unique_key is provided, only
genuinely new rows are appended; existing rows are updated in-place.

Usage:
    from src.utils.cache_utils import pull_with_cache

    df = pull_with_cache(
        source_name="pjm_load_rt_metered_hourly",
        pull_fn=load_rt_metered_hourly.pull,
        pull_kwargs={"schema": "pjm_cleaned"},
        cache_dir=CACHE_DIR,
        ttl_hours=4,
        unique_key=["date", "hour_ending", "region"],
    )
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.like_day_forecast.utils.logging_utils import get_pipeline_logger

logger = logging.getLogger(__name__)


def _plog():
    """Return the PipelineLogger if initialized, else None."""
    return get_pipeline_logger()


def _section(title: str) -> None:
    """Log a section divider via PipelineLogger (falls back to stdlib)."""
    pl = _plog()
    if pl:
        pl.section(title)
    else:
        logger.info("")
        logger.info(f"{'─' * 10} {title} {'─' * 10}")


def _meta_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(".meta.json")


def _write_metadata(
    parquet_path: Path,
    source_name: str,
    pull_kwargs: dict[str, Any],
    rows: int,
    unique_key: list[str] | None = None,
    new_rows: int | None = None,
) -> None:
    meta = {
        "source_name": source_name,
        "pull_kwargs": {k: str(v) for k, v in pull_kwargs.items()},
        "rows": rows,
        "unique_key": unique_key,
        "last_pull_new_rows": new_rows,
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
    source_name: str,
    ttl_hours: float,
) -> pd.DataFrame | None:
    """Read cached parquet if it exists and is fresh. Returns None otherwise."""
    parquet_path = cache_dir / f"{source_name}.parquet"

    if not parquet_path.exists():
        logger.info(f"Cache MISS (not found): {source_name}")
        return None

    meta = _read_metadata(parquet_path)
    if meta is None:
        logger.info(f"Cache MISS (no metadata): {source_name}")
        return None

    if not _is_fresh(meta, ttl_hours):
        age_hours = (time.time() - meta.get("cached_at_epoch", 0)) / 3600
        logger.info(f"Cache STALE ({age_hours:.1f}h old, TTL={ttl_hours}h): {source_name}")
        return None

    try:
        df = pd.read_parquet(parquet_path)
        age_hours = (time.time() - meta["cached_at_epoch"]) / 3600
        logger.info(f"Cache HIT ({meta['rows']:,} rows, {age_hours:.1f}h old): {source_name}")
        return df
    except Exception as e:
        logger.warning(f"Cache CORRUPT (falling back to fresh pull): {source_name} — {e}")
        return None


def write_cache(
    df: pd.DataFrame,
    cache_dir: Path,
    source_name: str,
    pull_kwargs: dict[str, Any],
    unique_key: list[str] | None = None,
    incremental: bool = False,
) -> Path:
    """Write DataFrame to parquet + metadata JSON.

    If *incremental* is True, *unique_key* is provided, and a cached
    parquet already exists, merge new data with existing data
    (append-only with upsert on key). Otherwise, overwrite completely.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / f"{source_name}.parquet"

    is_init = not parquet_path.exists()
    pulled_rows = len(df)
    new_rows = pulled_rows

    if is_init:
        _section(f"{source_name} — INIT (no existing cache)")
        logger.info(f"  Writing full pull: {pulled_rows:,} rows")
    elif incremental and unique_key:
        try:
            df_existing = pd.read_parquet(parquet_path)
            existing_rows = len(df_existing)
            _section(f"{source_name} — REFRESH (merging into {existing_rows:,} cached rows)")
            df_combined = pd.concat([df_existing, df], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=unique_key, keep="last")
            new_rows = len(df_combined) - existing_rows
            df = df_combined
            logger.info(f"  Pulled {pulled_rows:,} rows → {new_rows:,} new, {len(df):,} total")
        except Exception as e:
            _section(f"{source_name} — REFRESH (merge failed, overwriting)")
            logger.warning(f"  {e}")
    else:
        _section(f"{source_name} — OVERWRITE (no unique_key)")
        logger.info(f"  Writing {pulled_rows:,} rows")

    df.to_parquet(parquet_path, index=False)
    _write_metadata(parquet_path, source_name, pull_kwargs, len(df), unique_key, new_rows)
    logger.info(f"  Saved {parquet_path.name} ({len(df):,} rows)")
    return parquet_path


def purge_stale(
    cache_dir: Path,
    max_age_hours: float = 24.0,
    dry_run: bool = False,
) -> dict[str, float]:
    """Delete cached parquet + metadata pairs older than *max_age_hours*.

    Parameters
    ----------
    cache_dir : Path
        Directory containing .parquet / .meta.json pairs.
    max_age_hours : float
        Files older than this are removed.
    dry_run : bool
        If True, log what *would* be deleted without touching the filesystem.

    Returns
    -------
    dict mapping deleted source name → age in hours.
    """
    if not cache_dir.exists():
        logger.info("Cache dir does not exist — nothing to purge")
        return {}

    now = time.time()
    deleted: dict[str, float] = {}
    orphans_removed = 0

    for pq in sorted(cache_dir.glob("*.parquet")):
        meta = _read_metadata(pq)
        meta_file = _meta_path(pq)

        if meta is None:
            # Orphan parquet with no metadata — remove unconditionally
            age_hours = (now - pq.stat().st_mtime) / 3600
            label = f"{pq.name} (orphan, {age_hours:.1f}h old)"
            if dry_run:
                logger.info(f"  [DRY RUN] would delete {label}")
            else:
                pq.unlink()
                if meta_file.exists():
                    meta_file.unlink()
                logger.info(f"  Deleted {label}")
            orphans_removed += 1
            continue

        age_hours = (now - meta.get("cached_at_epoch", 0)) / 3600
        if age_hours < max_age_hours:
            continue

        key = pq.stem
        label = f"{key} ({age_hours:.1f}h old, {meta.get('rows', '?')} rows)"
        if dry_run:
            logger.info(f"  [DRY RUN] would delete {label}")
        else:
            pq.unlink()
            if meta_file.exists():
                meta_file.unlink()
            logger.info(f"  Deleted {label}")
        deleted[key] = age_hours

    # Clean up any leftover .meta.json without a matching .parquet
    for mj in sorted(cache_dir.glob("*.meta.json")):
        pq = mj.with_suffix("").with_suffix(".parquet")
        if not pq.exists():
            if dry_run:
                logger.info(f"  [DRY RUN] would delete orphan metadata {mj.name}")
            else:
                mj.unlink()
                logger.info(f"  Deleted orphan metadata {mj.name}")
            orphans_removed += 1

    total = len(deleted) + orphans_removed
    action = "Would delete" if dry_run else "Purged"
    logger.info(f"{action} {total} files ({len(deleted)} stale, {orphans_removed} orphans)")
    return deleted


def pull_with_cache(
    source_name: str,
    pull_fn: Callable[..., pd.DataFrame],
    pull_kwargs: dict[str, Any] | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    ttl_hours: float = 4.0,
    force_refresh: bool = False,
    unique_key: list[str] | None = None,
    refresh_lookback_days: int | None = None,
) -> pd.DataFrame:
    """Pull data, using local parquet cache when possible.

    Parameters
    ----------
    source_name : str
        Human-readable name for the data source (e.g. "pjm_load_rt_metered_hourly").
        This is the cache key — one parquet file per source_name.
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
    unique_key : list[str], optional
        Columns that uniquely identify a row. When provided, writes
        use append-only merge (upsert) instead of full overwrite.
    refresh_lookback_days : int, optional
        When refreshing an *existing* cache, only pull the last N days
        from the DB (via sql_overrides start_date) instead of the full
        history. On INIT (no existing cache) the full pull always runs.

    Returns
    -------
    pd.DataFrame
    """
    pull_kwargs = pull_kwargs or {}

    # Short-circuit: caching disabled or no cache dir
    if not cache_enabled or cache_dir is None:
        logger.info(f"Cache DISABLED — pulling fresh: {source_name}")
        return pull_fn(**pull_kwargs)

    parquet_path = cache_dir / f"{source_name}.parquet"
    cache_exists = parquet_path.exists()

    # Try cache (unless force refresh)
    if not force_refresh:
        cached = get_cache(cache_dir, source_name, ttl_hours)
        if cached is not None:
            return cached
    else:
        logger.info(f"Cache FORCE REFRESH: {source_name}")

    # On refresh of existing cache, narrow the pull to recent rows only
    incremental = False
    if cache_exists and refresh_lookback_days and unique_key:
        from datetime import date, timedelta
        start = str(date.today() - timedelta(days=refresh_lookback_days))
        pull_kwargs = {**pull_kwargs, "sql_overrides": {"start_date": start}}
        incremental = True
        logger.info(f"  Incremental pull: last {refresh_lookback_days} days (>= {start})")

    # Pull from DB
    df = pull_fn(**pull_kwargs)

    # Write to cache (incremental=True merges with existing; False overwrites)
    write_cache(df, cache_dir, source_name, pull_kwargs, unique_key, incremental=incremental)

    return df
