"""Profile database pull times for every source in the cache registry.

Runs each pull function sequentially (no caching, no concurrency) and
prints a ranked table sorted by elapsed time.

Usage:
    python -m src.scripts.profile_pulls
    python -m src.scripts.profile_pulls --only lmps_hourly_da gas_prices
    python -m src.scripts.profile_pulls --top 5
"""
import argparse
import logging
import sys
import time
from pathlib import Path

_BACKEND = str(Path(__file__).resolve().parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from src.scripts.refresh_cache import SOURCE_REGISTRY

logger = logging.getLogger(__name__)


def profile(
    only: list[str] | None = None,
    top: int | None = None,
) -> list[dict]:
    """Pull each source without caching and record wall-clock time.

    Returns list of dicts sorted slowest-first:
        [{"source": str, "rows": int, "elapsed_s": float, "error": str | None}, ...]
    """
    sources = SOURCE_REGISTRY
    if only:
        sources = [(n, fn, kw) for n, fn, kw in sources if n in only]
        unknown = set(only) - {n for n, _, _ in sources}
        if unknown:
            logger.warning(f"Unknown sources (skipped): {unknown}")

    results: list[dict] = []
    logger.info(f"Profiling {len(sources)} sources sequentially (no cache)")

    for name, fn, kwargs in sources:
        logger.info(f"  pulling {name} ...")
        t0 = time.perf_counter()
        try:
            df = fn(**kwargs)
            elapsed = time.perf_counter() - t0
            results.append({"source": name, "rows": len(df), "elapsed_s": elapsed, "error": None})
        except Exception as e:
            elapsed = time.perf_counter() - t0
            results.append({"source": name, "rows": -1, "elapsed_s": elapsed, "error": str(e)})
            logger.error(f"  {name}: FAILED in {elapsed:.2f}s — {e}")

    results.sort(key=lambda r: r["elapsed_s"], reverse=True)

    if top:
        results = results[:top]

    # Print summary table
    print(f"\n{'Source':<45} {'Rows':>10} {'Time (s)':>10} {'Status'}")
    print("-" * 80)
    for r in results:
        status = "OK" if r["error"] is None else f"FAIL: {r['error'][:30]}"
        rows = f"{r['rows']:,}" if r["rows"] >= 0 else "—"
        print(f"{r['source']:<45} {rows:>10} {r['elapsed_s']:>10.2f} {status}")

    total = sum(r["elapsed_s"] for r in results)
    print("-" * 80)
    print(f"{'Total':<45} {'':>10} {total:>10.2f}")

    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile database pull times")
    parser.add_argument(
        "--only", nargs="+", default=None,
        help="Only profile these source names (space-separated)",
    )
    parser.add_argument(
        "--top", type=int, default=None,
        help="Show only the N slowest sources",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_sources",
        help="List all available source names and exit",
    )
    return parser.parse_args()


def main():
    import src.like_day_forecast.settings  # noqa: F401 — loads .env

    args = _parse_args()

    if args.list_sources:
        for name, _, kwargs in SOURCE_REGISTRY:
            print(f"  {name:45s} {kwargs or ''}")
        return

    profile(only=args.only, top=args.top)


if __name__ == "__main__":
    main()
