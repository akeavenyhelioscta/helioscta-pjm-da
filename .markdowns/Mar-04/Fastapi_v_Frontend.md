# FastAPI vs Frontend Data Fetching: Architecture Comparison

**Date:** 2026-03-04
**Context:** Should PJM data sources be pulled through the FastAPI backend or directly in Next.js API routes?

**Validation scope:** Repo-verified facts are from this repository as of 2026-03-04. The Weather Bot section references an external repo (polymarket-kalshi-weather-bot) and is not independently verified here.

---

## 1. Current Architecture Snapshot

### PJM Platform (Hybrid Model)

**11 Next.js API routes** query Azure PostgreSQL directly via `lib/db.ts` connection pool. **3 FastAPI endpoints** (`/health`, `/like-day`, `/like-day-forecast`); comparison centered on `/like-day`.

```
Browser -> Next.js API Route -> Azure PostgreSQL (direct, pooled)
```

No Next.js-to-FastAPI proxy routes are currently active (removed in branch cleanup).

- Frontend pool: `pg.Pool` -- max 5 connections, 30s idle timeout, 15s connect timeout, SSL
- Backend DB: raw `psycopg2.connect()` -- new connection per query, no pool, no reuse
- All 11 direct routes use `Promise.all()` for parallel queries (2-12 queries per route)
- Uniform caching headers: `s-maxage=300, stale-while-revalidate=60`

### Weather Bot -- External Reference (Full-Backend Model)

**React is pure display.** All data flows through FastAPI -> SQLAlchemy (pooled) -> SQLite/PostgreSQL.

```
Browser -> FastAPI -> SQLAlchemy Engine -> Database
Browser -> FastAPI -> External APIs (cached in-memory, 30s-15min TTL)
```

- SQLAlchemy default connection pool (5 connections, 10 overflow)
- In-memory caching with TTL for external API calls (BTC candles: 30s, weather: 15min)
- Pydantic response models for type safety
- WebSocket for real-time updates

---

## 2. Performance Comparison

### Latency (Network Hops)

| Path | Hops | Typical Latency |
|------|------|-----------------|
| Next.js -> PostgreSQL (direct) | 1 | ~15-50ms per query (estimated, not benchmarked) |
| Next.js -> FastAPI -> PostgreSQL | 2 | ~80-200ms (estimated, not benchmarked) |
| FastAPI w/ connection pool (hypothetical) | 2 | ~40-100ms (estimated) |

The direct path eliminates one HTTP round-trip entirely. For the like-day endpoint, FastAPI creates fresh TCP connections per request: `connections per request ~ 2 x len(unique_markets)` (one pass in the `_pull_and_prefix` loop at lines 74-77, one pass in the profiles loop at lines 142-148), adding ~10-20ms handshake overhead per connection to Azure.

Note: The profiles pull loop (lines 142-148) re-pulls full market data via `lmps.pull()` for all like-day dates -- a second round of connections on the same markets already pulled in the feature-vector pass.

### Connection Pooling

| Aspect | Next.js (`pg.Pool`) | FastAPI (current `psycopg2`) |
|--------|---------------------|------------------------------|
| Pool size | 5 connections, reused | None -- new connection per query |
| Idle management | 30s timeout, auto-cleanup | N/A -- immediate close |
| Hot-reload safe | `globalThis._pgPool` in dev | N/A |
| Connection overhead | First request only (~15ms) | Every query (~10-20ms TCP + auth) |

**Impact:** A like-day request opening multiple connections wastes ~60-120ms (estimated) just on connection setup. Adding `psycopg2.pool.ThreadedConnectionPool` or switching to `asyncpg` would eliminate this.

### Query Parallelism

| Pattern | Next.js | FastAPI |
|---------|---------|--------|
| Parallel queries | `Promise.all()` -- native, zero overhead | Sequential `for` loop in `pipeline.py` |
| Dashboard example | 8 queries in Phase 1, 4 in Phase 2 | N/A |
| Like-day pipeline | N/A | Markets pulled sequentially, then pulled again for profiles |

Next.js routes routinely fire 2-12 queries in parallel. The FastAPI like-day pipeline pulls markets in a `for` loop:

```python
# pipeline.py lines 74-77 -- sequential market pulls
for mkt in unique_markets:
    df_mkt = _pull_and_prefix(hub=hub, market=mkt)  # New DB connection each time
    dfs.append(df_mkt)
```

With `asyncpg` + `asyncio.gather()`, these could run in parallel.

### Serialization Overhead

| Step | Next.js | FastAPI |
|------|---------|--------|
| DB -> app | `pg` returns JS objects directly | `psycopg2` -> `pd.read_sql()` -> DataFrame |
| App -> response | `JSON.stringify(rows)` | `df.to_dict(orient="records")` -> FastAPI JSON |
| Date handling | Native JS Date serialization | Manual `.astype(str)` conversion |
| NaN handling | N/A (SQL NULLs -> `null`) | `.where(df.notna(), None)` then serialize |

FastAPI pays a double serialization tax: SQL rows -> DataFrame -> dict -> JSON. For simple reads this overhead is unnecessary. For ML workloads the DataFrame is essential.

Note: `/like-day` uses direct `.to_dict(orient="records")` on DataFrame copies. `/like-day-forecast` uses a `_serialize_df()` helper (NaN -> None, dates -> str) plus manual `fan_chart` iteration over `df_forecast` rows. These are distinct serialization paths.

---

## 3. Architecture Pros and Cons

### Direct Next.js Queries

| Dimension | Assessment |
|-----------|------------|
| **Latency** | Fastest path -- single hop to DB, pooled connections |
| **Parallelism** | Excellent -- `Promise.all()` pattern across all routes |
| **Caching** | HTTP cache headers (`s-maxage=300`) work at CDN/browser level |
| **Credentials** | DB credentials in frontend `.env.local` -- acceptable for server-side API routes (never exposed to browser) |
| **Type safety** | Manual -- `as { field: type }` casts on query results |
| **Schema changes** | Must update hardcoded schema name in every SQL string |
| **Deployment** | Single service (Next.js) -- simpler infrastructure |
| **ML/compute** | Not practical -- no pandas, sklearn, numpy in Node.js |
| **Testing** | Harder -- queries embedded in route handlers |

### Full FastAPI Backend

| Dimension | Assessment |
|-----------|------------|
| **Latency** | Extra HTTP hop; currently worse due to no connection pool |
| **Parallelism** | Currently sequential; fixable with async |
| **Caching** | No caching implemented; could add Redis or in-memory TTL |
| **Credentials** | Centralized in backend -- frontend only needs `PYTHON_API_URL` |
| **Type safety** | Endpoints return raw dicts / DataFrame-derived payloads; Pydantic `response_model` not yet applied |
| **Schema changes** | Single `configs.py` constant |
| **Deployment** | Two services -- more infrastructure, but independent scaling |
| **ML/compute** | Native -- pandas, sklearn, numpy, full Python ecosystem |
| **Testing** | Easier -- pipeline functions testable independently |

---

## 4. Recommendation: Keep Hybrid, Improve Both Sides

The hybrid model is the right architecture. Forcing all reads through FastAPI adds latency and complexity for no benefit. Forcing ML into Node.js is impractical.

### Keep in Next.js (Simple Reads)
All 11 current API routes should stay. They're fast (single hop), parallel (`Promise.all`), and cached (5min `s-maxage`). Moving them to FastAPI would add ~30-80ms per request with no upside.

### Keep in FastAPI (ML/Compute)
Like-day similarity, DA forecast models, and any future ML features belong in Python. The ecosystem (pandas, sklearn, statsmodels) has no JS equivalent.

### Priority Improvements

#### P0 -- Backend connection pooling (`azure_postgresql.py`)

**Why:** Every query opens a new TCP + auth handshake to Azure (`psycopg2.connect()` at line 35 of `azure_postgresql.py`).

**Expected impact:** Eliminate ~60-120ms overhead per like-day request (estimated).

**Validation:** Time a `/like-day` request before/after; check connection count in `pg_stat_activity`.

```python
# backend/src/utils/azure_postgresql.py
from psycopg2.pool import ThreadedConnectionPool

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2, maxconn=10,
            user=settings.AZURE_POSTGRESQL_DB_USER,
            password=settings.AZURE_POSTGRESQL_DB_PASSWORD,
            host=settings.AZURE_POSTGRESQL_DB_HOST,
            port=settings.AZURE_POSTGRESQL_DB_PORT,
            dbname=settings.AZURE_POSTGRESQL_DB_NAME,
        )
    return _pool

def pull_from_db(query: str, database: str) -> pd.DataFrame:
    pool = get_pool()
    conn = pool.getconn()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        pool.putconn(conn)
```

#### P1 -- Reduce repeated like-day pulls (cache or refactor)

**Why:** The pipeline pulls the same market data twice -- once for feature vectors (lines 74-77) and once for hourly profiles (lines 142-148). Both call `lmps.pull(hub, market)` which opens a new DB connection each time.

**Expected impact:** Cut DB round-trips by ~50% per like-day request.

**Validation:** Add logging to `pull_from_db`, count calls per request.

#### P2 -- Centralize frontend schema constant (`lib/db.ts`)

**Why:** `dbt_pjm_v1_2026_feb_19` is hardcoded in 11 route files.

**Expected impact:** Single-line change on schema rotation instead of 11.

**Validation:** Grep for old schema name returns 0 hits after migration.

```typescript
// frontend/lib/db.ts
export const SCHEMA = "dbt_pjm_v1_2026_feb_19";
```

Then in routes: `` `SELECT ... FROM ${SCHEMA}.staging_v1_pjm_lmps_hourly` ``

---

## 5. Summary Comparison Table

| Dimension | Next.js Direct | FastAPI Backend | Winner |
|-----------|---------------|-----------------|--------|
| **Query latency** | ~15-50ms (1 hop, pooled) | ~80-200ms (2 hops, no pool) | Next.js |
| **Parallelism** | `Promise.all()` native | Sequential (fixable) | Next.js |
| **Connection pooling** | `pg.Pool` (max 5) | None (new conn per query) | Next.js |
| **Serialization** | Rows -> JSON (direct) | Rows -> DataFrame -> dict -> JSON | Next.js |
| **ML/compute** | Not viable | pandas, sklearn, numpy | FastAPI |
| **Type safety** | Manual casts | Raw dicts (Pydantic not yet applied) | Neutral |
| **Credential scope** | DB creds in frontend env | Centralized in backend | FastAPI |
| **Schema management** | Hardcoded in 11 routes | Single `configs.py` | FastAPI |
| **Deployment** | 1 service | 2 services | Next.js |

**Verdict:** Hybrid remains preferred under current workload mix and implementation constraints.

The priority improvements are:

1. **P0: FastAPI connection pooling** (biggest latency win)
2. **P1: Reduce repeated like-day pulls** (cut DB round-trips ~50%)
3. **P2: Shared schema constant** in `lib/db.ts` (maintenance win)
