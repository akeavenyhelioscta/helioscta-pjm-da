# Plan: Frontend ↔ Backend Communication for Like-Day on LMP Data

## Architecture Decision: FastAPI Microservice

**Approach:** Run the Python backend as a **FastAPI HTTP service** that the Next.js frontend calls from its API routes.

**Why FastAPI over alternatives:**

| Option | Pros | Cons |
|---|---|---|
| **FastAPI service** | Clean separation, async, auto-docs, native pandas/numpy, easy to extend | Extra process to run |
| Python subprocess from Node | No extra process | Slow startup per request, brittle arg passing, no streaming |
| Write results to DB, read from frontend | Decoupled | Stale data, extra DB table, no on-demand params |
| Rewrite in TypeScript | Same runtime | Lose scikit-learn, pandas, numpy ecosystem |

---

## System Diagram

```
┌─────────────────────────────────────────────────────────┐
│  Browser                                                │
│  PjmLmpsHourlyTable.tsx                                 │
│    ├── fetch /api/pjm-lmps-hourly       (existing)      │
│    └── fetch /api/pjm-like-day          (new)           │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP
┌──────────────────────▼──────────────────────────────────┐
│  Next.js API Routes (Node.js)                           │
│    ├── /api/pjm-lmps-hourly/route.ts    (existing)      │
│    └── /api/pjm-like-day/route.ts       (new proxy)     │
│         → forwards to Python FastAPI                     │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP (localhost:8000)
┌──────────────────────▼──────────────────────────────────┐
│  Python FastAPI Service                                  │
│    POST /like-day                                        │
│    ├── Calls pipeline.run()                              │
│    ├── Returns JSON: like days + hourly profiles         │
│    └── Connects to Azure PostgreSQL (same DB)            │
└─────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Implementation

### Step 1: Add FastAPI to Python Backend

**File:** `backend/src/api.py` (new)

Create a FastAPI app that wraps the existing `pipeline.run()` function:

```python
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, datetime, timedelta
from src.pjm_like_day.pipeline import run

app = FastAPI(title="Helios CTA - PJM Like Day API")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/like-day")
def like_day(
    target_date: date = Query(default=None),        # defaults to tomorrow
    hub: str = Query(default="WESTERN HUB"),
    n_neighbors: int = Query(default=5, ge=1, le=20),
    metric: str = Query(default="cosine"),
):
    if target_date is None:
        target_date = datetime.now().date() + timedelta(days=1)

    results = run(target_date=target_date, n_neighbors=n_neighbors, metric=metric)

    return {
        "target_date": str(target_date),
        "hub": hub,
        "metric": metric,
        "like_days": results.to_dict(orient="records"),
    }
```

**Dependencies to add:** `fastapi`, `uvicorn`

**Run with:** `uvicorn src.api:app --reload --port 8000`

---

### Step 2: Extend Pipeline to Accept Hub Parameter

**File:** `backend/src/pjm_like_day/pipeline.py`

Currently the pipeline hardcodes `WESTERN HUB` via configs. Modify `run()` to accept `hub` as a parameter and pass it through to the data layer. This lets the frontend control which hub to analyze.

---

### Step 3: Add Next.js Proxy API Route

**File:** `frontend/app/api/pjm-like-day/route.ts` (new)

Create a thin proxy that forwards the request to the Python service:

```typescript
import { NextResponse } from "next/server";

const PYTHON_API_URL = process.env.PYTHON_API_URL || "http://localhost:8000";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const target_date = searchParams.get("target_date");
  const hub = searchParams.get("hub") || "WESTERN HUB";
  const n_neighbors = searchParams.get("n_neighbors") || "5";
  const metric = searchParams.get("metric") || "cosine";

  const params = new URLSearchParams({ hub, n_neighbors, metric });
  if (target_date) params.set("target_date", target_date);

  const res = await fetch(`${PYTHON_API_URL}/like-day?${params}`, {
    method: "POST",
  });

  const data = await res.json();
  return NextResponse.json(data);
}
```

**Add to `.env.local`:** `PYTHON_API_URL=http://localhost:8000`

---

### Step 4: Add Like-Day UI to Frontend

**File:** `frontend/components/power/PjmLmpsHourlyTable.tsx` (modify existing)

Add a "Like Days" section to the existing LMP page:

1. **Trigger button** — "Find Like Days" button that calls `/api/pjm-like-day` with the current hub and a user-selected target date
2. **Results table** — Displays ranked like days: date, rank, distance, similarity score
3. **Chart integration** — When a like day is clicked, toggle its hourly profile on the existing Recharts chart (the hourly data is already available from the existing `/api/pjm-lmps-hourly` endpoint)

**UI mockup:**

```
┌─────────────────────────────────────────────────┐
│  Like Day Analysis                              │
│                                                 │
│  Target Date: [2026-02-23 ▼]                    │
│  Neighbors:   [5 ▼]   Metric: [cosine ▼]       │
│  [Find Like Days]                               │
│                                                 │
│  ┌──────┬──────┬──────────┬────────────┐        │
│  │ Rank │ Date │ Distance │ Similarity │        │
│  ├──────┼──────┼──────────┼────────────┤        │
│  │  1   │ 2/10 │  0.023   │   97.7%    │        │
│  │  2   │ 1/15 │  0.041   │   95.9%    │        │
│  │  3   │ 12/3 │  0.055   │   94.5%    │        │
│  │  4   │ 1/22 │  0.068   │   93.2%    │        │
│  │  5   │ 11/8 │  0.079   │   92.1%    │        │
│  └──────┴──────┴──────────┴────────────┘        │
│                                                 │
│  [Click a date to overlay its hourly profile]   │
└─────────────────────────────────────────────────┘
```

---

### Step 5: Return Hourly Profiles with Like-Day Results

**File:** `backend/src/pjm_like_day/pipeline.py` (extend)

After finding like days, also return the **hourly LMP profiles** for each like day and the target date. This lets the frontend chart them directly without a second API call:

```python
# After finding like_days, also grab the hourly profiles
like_dates = results["date"].tolist() + [target_date]
hourly_profiles = df[df["date"].isin(like_dates)]
```

**Response shape:**
```json
{
  "target_date": "2026-02-23",
  "hub": "WESTERN HUB",
  "metric": "cosine",
  "like_days": [
    {"date": "2026-02-10", "rank": 1, "distance": 0.023, "similarity": 0.977},
    ...
  ],
  "hourly_profiles": [
    {"date": "2026-02-23", "hour_ending": 1, "lmp_total": 32.5, ...},
    {"date": "2026-02-23", "hour_ending": 2, "lmp_total": 30.1, ...},
    ...
  ]
}
```

---

## File Changes Summary

| File | Action | Description |
|---|---|---|
| `backend/src/api.py` | **Create** | FastAPI app with `/like-day` endpoint |
| `backend/src/pjm_like_day/pipeline.py` | **Modify** | Accept `hub` param, return hourly profiles |
| `backend/src/pjm_like_day/configs.py` | **Modify** | Make hub configurable (not hardcoded) |
| `backend/src/pjm_like_day/data/lmps.py` | **Modify** | Accept hub parameter in pull() |
| `backend/pyproject.toml` | **Modify** | Add `fastapi`, `uvicorn` deps |
| `frontend/app/api/pjm-like-day/route.ts` | **Create** | Proxy route to Python API |
| `frontend/components/power/PjmLmpsHourlyTable.tsx` | **Modify** | Add like-day UI section |
| `frontend/.env.local` | **Modify** | Add `PYTHON_API_URL` |

---

## Running Locally

```bash
# Terminal 1: Python API
cd backend
uvicorn src.api:app --reload --port 8000

# Terminal 2: Next.js frontend
cd frontend
npm run dev
```

---

## Future Considerations

- **Production deployment:** Both services behind a reverse proxy (nginx/Azure App Service)
- **Phase 2+ features:** Just add new `data/` modules and feature columns — the API endpoint and frontend don't change structurally
- **Caching:** Cache like-day results in the Python service (target_date + hub + metric as cache key)
- **WebSocket:** If real-time updates are ever needed, FastAPI supports WebSockets natively
