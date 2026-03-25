from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.like_day_forecast import configs
from src.like_day_forecast.pipeline import run

import logging
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Helios CTA - PJM Like Day API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

VALID_MARKETS = {"da", "rt", "dart"}
VALID_COLS = set(configs.FEATURE_COLS)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/like-day")
def like_day(
    target_date: date | None = Query(default=None, description="Target date (YYYY-MM-DD). Defaults to tomorrow."),
    hub: str = Query(default="WESTERN HUB", description="PJM pricing hub"),
    market: str = Query(default="da", description="Deprecated fallback market if features not provided."),
    n_neighbors: int = Query(default=5, ge=1, le=20, description="Number of like days to return"),
    metric: str = Query(default="cosine", description="Distance metric: cosine, euclidean, or manhattan"),
    hist_start: Optional[date] = Query(default=None, description="Start of historical window (YYYY-MM-DD)"),
    hist_end: Optional[date] = Query(default=None, description="End of historical window (YYYY-MM-DD)"),
    hours: Optional[str] = Query(default=None, description="Comma-separated hours to include (1-24). All if omitted."),
    days_of_week: Optional[str] = Query(default=None, description="Comma-separated days of week (0=Sun..6=Sat). All if omitted."),
    months: Optional[str] = Query(default=None, description="Comma-separated months (1-12). All if omitted."),
    features: Optional[str] = Query(default=None, description="Comma-separated market.column:weight, e.g. da.lmp_total:1,rt.lmp_total:0.5"),
    # Deprecated params kept for backward compat
    feature_cols: Optional[str] = Query(default=None, description="Deprecated. Use features instead."),
    feature_weights: Optional[str] = Query(default=None, description="Deprecated. Use features instead."),
):
    if target_date is None:
        target_date = datetime.now().date() + timedelta(days=1)

    # Parse comma-separated filter lists
    hours_list = [int(h) for h in hours.split(",")] if hours else None
    days_list = [int(d) for d in days_of_week.split(",")] if days_of_week else None
    months_list = [int(m) for m in months.split(",")] if months else None

    # Parse features param: market.column:weight format
    features_list: list[dict] | None = None
    if features:
        features_list = []
        for entry in features.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            spec, weight_str = entry.rsplit(":", 1)
            if "." not in spec:
                continue
            mkt, col = spec.split(".", 1)
            mkt = mkt.strip().lower()
            col = col.strip()
            if mkt in VALID_MARKETS and col in VALID_COLS:
                try:
                    features_list.append({"market": mkt, "column": col, "weight": float(weight_str)})
                except ValueError:
                    pass
        if not features_list:
            features_list = None

    # Backward compat: build features_list from deprecated params
    if features_list is None:
        if feature_weights:
            features_list = []
            for pair in feature_weights.split(","):
                pair = pair.strip()
                if ":" in pair:
                    col, w = pair.split(":", 1)
                    col = col.strip()
                    if col in VALID_COLS:
                        try:
                            features_list.append({"market": market, "column": col, "weight": float(w)})
                        except ValueError:
                            pass
            if not features_list:
                features_list = None
        elif feature_cols:
            cols = [c.strip() for c in feature_cols.split(",") if c.strip() in VALID_COLS]
            if cols:
                features_list = [{"market": market, "column": c, "weight": 1.0} for c in cols]

    # Default: DA Total LMP
    if features_list is None:
        features_list = [{"market": market, "column": "lmp_total", "weight": 1.0}]

    try:
        output = run(
            target_date=target_date,
            hub=hub,
            features=features_list,
            n_neighbors=n_neighbors,
            metric=metric,
            hist_start=hist_start,
            hist_end=hist_end,
            hours=hours_list,
            days_of_week=days_list,
            months=months_list,
        )
    except Exception as e:
        logging.error(f"Like-day pipeline failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

    like_days_df = output["like_days"]
    hourly_df = output["hourly_profiles"]

    # Convert date objects to strings for JSON serialization
    like_days_records = like_days_df.copy()
    like_days_records[configs.DATE_COL] = like_days_records[configs.DATE_COL].astype(str)

    hourly_records = hourly_df.copy()
    hourly_records[configs.DATE_COL] = hourly_records[configs.DATE_COL].astype(str)

    return {
        "target_date": str(target_date),
        "hub": hub,
        "metric": metric,
        "n_neighbors": n_neighbors,
        "like_days": like_days_records.to_dict(orient="records"),
        "hourly_profiles": hourly_records.to_dict(orient="records"),
    }


def _serialize_df(df: pd.DataFrame) -> list[dict]:
    """Serialize a DataFrame to JSON-safe records (NaN → None, dates → str)."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].dtype == object:
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, date) else v)
    return df.where(df.notna(), None).to_dict(orient="records")


@app.post("/like-day-forecast")
def like_day_forecast(
    forecast_date: Optional[str] = Query(
        default=None,
        description="Date to forecast (YYYY-MM-DD). Defaults to tomorrow.",
    ),
    n_analogs: int = Query(
        default=30, ge=5, le=100,
        description="Number of analog days to find.",
    ),
    weight_method: str = Query(
        default="inverse_distance",
        description="Weighting method: inverse_distance, softmax, rank, uniform.",
    ),
    season_window_days: int = Query(
        default=30, ge=7, le=180,
        description="Season proximity window in days for pre-filtering.",
    ),
    same_dow_group: bool = Query(
        default=True,
        description="Whether to enforce same day-of-week group filtering.",
    ),
    apply_calendar_filter: bool = Query(
        default=True,
        description="Whether to apply calendar (DOW + season) filtering.",
    ),
    apply_regime_filter: bool = Query(
        default=True,
        description="Whether to apply LMP/gas regime filtering.",
    ),
):
    # Lazy import to avoid settings.py side effects at startup
    from src.like_day_forecast.pipelines.forecast import run as run_forecast
    from src.like_day_forecast.configs import ScenarioConfig

    scenario_config = ScenarioConfig(
        forecast_date=forecast_date,
        n_analogs=n_analogs,
        weight_method=weight_method,
        season_window_days=season_window_days,
        same_dow_group=same_dow_group,
        apply_calendar_filter=apply_calendar_filter,
        apply_regime_filter=apply_regime_filter,
    )

    try:
        result = run_forecast(config=scenario_config)
    except Exception as e:
        logging.error(f"Like-day forecast pipeline failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    # Serialize DataFrames
    output_table = _serialize_df(result["output_table"])
    quantiles_table = _serialize_df(result["quantiles_table"])
    analogs = _serialize_df(result["analogs"])

    # Build fan chart data from df_forecast
    fan_chart = []
    df_fc = result["df_forecast"]
    for _, row in df_fc.iterrows():
        point = {"hour_ending": int(row["hour_ending"]), "point_forecast": row["point_forecast"]}
        for col in df_fc.columns:
            if col.startswith("q_"):
                val = row[col]
                point[col] = None if (isinstance(val, float) and np.isnan(val)) else val
        fan_chart.append(point)

    return {
        "forecast_date": result["forecast_date"],
        "reference_date": result["reference_date"],
        "has_actuals": result["has_actuals"],
        "n_analogs_used": result["n_analogs_used"],
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "fan_chart": fan_chart,
        "analogs": analogs,
        "metrics": result["metrics"],
    }
