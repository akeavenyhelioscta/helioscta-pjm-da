# Load Forecasts

I think we cuffed it of 7-8am MOUNTAIN TOME

---

PJM Load Forecast — DA Cutoff Analysis Notebook                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         

 Context

 We want to evaluate how PJM load forecasts evolve leading up to the DA market window, and whether forecast revisions correlate with DA pricing outcomes. The key insight: the last load forecast before 7-8am MST (9am EST) is what a trader would use for DA bidding. Comparing this to forecasts from 12/24/48hr earlier reveals the direction and magnitude of revisions.

 Approach: Hybrid (SQL CTEs in .sql file, notebook for analysis)

 Why not dbt: Only ~2 weeks of forecast snapshot data exists. This is exploratory — we need to validate the cutoff logic and understand data patterns before committing to a permanent view. The SQL is structured so it can be promoted to dbt later once stabilized.

 Files to Create

 notebooks/pjm-load-forecast/
     pjm_load_forecast_da_cutoff.sql      # SQL with layered CTEs
     pjm_load_forecast_da_cutoff.ipynb    # Analysis notebook

 SQL Logic (pjm_load_forecast_da_cutoff.sql)

 Layered CTEs:

 1. forecast_raw — Pull all vintages for last 14 days from staging_v1_pjm_load_forecast_hourly
 2. cutoff_vintages — For each forecast_date, find MAX(forecast_execution_datetime) where execution is before forecast_date 09:00:00 (EPT = 9am EST = 7am MST)
 3. da_cutoff_forecast — Join back to get the full hourly forecast at that cutoff vintage
 4. lookback_*_vintage — For 12h/24h/48h: find MAX(forecast_execution_datetime) at or before cutoff - interval
 5. combined — Join cutoff + all 3 lookbacks on (forecast_date, hour_ending, region)
 6. with_deltas — Compute cutoff_load_mw - load_mw_Xh for each lookback

 Notebook Sections

 1. Setup & Data Pull

 - Imports (pandas, plotly, pull_from_db) — all in cell 1
 - Read SQL file, execute, print row count and date range

 2. Data Validation — Cutoff Vintage Inspection

 - Show actual cutoff execution timestamps per date (verify they land before 9am EST)
 - Bar chart of cutoff hour-of-day
 - Lookback coverage table (which dates have 12h/24h/48h data)

 3. Forecast Evolution — Cutoff vs Lookbacks

 - Per-region line charts for the latest forecast_date: 48h → 24h → 12h → cutoff overlaid by hour_ending
 - Older vintages shown with dashed/faded lines, cutoff as solid bold

 4. Forecast Deltas — MW Changes at Each Lookback

 - Grouped bar charts per region: delta_12h, delta_24h, delta_48h by hour_ending
 - Heatmap: 24hr delta across all dates × hours for RTO (RdBu diverging colorscale, 0 midpoint)

 5. DA LMP Connection

 - Pull DA LMPs for all hubs (market = 'da') and DA cleared load (RTO) for same 14-day window via inline SQL queries
 - Merge with cutoff forecast on (forecast_date, hour_ending)
 - Compute forecast_error_mw = cutoff_load_mw - da_load_mw
 - Scatter: 24hr forecast revision vs DA LMP, faceted/colored by hub
 - Forecast accuracy: cutoff forecast vs DA cleared load (scatter + 45-degree line + error histogram)
 - Summary stats: MAE, RMSE, Bias

 6. Summary & Next Steps

 - Findings and whether to promote SQL to dbt

 Key Design Decisions

 - Timezone: forecast_execution_datetime is EPT. Cutoff filter < forecast_date 09:00:00 works directly in EPT (9am EST = 7am MST winter, 9am EDT = 7am MDT summer)
 - LMP hubs: All hubs included — analysis faceted by hub to show how load forecast revisions impact different pricing nodes
 - 48hr lookback: Will often be NULL due to limited data window — surfaced explicitly in coverage check
 - Reuse: pull_from_db() from backend/src/utils/azure_postgresql.py, same pattern as existing notebooks

 Verification

 - Run all cells top-to-bottom after kernel restart
 - Validate cutoff timestamps are consistently before 9am EST
 - Confirm lookback coverage (expect good 12h/24h, sparse 48h)
 - Check merged dataset has non-null LMP and DA load columns for overlapping dates