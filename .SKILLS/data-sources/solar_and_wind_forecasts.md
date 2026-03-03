# Solar & Wind Forecasts

## Data Source: Solar Forecast

**Table:** `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_solar_forecast_hourly`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `forecast_rank` | bigint | Rank of the forecast (1 = latest/best) |
| `forecast_execution_datetime` | timestamp | When the forecast was generated |
| `forecast_execution_date` | date | Date the forecast was generated |
| `forecast_datetime` | timestamp | Target datetime being forecasted |
| `forecast_date` | date | Target date being forecasted |
| `hour_ending` | numeric | Hour ending (1-24) |
| `solar_forecast` | numeric | Grid-scale solar forecast (MW) |
| `solar_forecast_btm` | numeric | Behind-the-meter solar forecast (MW) |

### Query: Latest Forecast

```sql
SELECT *
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_solar_forecast_hourly
WHERE forecast_execution_datetime = (
  SELECT MAX(forecast_execution_datetime)
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_solar_forecast_hourly
)
ORDER BY hour_ending;
```

### Sample Data (2026-02-27, executed 2026-02-26 20:00 UTC, rank 1)

- **Peak grid-scale solar:** ~9,839 MW at HE 13
- **Peak BTM solar:** ~3,953 MW at HE 13
- **Solar window:** HE 7 through HE 19 (ramps up HE 7-10, plateaus HE 11-16, drops off HE 17-19)
- **Nighttime hours (HE 1-6, 20-24):** 0 MW for both grid-scale and BTM

### Notes

- `forecast_rank = 1` represents the most recent/primary forecast for a given target date
- Multiple forecast executions may exist per day (different `forecast_execution_datetime` values); use `MAX()` to get the latest
- BTM values are generally smaller than grid-scale but follow the same diurnal pattern
- Timestamps appear to be in UTC (e.g., HE 7 = `forecast_datetime` 13:00 UTC = ~8 AM ET in winter)

---

## Data Source: Wind Forecast

**Table:** `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_wind_forecast_hourly` (assumed, not yet queried)

_TODO: Query and document schema and sample data._
