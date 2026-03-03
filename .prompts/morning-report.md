# Morning Report Generator

Generate today's PJM morning commentary report. Follow these steps exactly:

## Step 1: Read all spec files

Read every file in `.skills/pjm-morning-commentary/` to load the report structure and formatting rules:
- `system.md` — Master report structure, general rules, data source mapping
- `commentary.md` — Opening narrative format and example
- `lmp_price_trends.md` — LMP/DART analysis format
- `load_analysis.md` — Load analysis format
- `fuel_mix_generation.md` — Fuel mix format
- `outages_supply.md` — Outages format
- `tie_flows_interchange.md` — Tie flows format

These specs are your **sole formatting instructions**. Follow them exactly.

## Step 2: Determine dates

- **Report date**: Today (use the system date)
- **Lookback window**: Previous 3 weekdays (skip weekends/holidays)
- **Forward outlook**: Next 3 days
- Format all dates as `Ddd Mmm-DD` in the report, but use ISO format for SQL queries

## Step 3: Query the database

Run all queries against schema `dbt_pjm_v1_2026_feb_19`. Use the MCP postgres tools. Run independent queries in parallel.

### 3a. Date metadata
```sql
SELECT date, day_of_week, is_weekend, is_nerc_holiday, is_onpeak_with_weekends_holidays
FROM dbt_pjm_v1_2026_feb_19.utils_v1_pjm_dates_daily
WHERE date BETWEEN '{lookback_start}' AND '{forward_end}'
ORDER BY date
```

### 3b. LMP daily (onpeak + flat, all key hubs)
```sql
SELECT date, hub, period, market,
       ROUND(lmp_total::numeric, 2) as lmp_total,
       ROUND(lmp_system_energy_price::numeric, 2) as system_energy,
       ROUND(lmp_congestion_price::numeric, 2) as congestion,
       ROUND(lmp_marginal_loss_price::numeric, 2) as losses
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_daily
WHERE date BETWEEN '{lookback_start}' AND '{today}'
  AND hub IN ('WESTERN HUB', 'DOMINION HUB', 'NEW JERSEY HUB', 'EASTERN HUB',
              'AEP-DAYTON HUB', 'CHICAGO HUB', 'N ILLINOIS HUB', 'ATSI GEN HUB', 'OHIO HUB')
  AND period IN ('onpeak', 'flat')
ORDER BY date, hub, market, period
```

### 3c. DA load daily (RTO)
```sql
SELECT date, region, period, ROUND(da_load::numeric, 0) as da_load_mw
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_da_daily
WHERE date BETWEEN '{lookback_start}' AND '{today}'
  AND region = 'RTO'
ORDER BY date, period
```

### 3d. RT load instantaneous hourly (RTO)
```sql
SELECT date, region, ROUND(rt_load_mw::numeric, 0) as rt_load_mw, hour_ending
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_instantaneous_hourly
WHERE date BETWEEN '{lookback_start}' AND '{today}'
  AND region = 'RTO'
ORDER BY date, hour_ending
```

### 3e. Load forecast (RTO, next 3 days)
```sql
SELECT forecast_date, region, hour_ending,
       ROUND(forecast_load_mw::numeric, 0) as forecast_mw
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
WHERE forecast_rank = 1
  AND forecast_date BETWEEN '{today}' AND '{forward_end}'
  AND region = 'RTO_COMBINED'
ORDER BY forecast_date, hour_ending
```

### 3f. Fuel mix daily
```sql
SELECT date, period,
       ROUND(gas::numeric, 0) as gas_mw,
       ROUND(coal::numeric, 0) as coal_mw,
       ROUND(nuclear::numeric, 0) as nuclear_mw,
       ROUND(wind::numeric, 0) as wind_mw,
       ROUND(solar::numeric, 0) as solar_mw,
       ROUND(total::numeric, 0) as total_mw,
       ROUND(gas_pct_thermal::numeric, 1) as gas_pct,
       ROUND(coal_pct_thermal::numeric, 1) as coal_pct
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_fuel_mix_daily
WHERE date BETWEEN '{lookback_start}' AND '{today}'
ORDER BY date, period
```

### 3g. Outages actual
```sql
SELECT date, region,
       ROUND(total_outages_mw::numeric, 0) as total_mw,
       ROUND(forced_outages_mw::numeric, 0) as forced_mw,
       ROUND(planned_outages_mw::numeric, 0) as planned_mw,
       ROUND(maintenance_outages_mw::numeric, 0) as maintenance_mw
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_outages_actual_daily
WHERE date BETWEEN '{lookback_start}' AND '{today}'
ORDER BY date, region
```

### 3h. Outages forecast (7-day)
```sql
SELECT forecast_date, region,
       ROUND(total_outages_mw::numeric, 0) as total_mw,
       ROUND(forced_outages_mw::numeric, 0) as forced_mw,
       ROUND(planned_outages_mw::numeric, 0) as planned_mw,
       ROUND(maintenance_outages_mw::numeric, 0) as maintenance_mw
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_outages_forecast_daily
WHERE forecast_rank = 1
  AND region = 'RTO'
  AND forecast_date BETWEEN '{today}' AND '{forward_end_7d}'
ORDER BY forecast_date
```

### 3i. Tie flows daily (key interfaces)
```sql
SELECT date, tie_flow_name, period,
       ROUND(actual_mw::numeric, 0) as actual_mw,
       ROUND(scheduled_mw::numeric, 0) as scheduled_mw
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_tie_flows_daily
WHERE date BETWEEN '{lookback_start}' AND '{today}'
  AND tie_flow_name IN ('PJM RTO', 'PJM MISO', 'NYIS', 'TVA', 'DUKE')
  AND period = 'flat'
ORDER BY date, tie_flow_name
```

## Step 4: Analyze the data

Before writing, compute these key metrics:

1. **DA price trend**: Direction and magnitude at Western Hub and Dominion Hub onpeak over the lookback window
2. **DART spreads**: Rank all hubs by onpeak DART for the most recent complete day. Decompose into system energy vs congestion vs losses.
3. **DA vs RT load error**: `(DA onpeak - RT instantaneous avg HE8-23) / RT avg × 100` for the most recent complete day. Note which RT source used.
4. **Forecast accuracy**: Compare today's forecast vs actual RT instantaneous for available hours
5. **Fuel mix shift**: Gas % of thermal trend, absolute gas/coal MW, wind/solar changes
6. **Outage trend**: Forced outage direction over lookback, 7-day forward outlook
7. **Net interchange**: PJM RTO direction and magnitude, MISO scheduled vs actual deviation

## Step 5: Write the report

Generate the report following the spec files from Step 1. Write it to:

```
morning-report/(YYYY-MM-DD) DDD MMM-DD.md
```

Example filename: `(2026-02-26) Thu Feb-26.md`

The report must have this structure:
```markdown
# PJM Morning Commentary — Ddd Mmm-DD

## Commentary
[3-5 sentences per commentary.md]

## LMP Price Trends & DART Analysis
[Price trend lead + numbered DART points per lmp_price_trends.md]

## Load Analysis
[Per load_analysis.md format]

## Fuel Mix & Generation Stack
[Per fuel_mix_generation.md format]

## Outages & Supply Availability
[Per outages_supply.md format]

## Tie Flows & Interchange
[Per tie_flows_interchange.md format]
```

## Critical Rules

- **Data integrity**: Only use numbers from the query results. Never invent figures.
- **Precision**: $/MWh with 2 decimals, MW with no decimals, +/- on DART spreads.
- **Date format**: `Ddd Mmm-DD` everywhere in the report narrative.
- **Hub priority**: Lead with Western Hub and Dominion Hub, others for comparison.
- **Tone**: Direct, no filler. Written for experienced PJM power traders at Helios CTA.
- **Bold**: Key figures, directional indicators, and notable anomalies.
- **Weekend/holiday awareness**: Flag when upcoming days trade flat. Flag if lookback includes flat days.
