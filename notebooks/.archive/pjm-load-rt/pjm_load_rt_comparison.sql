-- Pull all 3 RT load types for a single target date
-- {target_date} is replaced by Python before execution
WITH rt_metered AS (
  SELECT DISTINCT
    datetime, date, hour_ending, region, rt_load_mw,
    'metered' AS load_type
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly
  WHERE date = '{target_date}'::date
),
rt_prelim AS (
  SELECT DISTINCT
    datetime, date, hour_ending, region, rt_load_mw,
    'prelim' AS load_type
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_prelim_hourly
  WHERE date = '{target_date}'::date
),
rt_instantaneous AS (
  SELECT DISTINCT
    datetime, date, hour_ending, region, rt_load_mw,
    'instantaneous' AS load_type
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_instantaneous_hourly
  WHERE date = '{target_date}'::date
)

SELECT * FROM rt_metered
UNION ALL
SELECT * FROM rt_prelim
UNION ALL
SELECT * FROM rt_instantaneous
ORDER BY hour_ending ASC, region, load_type;
