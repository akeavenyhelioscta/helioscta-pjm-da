--------------------
-- Load Forecast
--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
ORDER BY region

--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_gridstatus_pjm_load_forecast_hourly
ORDER BY region

--------------------
-- DA Load
--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_da_hourly
ORDER BY region

--------------------
-- RT Load
--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_instantaneous_hourly
ORDER BY region

--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly
ORDER BY region

--------------------

-- MIDATL
-- RTO
-- SOUTH
-- WEST

select distinct region
from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_prelim_hourly
ORDER BY region