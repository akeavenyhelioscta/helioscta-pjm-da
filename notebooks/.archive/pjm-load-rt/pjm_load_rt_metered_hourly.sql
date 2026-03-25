select  

    datetime
    ,date
    ,hour_ending
    ,region
    ,rt_load_mw

from dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly

WHERE
  forecast_date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::date - 7

ORDER BY datetime desc, region;
