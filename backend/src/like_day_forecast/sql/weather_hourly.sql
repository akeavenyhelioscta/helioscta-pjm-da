with params as (
    select
        case
            when left('{station}', 1) = chr(123) and right('{station}', 1) = chr(125) then 'PJM'
            else coalesce(nullif('{station}', ''), 'PJM')
        end::text as station,
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, date '2020-01-01') as start_date,
        coalesce(nullif(regexp_replace('{forecast_start_date}', '[^0-9-]', '', 'g'), '')::date, current_date) as forecast_start_date
),
observed as (
    select
        o.date
        ,o.hour_ending
        ,o.station_name
        ,o.temperature as temp
    from wsi_cleaned.temp_observed_hourly o
    where o.station_name = (select station from params)
      and o.date >= (select start_date from params)
),
forecast as (
    select
        f.date
        ,f.hour_ending
        ,f.station_name
        ,f.temperature as temp
    from wsi_cleaned.temp_forecast_hourly f
    where f.station_name = (select station from params)
      and f.date >= (select forecast_start_date from params)
)
select * from observed
union all
select * from forecast
order by date, hour_ending
