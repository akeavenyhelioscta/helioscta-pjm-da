-- Meteologica generation forecast DA cutoff vintages.
-- Selects 4 vintages anchored to the DA cutoff execution:
--   DA Cutoff (0h), DA -12h, DA -24h, DA -48h.
-- For each vintage, picks the highest forecast_rank per (forecast_date, hour_ending)
-- where forecast_date >= today.
-- Parameterized by {region} (default RTO) and {source} (e.g. 'solar', 'wind').
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region,
        case
            when left('{source}', 1) = chr(123) and right('{source}', 1) = chr(125) then 'solar'
            else coalesce(nullif('{source}', ''), 'solar')
        end::text as source,
        case
            when left('{da_cutoff_time}', 1) = chr(123) and right('{da_cutoff_time}', 1) = chr(125) then '10:00:00'
            else coalesce(nullif('{da_cutoff_time}', ''), '10:00:00')
        end::text as da_cutoff_time
),
da_targets as (
    select *
    from (values
        ('DA Cutoff',  0),
        ('DA -12h',   12),
        ('DA -24h',   24),
        ('DA -48h',   48)
    ) as t(vintage_label, vintage_offset_hours)
),
da_anchor as (
    select
        max(forecast_execution_datetime) as da_cutoff_execution_datetime
    from meteologica_cleaned.meteologica_pjm_generation_forecast_hourly
    where region = (select region from params)
      and source = (select source from params)
      and forecast_execution_datetime::time < (select da_cutoff_time from params)::time
),
resolved_cutoffs as (
    select
        t.vintage_label,
        t.vintage_offset_hours,
        max(src.forecast_execution_datetime) as vintage_anchor_execution_datetime
    from da_targets t
    cross join da_anchor a
    left join meteologica_cleaned.meteologica_pjm_generation_forecast_hourly src
        on src.region = (select region from params)
       and src.source = (select source from params)
       and a.da_cutoff_execution_datetime is not null
       and src.forecast_execution_datetime <= (
           a.da_cutoff_execution_datetime - (t.vintage_offset_hours || ' hours')::interval
       )
    group by t.vintage_label, t.vintage_offset_hours
),
ranked as (
    select
        c.vintage_label,
        c.vintage_anchor_execution_datetime,
        d.forecast_date,
        d.hour_ending,
        d.forecast_generation_mw,
        d.forecast_execution_datetime,
        d.forecast_datetime,
        d.forecast_rank,
        row_number() over (
            partition by c.vintage_label, d.forecast_date, d.hour_ending
            order by d.forecast_rank desc
        ) as rn
    from resolved_cutoffs c
    join meteologica_cleaned.meteologica_pjm_generation_forecast_hourly d
        on d.region = (select region from params)
       and d.source = (select source from params)
       and c.vintage_anchor_execution_datetime is not null
       and d.forecast_execution_datetime <= c.vintage_anchor_execution_datetime
       and d.forecast_date >= current_date
)
select
    forecast_datetime,
    forecast_date,
    hour_ending,
    forecast_rank,
    forecast_generation_mw,
    forecast_execution_datetime,
    vintage_label,
    vintage_anchor_execution_datetime
from ranked
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc
