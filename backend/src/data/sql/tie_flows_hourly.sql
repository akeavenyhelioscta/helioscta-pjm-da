with params as (
    select
        case
            when left('{tie_flow_name}', 1) = chr(123) and right('{tie_flow_name}', 1) = chr(125) then null
            else nullif('{tie_flow_name}', '')
        end::text as tie_flow_name,
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2023-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    t.datetime
    ,t.date
    ,t.hour_ending
    ,t.tie_flow_name
    ,t.actual_mw
    ,t.scheduled_mw
from pjm_cleaned.pjm_tie_flows_hourly t
cross join params p
where (p.tie_flow_name is null or t.tie_flow_name = p.tie_flow_name)
  and (p.start_date is null or t.date >= p.start_date)
  and (p.end_date is null or t.date <= p.end_date)
order by t.date, t.hour_ending, t.tie_flow_name
