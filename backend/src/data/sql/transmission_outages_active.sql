with params as (
    select
        coalesce(nullif(regexp_replace('{reference_date}', '[^0-9-]', '', 'g'), '')::date, CURRENT_DATE) as reference_date
)
-- Active/Approved outages
select
    t.ticket_id,
    t.zone,
    t.facility_name,
    t.equipment_type,
    t.voltage_kv::int as voltage_kv,
    t.start_datetime,
    t.end_datetime,
    t.outage_state,
    t.risk,
    t.cause,
    t.last_revised,
    extract(day from (now() - t.start_datetime))::int as days_out,
    case
        when t.end_datetime < now() then null
        else extract(day from (t.end_datetime - now()))::int
    end as days_to_return
from pjm.transmission_outages t
cross join params p
where t.outage_state in ('Active', 'Approved')
  and t.equipment_type in ('LINE', 'XFMR', 'PS')
  and t.voltage_kv >= 230

union all

-- Recently cancelled outages (last 7 days)
select
    t.ticket_id,
    t.zone,
    t.facility_name,
    t.equipment_type,
    t.voltage_kv::int as voltage_kv,
    t.start_datetime,
    t.end_datetime,
    t.outage_state,
    t.risk,
    t.cause,
    t.last_revised,
    null::int as days_out,
    null::int as days_to_return
from pjm.transmission_outages t
cross join params p
where t.outage_state = 'Cancelle'
  and t.equipment_type in ('LINE', 'XFMR', 'PS')
  and t.voltage_kv >= 230
  and t.last_revised >= now() - interval '7 days'

order by voltage_kv desc, start_datetime desc
