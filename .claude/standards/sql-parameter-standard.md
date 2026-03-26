# Like-Day SQL Parameter Standard

This directory uses a common pattern so SQL files can define defaults in SQL and allow Python to override them.

## Required pattern

1. Start each query with a top-level `params` CTE.
2. Define user-facing parameters in `params` using SQL defaults:
   - Text (SQL-editor-safe): `case when left('{param}', 1) = chr(123) and right('{param}', 1) = chr(125) then 'default' else coalesce(nullif('{param}', ''), 'default') end::text`
   - Integer (SQL-editor-safe): `coalesce(nullif(regexp_replace('{param}', '[^0-9-]', '', 'g'), '')::int, 30)`
   - Boolean: `case when lower('{param}') = 'true' then true when lower('{param}') = 'false' then false else false end`
   - Date (SQL-editor-safe): `coalesce(nullif(regexp_replace('{param}', '[^0-9-]', '', 'g'), '')::date, date '2020-01-01')`
3. Reference those values from the query body via scalar subselects or `cross join params`.
4. For SQL-editor compatibility, avoid unresolved identifier placeholders in `FROM` clauses.
   - Use fixed default schemas directly in SQL (for example `pjm_cleaned`, `wsi_cleaned`).
   - Python overrides should target value parameters in `params`.

## Python renderer

All like-day SQL loaders should render templates via:

- `src.like_day_forecast.utils.sql_templates.render_sql_template`
- Data pull functions should expose `sql_overrides` so callers can pass
  ad-hoc parameter overrides without editing SQL.

Renderer behavior:

- Missing template keys are rendered as `''` (empty string).
- SQL defaults in `params` then take effect automatically.
- Any provided override in Python replaces the default.

## Example

```sql
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region,
        coalesce(nullif(regexp_replace('{lookback_days}', '[^0-9-]', '', 'g'), '')::int, 30) as lookback_days
)
select *
from pjm_cleaned.pjm_load_forecast_hourly
where region = (select region from params)
  and forecast_date >= current_date - (select lookback_days from params)
```

## Notes

- Timezone-sensitive logic should expose a `timezone` parameter with default `America/New_York`.
- Strip queries that may include already-passed hours should expose `include_passed_hours`.
- Preserve backward compatibility by keeping existing Python function arguments and mapping them to template overrides.
- This standard is designed to run both:
  - through Python template rendering, and
  - directly in a SQL editor with defaults.
