WITH observed AS (
    SELECT
        date,
        hour_ending,
        station_name,
        temperature as temp
    FROM {schema}.temp_observed_hourly
    WHERE station_name = '{station}'
        AND date >= '{start_date}'
),

forecast AS (
    SELECT
        date,
        hour_ending,
        station_name,
        temperature as temp
    FROM {schema}.temp_forecast_hourly
    WHERE station_name = '{station}'
        AND date >= CURRENT_DATE
)

SELECT * FROM observed
UNION ALL
SELECT * FROM forecast
ORDER BY date, hour_ending
