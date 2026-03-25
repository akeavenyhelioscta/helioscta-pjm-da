-- RT metered actuals for a date range (ramp comparison window, RTO only)
-- Placeholders: {history_start}, {history_end}
SELECT DISTINCT
    datetime,
    date,
    hour_ending,
    region,
    rt_load_mw
FROM pjm_cleaned.pjm_load_rt_metered_hourly
WHERE date BETWEEN '{history_start}' AND '{history_end}'
    AND region = 'RTO'
ORDER BY date, hour_ending;
