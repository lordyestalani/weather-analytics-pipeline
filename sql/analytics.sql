-- Keep one current value per city, local weather timestamp, timezone, and source type.
-- Raw snapshot history remains in weather_forecasts.
CREATE OR REPLACE VIEW latest_weather_hourly AS
SELECT DISTINCT ON (city, weather_time_local, timezone, data_type)
    city,
    weather_time_local,
    timezone,
    data_type,
    temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    fetched_at_utc,
    source_file
FROM weather_forecasts
ORDER BY city, weather_time_local, timezone, data_type, fetched_at_utc DESC;

-- Metrics cover the available latest hours; require is_complete_day for full-day comparisons.
-- This project's configured Philippine cities all have 24-hour local calendar days.
CREATE OR REPLACE VIEW daily_weather_summary AS
SELECT
    city,
    weather_time_local::date AS weather_date,
    timezone,
    data_type,
    COUNT(*) AS hours_available,
    (COUNT(*) = 24 AND BOOL_AND(weather_time_local = date_trunc('hour', weather_time_local)))
        AS is_complete_day,
    ROUND(AVG(temperature_c)::numeric, 2) AS average_temperature_c,
    ROUND(MIN(temperature_c)::numeric, 2) AS minimum_temperature_c,
    ROUND(MAX(temperature_c)::numeric, 2) AS maximum_temperature_c,
    ROUND(AVG(relative_humidity_pct)::numeric, 2) AS average_humidity_pct,
    ROUND(SUM(precipitation_mm)::numeric, 2) AS total_precipitation_mm,
    COUNT(DISTINCT fetched_at_utc) AS selected_snapshot_count,
    MAX(fetched_at_utc) AS data_as_of_utc
FROM latest_weather_hourly
GROUP BY city, weather_time_local::date, timezone, data_type;
