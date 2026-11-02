-- Apply to an existing table in a transaction; existing forecast rows are preserved.
ALTER TABLE weather_forecasts
    DROP CONSTRAINT IF EXISTS weather_forecasts_data_type_check;
ALTER TABLE weather_forecasts
    ADD CONSTRAINT weather_forecasts_data_type_check
    CHECK (data_type IN ('forecast', 'historical_forecast'));

ALTER TABLE weather_forecasts
    DROP CONSTRAINT IF EXISTS weather_forecasts_pkey;
ALTER TABLE weather_forecasts
    ADD PRIMARY KEY (city, weather_time_local, timezone, data_type, fetched_at_utc);
