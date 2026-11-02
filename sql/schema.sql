-- One row represents one weather hour in one fetched forecast snapshot.
CREATE TABLE IF NOT EXISTS weather_forecasts (
    city TEXT NOT NULL CHECK (city <> ''),
    weather_time_local TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    timezone TEXT NOT NULL CHECK (timezone <> ''),
    temperature_c DOUBLE PRECISION NOT NULL
        CHECK (temperature_c > '-Infinity'::DOUBLE PRECISION AND temperature_c < 'Infinity'::DOUBLE PRECISION),
    relative_humidity_pct SMALLINT NOT NULL CHECK (relative_humidity_pct BETWEEN 0 AND 100),
    precipitation_mm DOUBLE PRECISION NOT NULL
        CHECK (precipitation_mm >= 0 AND precipitation_mm < 'Infinity'::DOUBLE PRECISION),
    fetched_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    data_type TEXT NOT NULL CONSTRAINT weather_forecasts_data_type_check CHECK (data_type IN ('forecast', 'historical_forecast')),
    source_file TEXT NOT NULL CHECK (source_file <> ''),
    PRIMARY KEY (city, weather_time_local, timezone, data_type, fetched_at_utc)
);
