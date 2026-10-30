# Philippine Weather ETL Pipeline

A learning project for Python, SQL, and data engineering, progressing toward
a daily Apache Airflow DAG. The intended analytical output is a comparison of
weather across Manila, Cebu, and Davao.

## Current milestone: extract Manila's forecast

The project currently fetches one day's hourly forecast for Manila and saves
the original JSON response plus a separate metadata file. It uses only Python's
standard library: no third-party packages or API key are required.

PostgreSQL loading, transformations, and Airflow orchestration are future steps.

## Run

Install Python 3.10 or newer, then run from the repository directory:

```powershell
python extract_weather.py
```

On Windows, `py extract_weather.py` also works if the Python launcher is installed.
An internet connection is required. HTTP, network, timeout, and JSON errors cause
the script to exit with an error so a future orchestrator can detect failure.

Output appears in `data/raw/`:

```text
manila_forecast_<UTC timestamp>.json
manila_forecast_<UTC timestamp>.metadata.json
```

The metadata records the city, source request URL, and UTC fetch completion time.
Each run creates a new snapshot because forecasts for the same hour can change.
Generated snapshots are ignored by Git.

## Understanding the data

The `hourly` object contains parallel arrays. Values at the same position in
`time`, `temperature_2m`, `relative_humidity_2m`, and `precipitation` describe
one weather hour. The response's `hourly_units` object specifies the units.

Weather timestamps use `Asia/Manila`; filenames and fetch metadata use UTC.
The fetch time is not the model's forecast issue time. This endpoint supplies
modeled forecasts, not direct station observations.

## Learning roadmap

1. Extract and preserve a raw API response (current step).
2. Transform parallel arrays into validated rows.
3. Load PostgreSQL and handle repeated processing without duplicates.
4. Add date-based processing and recovery of missed data.
5. Schedule and monitor the pipeline using Apache Airflow.
6. Expand to three cities and build daily analytical summaries.

## Data source and attribution

Weather data provided by [Open-Meteo](https://open-meteo.com/).
See the [API documentation](https://open-meteo.com/en/docs) and
[terms and data attribution requirements](https://open-meteo.com/en/terms).
The free endpoint is intended for noncommercial use within its usage limits.
