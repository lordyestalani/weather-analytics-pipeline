# Philippine Weather Analytics Pipeline

A learning project for Python, SQL, and data engineering, progressing toward a daily Apache Airflow DAG. The intended analytical output is a comparison of weather across Manila, Cebu, and Davao.

## Current milestone: daily ETL and analytical summaries for three cities

```text
Open-Meteo API -> raw JSON -> validated CSV -> PostgreSQL
                  extract      transform       load
```

Airflow orchestrates a daily extract -> transform -> load DAG in Docker. The
ETL modules can also run directly from your terminal. Date-based recovery is
supported for all three cities. PostgreSQL views provide latest-hourly data and
daily analytical summaries.

## Requirements

- Python 3.10 or newer (developed with Python 3.11).
- Docker Desktop running with Linux containers and Docker Compose.
- An internet connection for extraction and initial dependency installation.

The transformer uses Python's standard library. The extractor uses Requests for
HTTP requests and default certificate verification. The loader uses
[Psycopg](https://www.psycopg.org/psycopg3/docs/basic/usage.html) to connect to PostgreSQL
and python-dotenv to read local configuration.

## Setup (PowerShell)

Run from the repository directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

For a fresh checkout, create local configuration:

```powershell
Copy-Item .env.example .env
```

Set `WEATHER_DB_PASSWORD` in `.env` to your own local password before starting the
database. If `.env` already exists, keep it. The initial setup in this project
already generated a local password. `.env` is ignored by Git; `.env.example`
contains placeholders. Environment variables override the file's values.

### Configure Airflow environment variables

The current local `.env` already contains generated Airflow settings. On a fresh
checkout, fill in the Airflow settings from `.env.example`:

- `AIRFLOW_ADMIN_PASSWORD`: your local UI password for username `admin`.
- `AIRFLOW_DB_PASSWORD`: a separate metadata database password. Use letters and
  numbers or generate it with the JWT command below, since it is part of a URL.
- `AIRFLOW_FERNET_KEY`: generate a valid Fernet key with:

  ```powershell
  python -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
  ```

- `AIRFLOW_JWT_SECRET`: generate a random signing secret with:

  ```powershell
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

Keep these values stable across restarts. They are shared between Airflow components.
`AIRFLOW_WEB_PORT` defaults to 8080. Keep actual secrets in `.env`, not in Git.

Compose validates all required variables before selecting services. Complete these
settings before any Compose command, including weather-db-only startup. If you
already have a .env, add the new keys from .env.example and preserve your existing
weather database credentials.

Start PostgreSQL and wait for it to be ready:

```powershell
docker compose up -d --wait --wait-timeout 60 weather-db
docker compose ps
```

The local connection defaults are host `localhost`, port `5433`, database `weather`,
and user `weather`. Docker maps your local port 5433 to the container's port 5432.
The service binds to localhost. You can change the host port in `.env` if needed.

## Airflow setup (local development)

This setup pins Airflow 3.3.2 with Python 3.12 and uses LocalExecutor. The official
[Docker guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html)
provides the component layout; this project omits Celery and Redis because the
scheduler executes tasks locally. This is a learning environment bound to localhost.

| Service | Purpose |
| --- | --- |
| `weather-db` | Stores weather records; reachable locally on port 5433 |
| `airflow-db` | Stores Airflow's internal metadata; no published host port |
| `airflow-init` | Runs migrations and synchronizes the UI password, then exits successfully |
| `airflow-api-server` | Serves the UI and APIs at localhost:8080 |
| `airflow-scheduler` | Schedules tasks and runs them using LocalExecutor |
| `airflow-dag-processor` | Parses the Python files in `dags/` |

### Start Airflow

After completing the environment-variable setup above, build and start Airflow.

Build the shared image and start the services:

```powershell
docker compose build airflow-api-server
docker compose up -d --wait --wait-timeout 120 airflow-api-server airflow-scheduler airflow-dag-processor weather-db
docker compose ps -a
```

The dependency chain starts `airflow-db`, waits for readiness, runs `airflow-init`,
then starts the API server, scheduler, and DAG processor. `airflow-init` showing
`Exited (0)` is expected. First startup can take a few minutes. Docker should have
at least 4 GB of memory available, preferably 8 GB.

Open [http://localhost:8080](http://localhost:8080). The username is `admin` and
the password is `AIRFLOW_ADMIN_PASSWORD` from your local `.env`.

`airflow-init` writes that value to the password file used by SimpleAuthManager.
To change the password, edit `AIRFLOW_ADMIN_PASSWORD` in `.env`, then run:

```powershell
docker compose run --rm airflow-init
```

The next login uses the updated password; an API restart is not required.
Do not commit `.env` or publish its values. SimpleAuthManager is intended for
local development and testing; its password file remains in the
`airflow_auth` named volume.

### Shared files and dependencies

- `dags/` is mounted read-only at `/opt/airflow/dags` and contains `weather_etl_dag.py`.
- `weather_etl/` and `sql/` are mounted read-only under `/opt/airflow/project`.
- `data/` is shared read/write at `/opt/airflow/project/data`.
- `PYTHONPATH` makes the pipeline package importable inside each Airflow service.
- The Dockerfile installs `requirements.txt` at build time while keeping Airflow pinned.
- Airflow logs, the UI password file, and metadata use separate named volumes.

Inside containers, the weather database host is `weather-db` and port is `5432`.
The Compose environment supplies these values; your local `.env` keeps `localhost`
and port `5433` for Python commands run directly on Windows.

You can also run the extractor directly in the same environment as the DAG tasks:

```powershell
docker compose exec airflow-scheduler python -m weather_etl.extract
```

Its output appears in your local `data/raw/`. This CLI command executes only
extraction; use the DAG below to orchestrate all three steps.

Check the services and logs:

```powershell
docker compose ps -a
docker compose logs --tail 50 airflow-scheduler airflow-dag-processor
```

The `/api/v2/monitor/health` endpoint reports component status. An HTTP 200 alone
does not mean all components are healthy; inspect the returned status fields.

## Daily schedule and manual runs

The `weather_etl` DAG runs daily at **6:00 AM Asia/Manila** using the cron expression
`0 6 * * *` and an explicit CronTriggerTimetable. Its first scheduled run is
**October 31, 2026 at 6:00 AM Philippine time** (October 30 at 22:00 UTC).
The fixed, timezone-aware start date establishes the first allowed trigger.

The scheduler must be running for on-time execution. Keep Docker Desktop and the
project containers running. `catchup=False` prevents a backlog of historical runs,
and `max_active_runs=1` keeps two DAG runs from executing at the same time. The requested
weather day comes from the run's logical date or its explicit date override; a
late run for a past day uses the historical forecast archive.

Each task has **two retries with a five-minute delay**, for up to three attempts
including the initial attempt. A failed task enters `up_for_retry` while attempts
remain. If all attempts fail, downstream tasks do not continue. Retries of transform
and load keep the upstream filename; extraction retries make a new request. Existing
snapshot rows remain protected by the database primary key.

Airflow stores timestamps in UTC; the UI timezone selector controls how you see
them. The DAG's schedule remains 6:00 AM Philippine time regardless of that display.

To run immediately, open [Airflow](http://localhost:8080), find `weather_etl`, and
choose **Trigger Dag**. Manual triggering is still supported alongside the schedule.

You can also trigger it from PowerShell:

```powershell
docker compose exec airflow-scheduler airflow dags trigger weather_etl
```

The graph has three independent city groups (nine tasks):

```text
manila: extract -> transform -> load
cebu:   extract -> transform -> load
davao:  extract -> transform -> load
```

- `extract` calls `extract_weather()` and returns the saved raw JSON filename.
- `transform` receives that exact filename, validates its data, and returns a CSV filename.
- `load` receives that CSV filename and returns the number of newly inserted rows.

The [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/3.3.2/tutorial/taskflow.html)
creates dependencies from function arguments. It passes the small filename strings
through XCom; the weather data itself stays in the shared `data/` directory.
Tasks do not search for the latest file, so other snapshots cannot change a run's input.

Task IDs are prefixed with the city, such as `cebu.extract` and `davao.load`.
Each city passes its own filenames through XCom. A failed city blocks only its
dependent tasks; the other cities can finish. The overall DAG run still reports
failure if a city ultimately fails.

The Python modules expose reusable functions while retaining their `python -m`
commands. SQL transactions and duplicate handling are unchanged. Re-running load
with the same CSV inserts zero duplicate rows. Triggering a new DAG run fetches a
new forecast snapshot, which is intentionally stored as a separate version.

Use the run's Graph view to inspect each task's status and logs. Extract and
transform log their output filenames; load logs its inserted count. If a city's upstream
task fails, its downstream tasks do not continue with unrelated or stale files.

## City configuration

`weather_etl/cities.py` holds the location configuration shared by extraction,
backfills, and the DAG. The same ETL functions process every configured city.

| CLI key | Stored city | Latitude | Longitude |
| --- | --- | --- | --- |
| `manila` | Manila | 14.5995 | 120.9842 |
| `cebu` | Cebu | 10.31672 | 123.89071 |
| `davao` | Davao | 7.07306 | 125.61278 |

Cebu City and Davao City coordinates were verified using the
[Open-Meteo Geocoding API](https://open-meteo.com/en/docs/geocoding-api).
All three locations use Asia/Manila time. Metadata records the requested
coordinates; the raw response also contains the weather model's selected grid
coordinates, which may differ slightly.

The daily DAG processes **all three cities**. Extract and backfill CLI commands
still default to **Manila** to preserve existing usage.

Extract one city:

```powershell
.\.venv\Scripts\python.exe -m weather_etl.extract --city cebu --date 2026-11-01
```

Backfill selected cities:

```powershell
docker compose exec airflow-scheduler python -m weather_etl.backfill --start-date 2026-10-31 --end-date 2026-11-01 --cities cebu davao
```

Use `--cities manila cebu davao` for all three. Unknown, empty, or duplicate city
selections are rejected before any backfill day runs. The command processes the
dates and selected cities sequentially. If one city-day fails, it stops and keeps
earlier successful loads; resume using the failed date and city selection.

City keys prefix output filenames; display names are stored in the CSV and
database. The primary key already includes city, so rows for matching hours
cannot overwrite another city's data. No new database migration is required for
this city expansion.

## Date-based processing and backfills

A weather date is a local calendar day in `Asia/Manila`. Dates must use
`YYYY-MM-DD` and cannot be in the future.

| Requested day | Source | Stored data_type |
| --- | --- | --- |
| Today in Manila | Forecast API | `forecast` |
| An earlier day | Historical Forecast API | `historical_forecast` |

The [Historical Forecast API](https://open-meteo.com/en/docs/historical-forecast-api)
stitches the first hours of successive archived model runs into a continuous series.
It does **not** recreate the exact forecast JSON our original 6 AM run would have
saved, and it is not a direct station observation. Keep these source types separate
when comparing or aggregating records. Archive coverage varies by model; unsupported
dates, missing values, or incomplete days fail instead of being filled with zeros.

### Upgrade an existing weather database

The database in this workspace has already been migrated. On another existing
checkout, apply this migration before running the updated loader:

```powershell
Get-Content sql/migrations/001_historical_forecasts.sql -Raw | docker compose exec -T weather-db sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --single-transaction -v ON_ERROR_STOP=1'
```

The migration preserves existing rows, allows `historical_forecast`, and adds
`data_type` to the primary key. It can be reapplied. A fresh database uses the
updated `sql/schema.sql` automatically when its first CSV is loaded.

### Request one date

Extract a particular date, then use the printed filenames with the existing
transform and load commands:

```powershell
.\.venv\Scripts\python.exe -m weather_etl.extract --date 2026-11-01
```

Omitting `--date` still requests today in Manila. The response must contain exactly
the requested day's 24 hourly timestamps before extraction saves it. Metadata now
also records `weather_date`. Existing saved snapshots without that metadata field
can still be transformed and loaded.

### Recover an inclusive date range

This command runs extract, transform, and load once per date:

```powershell
docker compose exec airflow-scheduler python -m weather_etl.backfill --start-date 2026-10-31 --end-date 2026-11-01
```

The same command works locally using `.\.venv\Scripts\python.exe -m weather_etl.backfill`.
Both bounds are included. The range is validated before requests or writes begin,
and days run sequentially. Each day's load has its own transaction. If a day fails,
the command stops; earlier successful days remain committed. Resume with the
failed or remaining date range.

Re-fetching a date creates a new snapshot with a new UTC fetch time. Loading the
same saved CSV again inserts zero duplicates; re-fetching is intentionally a new
version, not a claim to restore the original collection time. To replay exactly
what was collected previously, transform and load that saved raw snapshot instead.

### Select a date in Airflow

In **Trigger Dag**, set the optional `weather_date` field to a date such as
`2026-10-30`. Leave it empty for the run's default date.

- Scheduled runs use their logical date converted to Asia/Manila.
- Manual runs without a logical date use their trigger time.
- An explicit `weather_date` overrides either value.

This keeps the requested date stable across delayed execution and retries.
A late run for a past day uses the archive and is labeled accordingly. The
6 AM schedule, retry policy, and `catchup=False` remain in effect.

The range command is an application-level recovery tool, not `airflow backfill`:
it directly runs the ETL functions. The Airflow override creates a normal DAG run
with all nine city-task states and logs. The weather-date override applies to all three cities. We are not enabling automatic catch-up of
all historical schedule intervals.

## 1. Extract

```powershell
.\.venv\Scripts\python.exe -m weather_etl.extract
```

Without `--city`, this requests one day's hourly forecast for Manila. Other cities
use the same command with `--city cebu` or `--city davao`. Each extraction writes two files:

```text
data/raw/<city>_<data_type>_<weather_date>_<UTC timestamp>.json
data/raw/<city>_<data_type>_<weather_date>_<UTC timestamp>.metadata.json
```

The raw JSON is preserved unmodified. Metadata records the city, source URL,
and UTC fetch completion time, which is different from the forecast model's issue time.
Each run creates a separate snapshot. The free noncommercial endpoint requires no API key.

HTTP, timeout, network, or JSON errors fail the run rather than reporting success.

## 2. Transform

Use the actual raw filename printed by the extractor, not the metadata filename:

```powershell
.\.venv\Scripts\python.exe -m weather_etl.transform "data/raw/<city>_<data_type>_<weather_date>_<UTC timestamp>.json"
```

Use the actual filename printed by extraction, including its data type, weather date, and UTC fetch timestamp. The matching metadata
file must be next to the raw response. This step works offline and writes a CSV to
`data/processed/` with the same base filename.

The source contains parallel arrays: values at the same position in `time`,
`temperature_2m`, `relative_humidity_2m`, and `precipitation` describe one weather hour.
The transformation combines them into rows and keeps provenance on every row.

Validation rejects unequal or empty arrays, duplicate timestamps, missing or
nonfinite measurements, unexpected units, humidity outside 0-100%, and negative
precipitation. Missing keys also fail the run. Missing measurements are not replaced
with zero. Reprocessing the same snapshot replaces its CSV; the raw input stays intact.
CSV writes are not atomic yet, so rerun transformation if a write is interrupted.

## 3. Load

Use the CSV filename printed by the transform step:

```powershell
.\.venv\Scripts\python.exe -m weather_etl.load "data/processed/<city>_<data_type>_<weather_date>_<UTC timestamp>.csv"
```

The loader creates the table from `sql/schema.sql` if it does not already exist,
converts CSV strings into numbers and timestamps, and inserts the batch in one
transaction. A failed insertion rolls back the whole batch. SQL parameters keep
values separate from SQL instructions.

### Record identity and repeat loads

A record is unique by:

```text
(city, weather_time_local, timezone, data_type, fetched_at_utc)
```

Loading the same snapshot again inserts zero additional rows using
`ON CONFLICT DO NOTHING`. Existing snapshot records are immutable: editing the CSV
and reloading the same key does not update them. A later fetch retains a new forecast
version for the same weather hour. Multiple forecasts for an hour are intentional.

`weather_time_local` is a SQL timestamp without an offset, accompanied by the source
timezone. `fetched_at_utc` is a timezone-aware SQL timestamp. These fields represent
different events. Data is modeled forecast output, not direct station observations.

## Inspect the database

Open an interactive SQL prompt:

```powershell
docker compose exec weather-db sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Count all stored forecast records:

```sql
SELECT count(*) FROM weather_forecasts;
```

Inspect one snapshot's hourly records:

```sql
SELECT city, weather_time_local, temperature_c,
       relative_humidity_pct, precipitation_mm, fetched_at_utc
FROM weather_forecasts
ORDER BY fetched_at_utc DESC, weather_time_local
LIMIT 24;
```

Exit psql with `\q`.

## Daily analytical outputs

`sql/analytics.sql` defines two ordinary PostgreSQL views. A view is a saved SQL
query evaluated when you query it, so subsequent ETL loads appear automatically.
There is no extra refresh job or Airflow task, and raw snapshot history remains
unchanged.

| View | What one row represents |
| --- | --- |
| `latest_weather_hourly` | Latest fetched value for a city, local weather hour, timezone, and data type |
| `daily_weather_summary` | Daily metrics for a city, local weather date, timezone, and data type |

### Create the views once per database

The views have already been created in this workspace. On a fresh checkout, run
the ETL at least once to create `weather_forecasts`, then apply:

```powershell
Get-Content sql/analytics.sql -Raw | docker compose exec -T weather-db sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --single-transaction -v ON_ERROR_STOP=1'
```

The setup uses `CREATE OR REPLACE VIEW` and can be reapplied. It does not insert,
update, or delete weather records. View creation is a database setup step, not
something every city's load task needs to repeat.

### Compare complete daily forecasts

Open the SQL prompt using the command above, then run:

```sql
SELECT city, weather_date,
       average_temperature_c,
       minimum_temperature_c,
       maximum_temperature_c,
       average_humidity_pct,
       total_precipitation_mm
FROM daily_weather_summary
WHERE data_type = 'forecast'
  AND timezone = 'Asia/Manila'
  AND is_complete_day
ORDER BY weather_date DESC, maximum_temperature_c DESC, city;
```

Compare recovered historical data separately:

```sql
SELECT city, weather_date, total_precipitation_mm
FROM daily_weather_summary
WHERE data_type = 'historical_forecast'
  AND timezone = 'Asia/Manila'
  AND is_complete_day
ORDER BY weather_date DESC, total_precipitation_mm DESC, city;
```

These metrics summarize model output. They are not station observations or proof
of the actual temperature or rainfall.

### Understand completeness and provenance

- `hours_available` counts the latest records used in the summary.
- `is_complete_day` requires 24 hour-aligned timestamps in the local calendar day.
  All configured cities use Asia/Manila, which has 24-hour days.
- Incomplete summaries remain visible. Their metrics cover only available hours;
  filter `is_complete_day` when comparing full-day totals.
- `selected_snapshot_count` reports how many fetch versions contribute the selected
  hourly values. A summary can combine hours from different fetches; it does not
  claim to represent one forecast issued at a single time.
- `data_as_of_utc` is the most recent fetch time among the selected hours. It is
  not the weather date or forecast issue time.
- Each metric stays separate by city, timezone, and `data_type`. Historical forecast
  recoveries are not blended with saved daily forecast snapshots.
- Metrics are rounded to two decimal places. Hourly raw values remain available
  in `weather_forecasts` and `latest_weather_hourly`.

Check completeness explicitly:

```sql
SELECT city, weather_date, data_type, hours_available,
       is_complete_day, selected_snapshot_count, data_as_of_utc
FROM daily_weather_summary
ORDER BY weather_date DESC, city, data_type;
```

Repeated snapshots are removed before aggregation. For example, collecting the
same weather hour twice does not add its rainfall twice. A later fetch replaces
that hour's selected value while both originals remain in snapshot history.

## Tests

Run the offline file-handoff and transform tests (database and DAG checks are skipped):

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```

With the local database running, enable the integration tests too:

```powershell
$env:WEATHER_DB_TESTS = "1"
.\.venv\Scripts\python.exe -m unittest discover -v
Remove-Item Env:WEATHER_DB_TESTS
```

Database tests create and remove a private temporary schema. They check duplicate
prevention, forecast history, batch rollback, missing measurements, timestamp offsets,
and empty input without changing your weather table.


The DAG-loading test requires Airflow. Run the complete suite in the existing
Airflow image while the databases are running:

```powershell
docker compose run --rm --no-deps -e WEATHER_DB_TESTS=1 -v "${PWD}/tests:/opt/airflow/project/tests:ro" airflow-scheduler python -m unittest discover -v
```

This temporarily mounts `tests/` read-only and uses the container's database
settings. The DAG checks verify the first and following daily trigger times and
all three independent city chains. City checks verify request coordinates, file
and metadata isolation, selection validation, and database key separation.
Analytical tests check latest-version totals, source/date/timezone separation,
complete-day flags, live view updates, and empty datasets.
The file-handoff checks use temporary files; the external HTTP response is mocked
only for the offline extractor test.

## Stop and restart

```powershell
docker compose stop
docker compose start
```

`docker compose down` removes the project containers and network while keeping its
named database volume. Do not add `--volumes` unless you intend to delete stored data.
PostgreSQL initialization credentials are applied only when its volume is first
created; editing the password in `.env` does not change an existing database password.

The local Docker database uses an administrative account for this learning milestone.
A deployment would need a separate application role and managed secrets.

## Repository files

```text
weather-etl/
|-- weather_etl/         # Python pipeline package
|   |-- __init__.py
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- dates.py
|   |-- cities.py
|   `-- backfill.py
|-- tests/               # Offline and database tests
|-- dags/                # Airflow DAG definitions
|-- docker/
|   -- airflow/Dockerfile
|-- sql/
|   |-- schema.sql
|   |-- analytics.sql
|   `-- migrations/
|-- data/                # Generated files, ignored by Git
|   |-- raw/
|   `-- processed/
|-- compose.yaml
|-- requirements.txt
|-- .env.example
|-- .gitignore
`-- README.md
```

Run module commands from the repository root. `python -m weather_etl.extract`
means run the `extract` module inside the `weather_etl` package. Scripts locate
`.env`, SQL, and output directories relative to the project root, not their own
package folder. `tests/__init__.py` enables recursive unittest discovery.

- `weather_etl/extract.py`: HTTP extraction, raw snapshots, and `extract_weather()`.
- `weather_etl/transform.py`: validated rows, CSV output, and `transform_file()`.
- `weather_etl/load.py`: transactional loading through `load_csv()` and `load_file()`.
- `sql/schema.sql`: table types, primary key, and constraints.
- `sql/analytics.sql`: latest-hourly and daily-summary views.
- `sql/migrations/`: upgrades for an existing weather database.
- `weather_etl/dates.py`: shared date parsing and run-date selection.
- `weather_etl/cities.py`: configured city names and coordinates.
- `weather_etl/backfill.py`: sequential date-range recovery.
- compose.yaml: weather database and Airflow services, health checks, and persistent storage.
- docker/airflow/Dockerfile: pinned Airflow image with pipeline dependencies.
- `.env.example`: configuration template; actual `.env` is local.
- dags/weather_etl_dag.py: three city ETL groups sharing the daily schedule and retry policy.
- 	ests/: file handoff, transformation, database, and DAG checks.

Generated data and the virtual environment are ignored by Git. No database files
are stored in the repository.

## Next milestones

1. Polish the README and project architecture documentation for GitHub.

## Sources

Weather data provided by [Open-Meteo](https://open-meteo.com/).
See the [API documentation](https://open-meteo.com/en/docs) and
[terms and data attribution requirements](https://open-meteo.com/en/terms).
The free endpoint is intended for noncommercial use within its usage limits.

Database runtime: [official PostgreSQL Docker image](https://hub.docker.com/_/postgres).

## HTTPS certificate verification

The extractor uses [Requests](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification)
with its default certificate verification and maintained CA bundle from certifi.
No custom TLS context or truststore configuration is needed. Install dependencies
into the virtual environment before running extraction.

The extractor checks HTTP error statuses before saving the original JSON response
and records the full request URL in the metadata. Its 30-second timeout limits
network waiting. Requests does not impose a total deadline for the entire download.

On the local Windows setup, Python's default urllib/OpenSSL verification selected
an expired intermediate certificate. A fresh Requests installation successfully
fetched the same API with default verification enabled. Other environments should
also be checked when the extractor is moved into Docker or deployed.
