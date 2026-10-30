# Philippine Weather ETL Pipeline

A learning project for Python, SQL, and data engineering, progressing toward a daily Apache Airflow DAG. The intended analytical output is a comparison of weather across Manila, Cebu, and Davao.

## Current milestone: a working ETL pipeline for Manila

```text
Open-Meteo API -> raw JSON -> validated CSV -> PostgreSQL
                  extract      transform       load
```

The ETL modules can run locally or inside the Airflow image. PostgreSQL and the
Airflow infrastructure run in Docker. The weather DAG and scheduling are the next
milestone; no DAG has been added yet.

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

- `dags/` is mounted read-only at `/opt/airflow/dags` (empty at this milestone).
- `weather_etl/` and `sql/` are mounted read-only under `/opt/airflow/project`.
- `data/` is shared read/write at `/opt/airflow/project/data`.
- `PYTHONPATH` makes the pipeline package importable inside each Airflow service.
- The Dockerfile installs `requirements.txt` at build time while keeping Airflow pinned.
- Airflow logs, the UI password file, and metadata use separate named volumes.

Inside containers, the weather database host is `weather-db` and port is `5432`.
The Compose environment supplies these values; your local `.env` keeps `localhost`
and port `5433` for Python commands run directly on Windows.

You can run the existing extractor in the same environment future DAG tasks will use:

```powershell
docker compose exec airflow-scheduler python -m weather_etl.extract
```

Its output appears in your local `data/raw/`. This is a CLI check, not a scheduled
Airflow run. A DAG connecting extract, transform, and load comes in the next commit.

Check the services and logs:

```powershell
docker compose ps -a
docker compose logs --tail 50 airflow-scheduler airflow-dag-processor
```

The `/api/v2/monitor/health` endpoint reports component status. An HTTP 200 alone
does not mean all components are healthy; inspect the returned status fields.

## 1. Extract

```powershell
.\.venv\Scripts\python.exe -m weather_etl.extract
```

This requests one day's hourly forecast for Manila and writes two files:

```text
data/raw/manila_forecast_<UTC timestamp>.json
data/raw/manila_forecast_<UTC timestamp>.metadata.json
```

The raw JSON is preserved unmodified. Metadata records the city, source URL,
and UTC fetch completion time, which is different from the forecast model's issue time.
Each run creates a separate snapshot. The free noncommercial endpoint requires no API key.

HTTP, timeout, network, or JSON errors fail the run rather than reporting success.

## 2. Transform

Use the actual raw filename printed by the extractor, not the metadata filename:

```powershell
.\.venv\Scripts\python.exe -m weather_etl.transform "data/raw/manila_forecast_<UTC timestamp>.json"
```

Replace `<UTC timestamp>` with the timestamp in your filename. The matching metadata
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
.\.venv\Scripts\python.exe -m weather_etl.load "data/processed/manila_forecast_<UTC timestamp>.csv"
```

The loader creates the table from `sql/schema.sql` if it does not already exist,
converts CSV strings into numbers and timestamps, and inserts the batch in one
transaction. A failed insertion rolls back the whole batch. SQL parameters keep
values separate from SQL instructions.

### Record identity and repeat loads

A record is unique by:

```text
(city, weather_time_local, timezone, fetched_at_utc)
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

Compare daily forecasts using only the latest fetch per city, hour, and timezone:

```sql
WITH latest AS (
    SELECT DISTINCT ON (city, weather_time_local, timezone) *
    FROM weather_forecasts
    ORDER BY city, weather_time_local, timezone, fetched_at_utc DESC
)
SELECT city, weather_time_local::date AS weather_date,
       round(avg(temperature_c)::numeric, 2) AS average_temperature_c,
       round(sum(precipitation_mm)::numeric, 2) AS total_precipitation_mm
FROM latest
GROUP BY city, weather_time_local::date
ORDER BY weather_date, city;
```

Selecting the latest version avoids counting the same hour's precipitation across
multiple forecast snapshots. These summaries describe forecasts, not measured outcomes.
Exit psql with `\q`.

## Tests

Run the offline transform tests (database tests are skipped):

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
|   `-- load.py
|-- tests/               # Offline and database tests
|-- dags/                # Future Airflow DAGs
|-- docker/
|   -- airflow/Dockerfile
|-- sql/
|   `-- schema.sql
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

- `weather_etl/extract.py`: HTTP extraction and raw snapshots.
- `weather_etl/transform.py`: validated rows and CSV output.
- `weather_etl/load.py`: transactional, repeatable database loading.
- `sql/schema.sql`: table types, primary key, and constraints.
- compose.yaml: weather database and Airflow services, health checks, and persistent storage.
- docker/airflow/Dockerfile: pinned Airflow image with pipeline dependencies.
- `.env.example`: configuration template; actual `.env` is local.
- `tests/test_transform_weather.py`, `tests/test_load_weather.py`: transformation and database checks.

Generated data and the virtual environment are ignored by Git. No database files
are stored in the repository.

## Next milestones

1. Add the weather DAG and verify a manual run.
2. Add daily scheduling, retries, and date-based processing.
3. Cebu and Davao, daily summaries, and project architecture documentation.

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
