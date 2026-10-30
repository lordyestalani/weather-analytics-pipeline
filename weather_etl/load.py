"""Load a transformed forecast CSV into PostgreSQL without duplicate snapshots."""

import argparse
import csv
import math
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv

PROJECT_DIRECTORY = Path(__file__).resolve().parents[1]
COLUMNS = (
    "city", "weather_time_local", "timezone", "temperature_c",
    "relative_humidity_pct", "precipitation_mm", "fetched_at_utc",
    "data_type", "source_file",
)
INSERT_SQL = """
    INSERT INTO weather_forecasts (
        city, weather_time_local, timezone, temperature_c,
        relative_humidity_pct, precipitation_mm, fetched_at_utc,
        data_type, source_file
    ) VALUES (
        %(city)s, %(weather_time_local)s, %(timezone)s, %(temperature_c)s,
        %(relative_humidity_pct)s, %(precipitation_mm)s, %(fetched_at_utc)s,
        %(data_type)s, %(source_file)s
    )
    ON CONFLICT (city, weather_time_local, timezone, fetched_at_utc) DO NOTHING
"""


def database_settings():
    """Read local .env settings; explicit environment variables take precedence."""
    load_dotenv(PROJECT_DIRECTORY / ".env")
    password = os.getenv("WEATHER_DB_PASSWORD")
    if not password:
        raise ValueError("Set WEATHER_DB_PASSWORD in .env or your environment")
    return {
        "host": os.getenv("WEATHER_DB_HOST", "localhost"),
        "port": int(os.getenv("WEATHER_DB_PORT", "5433")),
        "dbname": os.getenv("WEATHER_DB_NAME", "weather"),
        "user": os.getenv("WEATHER_DB_USER", "weather"),
        "password": password,
        "connect_timeout": 10,
    }


def read_rows(csv_path):
    """Convert CSV strings into typed database values before opening a transaction."""
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None or sorted(reader.fieldnames) != sorted(COLUMNS):
            raise ValueError("CSV headers must match the transform output")
        rows = []
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(not row[field] or not row[field].strip() for field in COLUMNS):
                raise ValueError(f"Missing or extra CSV values on line {line_number}")
            row["weather_time_local"] = datetime.fromisoformat(row["weather_time_local"])
            row["fetched_at_utc"] = datetime.fromisoformat(row["fetched_at_utc"])
            if row["weather_time_local"].tzinfo is not None:
                raise ValueError("weather_time_local must be a local time without an offset")
            if row["fetched_at_utc"].utcoffset() is None:
                raise ValueError("fetched_at_utc must include a timezone offset")
            ZoneInfo(row["timezone"])
            row["temperature_c"] = float(row["temperature_c"])
            row["relative_humidity_pct"] = int(row["relative_humidity_pct"])
            row["precipitation_mm"] = float(row["precipitation_mm"])
            if not all(math.isfinite(row[field]) for field in ("temperature_c", "precipitation_mm")):
                raise ValueError(f"Nonfinite measurement on line {line_number}")
            rows.append(row)
    if not rows:
        raise ValueError("CSV contains no weather records")
    return rows


def load_csv(csv_path, connection):
    """Commit all new rows together, or roll back the entire batch on failure."""
    rows = read_rows(csv_path)
    schema = (PROJECT_DIRECTORY / "sql" / "schema.sql").read_text(encoding="utf-8-sig")
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(schema)
            # Parameter binding keeps data separate from SQL instructions.
            cursor.executemany(INSERT_SQL, rows)
            inserted = cursor.rowcount
    return inserted


def load_file(csv_file: str | Path) -> int:

    with psycopg.connect(**database_settings(), autocommit=True) as connection:
        inserted = load_csv(csv_file, connection)
        total = connection.execute("SELECT count(*) FROM weather_forecasts").fetchone()[0]
    print(f"Inserted {inserted} new forecast rows. Database total: {total}.")
    print("Existing snapshot rows are skipped; later fetches are kept separately.")
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file", type=Path, help="CSV produced by python -m weather_etl.transform")
    args = parser.parse_args()
    load_file(args.csv_file)


if __name__ == "__main__":
    main()
