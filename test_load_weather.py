"""Database integration tests: opt in with WEATHER_DB_TESTS=1."""

import csv
import os
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from load_weather import database_settings, load_csv


@unittest.skipUnless(os.getenv("WEATHER_DB_TESTS") == "1", "Set WEATHER_DB_TESTS=1 with the local database running")
class LoadTests(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(**database_settings(), autocommit=True)
        self.addCleanup(self.connection.close)
        schema = "test_weather_" + uuid4().hex
        self.connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.addCleanup(lambda: self.connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))))
        self.connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        self.connection.execute((Path(__file__).parent / "sql" / "schema.sql").read_text(encoding="utf-8-sig"))
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.csv_path = Path(temporary.name) / "forecast.csv"
        self.rows = [
            {"city": "Manila", "weather_time_local": "2026-10-30T00:00", "timezone": "Asia/Manila", "temperature_c": "26.6", "relative_humidity_pct": "84", "precipitation_mm": "0.9", "fetched_at_utc": "2026-10-30T11:53:27+00:00", "data_type": "forecast", "source_file": "snapshot.json"},
            {"city": "Manila", "weather_time_local": "2026-10-30T01:00", "timezone": "Asia/Manila", "temperature_c": "27.0", "relative_humidity_pct": "80", "precipitation_mm": "0.0", "fetched_at_utc": "2026-10-30T11:53:27+00:00", "data_type": "forecast", "source_file": "snapshot.json"},
        ]

    def write_csv(self, rows):
        with self.csv_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(self.rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_reloading_snapshot_does_not_duplicate_or_change_values(self):
        self.write_csv(self.rows)
        self.assertEqual(load_csv(self.csv_path, self.connection), 2)
        self.assertEqual(load_csv(self.csv_path, self.connection), 0)
        result = self.connection.execute("SELECT count(*), min(temperature_c), sum(precipitation_mm) FROM weather_forecasts").fetchone()
        self.assertEqual(result, (2, 26.6, 0.9))

    def test_later_fetch_preserves_both_forecast_versions(self):
        self.write_csv(self.rows)
        load_csv(self.csv_path, self.connection)
        later = [dict(row, fetched_at_utc="2026-10-30T12:53:27+00:00", temperature_c="30.0") for row in self.rows]
        self.write_csv(later)
        self.assertEqual(load_csv(self.csv_path, self.connection), 2)
        self.assertEqual(self.connection.execute("SELECT count(*), count(DISTINCT fetched_at_utc) FROM weather_forecasts").fetchone(), (4, 2))

    def test_bad_second_row_rolls_back_first_insert(self):
        self.rows[1]["relative_humidity_pct"] = "120"
        self.write_csv(self.rows)
        with self.assertRaises(psycopg.errors.CheckViolation):
            load_csv(self.csv_path, self.connection)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM weather_forecasts").fetchone()[0], 0)

    def test_missing_measurement_is_not_loaded_as_zero(self):
        self.rows[1]["precipitation_mm"] = ""
        self.write_csv(self.rows)
        with self.assertRaises(ValueError):
            load_csv(self.csv_path, self.connection)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM weather_forecasts").fetchone()[0], 0)

    def test_fetch_timestamp_requires_a_timezone(self):
        self.rows[0]["fetched_at_utc"] = "2026-10-30T11:53:27"
        self.write_csv(self.rows)
        with self.assertRaises(ValueError):
            load_csv(self.csv_path, self.connection)

    def test_empty_csv_is_rejected(self):
        self.write_csv([])
        with self.assertRaises(ValueError):
            load_csv(self.csv_path, self.connection)


if __name__ == "__main__":
    unittest.main()
