"""Exercise analytical SQL against isolated real PostgreSQL schemas."""

import os
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from weather_etl.load import database_settings


@unittest.skipUnless(os.getenv("WEATHER_DB_TESTS") == "1", "Set WEATHER_DB_TESTS=1 with the local database running")
class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(**database_settings(), autocommit=True)
        self.addCleanup(self.connection.close)
        schema = "test_analytics_" + uuid4().hex
        self.connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.addCleanup(lambda: self.connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))))
        self.connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        root = Path(__file__).resolve().parents[1]
        self.connection.execute((root / "sql" / "schema.sql").read_text(encoding="utf-8-sig"))
        self.connection.execute((root / "sql" / "analytics.sql").read_text(encoding="utf-8-sig"))

    def insert_hour(self, weather_time="2026-11-02T00:00", city="Manila", data_type="forecast", fetched_at="2026-11-02T00:00:00+00:00", temperature=24, humidity=50, rain=1, zone="Asia/Manila", source_file="fixture.json"):
        self.connection.execute(
            """INSERT INTO weather_forecasts (
                city, weather_time_local, timezone, data_type, fetched_at_utc,
                temperature_c, relative_humidity_pct, precipitation_mm, source_file
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (city, datetime.fromisoformat(weather_time), zone, data_type, datetime.fromisoformat(fetched_at), temperature, humidity, rain, source_file),
        )

    def test_latest_versions_change_metrics_without_counting_an_hour_twice(self):
        self.insert_hour(temperature=20.1, humidity=80, rain=1.1)
        self.insert_hour(weather_time="2026-11-02T01:00", temperature=24.2, humidity=60, rain=2.2)
        before = self.connection.execute("SELECT average_temperature_c, total_precipitation_mm FROM daily_weather_summary").fetchone()
        self.assertEqual(before, (Decimal("22.15"), Decimal("3.30")))
        self.insert_hour(temperature=30.1, humidity=51, rain=3.1, fetched_at="2026-11-02T01:00:00+00:00", source_file="later.json")
        result = self.connection.execute("""SELECT average_temperature_c, minimum_temperature_c, maximum_temperature_c,
            average_humidity_pct, total_precipitation_mm, hours_available, is_complete_day, selected_snapshot_count, data_as_of_utc
            FROM daily_weather_summary""").fetchone()
        self.assertEqual(result, (Decimal("27.15"), Decimal("24.20"), Decimal("30.10"), Decimal("55.50"), Decimal("5.30"), 2, False, 2, datetime(2026, 11, 2, 1, tzinfo=timezone.utc)))
        self.assertEqual(self.connection.execute("SELECT count(*) FROM weather_forecasts").fetchone()[0], 3)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM latest_weather_hourly").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT source_file FROM latest_weather_hourly WHERE weather_time_local = '2026-11-02T00:00'").fetchone()[0], "later.json")

    def test_city_source_timezone_and_local_weather_date_stay_separate(self):
        self.insert_hour(city="Manila", rain=1)
        self.insert_hour(city="Cebu", rain=7)
        self.insert_hour(city="Manila", data_type="historical_forecast", rain=9)
        self.insert_hour(city="Manila", zone="UTC", rain=11)
        # The weather date is November 3 even though collection was November 2 UTC.
        self.insert_hour(weather_time="2026-11-03T00:00", fetched_at="2026-11-02T18:00:00+00:00", rain=13)
        rows = self.connection.execute("SELECT city, weather_date, timezone, data_type, total_precipitation_mm FROM daily_weather_summary").fetchall()
        actual = {row[:4]: row[4] for row in rows}
        self.assertEqual(actual, {
            ("Manila", date(2026, 11, 2), "Asia/Manila", "forecast"): Decimal("1.00"),
            ("Cebu", date(2026, 11, 2), "Asia/Manila", "forecast"): Decimal("7.00"),
            ("Manila", date(2026, 11, 2), "Asia/Manila", "historical_forecast"): Decimal("9.00"),
            ("Manila", date(2026, 11, 2), "UTC", "forecast"): Decimal("11.00"),
            ("Manila", date(2026, 11, 3), "Asia/Manila", "forecast"): Decimal("13.00"),
        })

    def test_complete_day_reports_24_hours_and_daily_metrics(self):
        for hour in range(24):
            self.insert_hour(weather_time=f"2026-11-02T{hour:02}:00", temperature=36 if hour == 15 else 24)
        row = self.connection.execute("""SELECT hours_available, is_complete_day, average_temperature_c,
            minimum_temperature_c, maximum_temperature_c, total_precipitation_mm, selected_snapshot_count
            FROM daily_weather_summary""").fetchone()
        self.assertEqual(row, (24, True, Decimal("24.50"), Decimal("24.00"), Decimal("36.00"), Decimal("24.00"), 1))

    def test_24_non_hourly_records_are_not_marked_as_complete(self):
        for hour in range(24):
            self.insert_hour(weather_time=f"2026-11-02T{hour:02}:30")
        self.assertEqual(self.connection.execute("SELECT hours_available, is_complete_day FROM daily_weather_summary").fetchone(), (24, False))

    def test_empty_tables_do_not_invent_zero_weather_days(self):
        self.assertEqual(self.connection.execute("SELECT count(*) FROM latest_weather_hourly").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM daily_weather_summary").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
