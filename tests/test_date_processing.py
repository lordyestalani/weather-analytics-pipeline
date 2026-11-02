"""Check requested-day validation, historical provenance, and inclusive date ranges."""

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from weather_etl.backfill import dates_in_range
from weather_etl.dates import weather_date_for_run
from weather_etl.extract import extract_weather


class DateProcessingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        clock = patch("weather_etl.dates.today_in_manila", return_value=date(2026, 11, 2))
        clock.start()
        self.addCleanup(clock.stop)
        self.payload = {
            "timezone": "Asia/Manila",
            "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm"},
            "hourly": {
                "time": [f"2026-11-01T{hour:02}:00" for hour in range(24)],
                "temperature_2m": [26.6] * 24,
                "relative_humidity_2m": [84] * 24,
                "precipitation": [0.9] * 24,
            },
        }

    def test_past_date_saves_archived_data_with_historical_label(self):
        with patch("weather_etl.extract.PROJECT_DIRECTORY", self.directory), patch("weather_etl.extract.requests.get") as request:
            response = request.return_value.__enter__.return_value
            response.content = json.dumps(self.payload).encode()
            response.url = "https://historical-forecast-api.open-meteo.com/v1/forecast?start_date=2026-11-01&end_date=2026-11-01"
            output = extract_weather("2026-11-01")
            self.assertEqual(request.call_args.args[0], "https://historical-forecast-api.open-meteo.com/v1/forecast")
            self.assertEqual(request.call_args.kwargs["params"]["start_date"], "2026-11-01")
            self.assertEqual(request.call_args.kwargs["params"]["end_date"], "2026-11-01")
        metadata = json.loads(output.with_suffix(".metadata.json").read_text())
        self.assertEqual(metadata["data_type"], "historical_forecast")
        self.assertEqual(metadata["weather_date"], "2026-11-01")
        self.assertEqual(len(json.loads(output.read_bytes())["hourly"]["time"]), 24)

    def test_invalid_or_future_dates_do_not_create_files(self):
        for value in ("2026-02-30", "20261101", "2026-11-03"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                extract_weather(value)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_wrong_day_or_incomplete_response_is_rejected_before_saving(self):
        for times in ([f"2026-10-31T{hour:02}:00" for hour in range(24)], self.payload["hourly"]["time"][:-1]):
            self.payload["hourly"]["time"] = times
            with patch("weather_etl.extract.PROJECT_DIRECTORY", self.directory), patch("weather_etl.extract.requests.get") as request:
                request.return_value.__enter__.return_value.content = json.dumps(self.payload).encode()
                with self.assertRaises(ValueError):
                    extract_weather("2026-11-01")
        self.assertFalse((self.directory / "data").exists())

    def test_date_range_is_inclusive_and_rejects_reversed_or_future_ranges(self):
        self.assertEqual(dates_in_range("2026-10-30", "2026-11-01"), [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1)])
        self.assertEqual(dates_in_range("2026-11-01", "2026-11-01"), [date(2026, 11, 1)])
        for start, end in (("2026-11-01", "2026-10-30"), ("2026-11-01", "2026-11-03")):
            with self.assertRaises(ValueError):
                dates_in_range(start, end)

    def test_delayed_run_keeps_logical_date_and_manual_run_uses_its_trigger_date(self):
        trigger = datetime(2026, 11, 1, 18, tzinfo=timezone.utc)
        logical_date = datetime(2026, 10, 31, 22, tzinfo=timezone.utc)
        self.assertEqual(weather_date_for_run(None, logical_date, trigger), date(2026, 11, 1))
        self.assertEqual(weather_date_for_run(None, None, trigger), date(2026, 11, 2))
        self.assertEqual(weather_date_for_run("2026-10-30", logical_date, trigger), date(2026, 10, 30))


if __name__ == "__main__":
    unittest.main()
