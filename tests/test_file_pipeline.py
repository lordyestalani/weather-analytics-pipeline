"""Check file handoff between the reusable extraction and transformation functions."""

import csv
from datetime import date
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from weather_etl.extract import extract_weather
from weather_etl.transform import transform_file


class FilePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        clock = patch("weather_etl.dates.today_in_manila", return_value=date(2026, 10, 30))
        clock.start()
        self.addCleanup(clock.stop)
        self.raw_bytes = json.dumps({
            "timezone": "Asia/Manila",
            "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm"},
            "hourly": {
                "time": [f"2026-10-30T{hour:02}:00" for hour in range(24)],
                "temperature_2m": [26.6] * 24,
                "relative_humidity_2m": [84] * 24,
                "precipitation": [0.9] * 24,
            },
        }, indent=1).encode()

    def test_extract_returns_saved_snapshot_with_original_bytes_and_metadata(self):
        # Only the external HTTP response is mocked; file writing is real.
        with patch("weather_etl.extract.PROJECT_DIRECTORY", self.directory), patch("weather_etl.extract.requests.get") as request:
            response = request.return_value.__enter__.return_value
            response.content = self.raw_bytes
            response.url = "https://api.open-meteo.com/v1/forecast?latitude=14.5995"
            output = extract_weather()
        self.assertEqual(output.read_bytes(), self.raw_bytes)
        metadata = json.loads(output.with_suffix(".metadata.json").read_text())
        self.assertEqual(metadata["raw_file"], output.name)
        self.assertEqual(metadata["city"], "Manila")
        self.assertEqual(metadata["source_url"], "https://api.open-meteo.com/v1/forecast?latitude=14.5995")

    def test_transform_uses_the_supplied_snapshot_despite_unrelated_files(self):
        raw_path = self.directory / "chosen.json"
        raw_path.write_bytes(self.raw_bytes)
        raw_path.with_suffix(".metadata.json").write_text(json.dumps({
            "city": "Manila", "fetched_at_utc": "2026-10-30T12:00:00+00:00",
            "data_type": "forecast", "raw_file": "chosen.json",
        }))
        (self.directory / "newer.json").write_text("unrelated file")
        with patch("weather_etl.transform.PROJECT_DIRECTORY", self.directory):
            output = transform_file(raw_path)
        with output.open(newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), 24)
        self.assertEqual(rows[0]["source_file"], "chosen.json")
        self.assertEqual(rows[0]["temperature_c"], "26.6")
        self.assertEqual(rows[0]["fetched_at_utc"], "2026-10-30T12:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
