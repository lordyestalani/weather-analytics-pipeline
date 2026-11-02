"""Check city coordinates, file isolation, and city validation before processing."""

import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from weather_etl.backfill import backfill
from weather_etl.extract import extract_weather
from weather_etl.transform import transform_file


class CityProcessingTests(unittest.TestCase):
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

    def test_cebu_and_davao_use_correct_coordinates_and_keep_their_own_files(self):
        paths = []
        for city, name, latitude, longitude in (
            ("cebu", "Cebu", 10.31672, 123.89071),
            ("davao", "Davao", 7.07306, 125.61278),
        ):
            with self.subTest(city=city), patch("weather_etl.extract.PROJECT_DIRECTORY", self.directory), patch("weather_etl.transform.PROJECT_DIRECTORY", self.directory), patch("weather_etl.extract.requests.get") as request:
                response = request.return_value.__enter__.return_value
                response.content = json.dumps(self.payload).encode()
                response.url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
                raw_path = extract_weather("2026-11-01", city=city)
                self.assertEqual(request.call_args.kwargs["params"]["latitude"], latitude)
                self.assertEqual(request.call_args.kwargs["params"]["longitude"], longitude)
                self.assertTrue(raw_path.name.startswith(city + "_"))
                metadata = json.loads(raw_path.with_suffix(".metadata.json").read_text())
                self.assertEqual(metadata["city"], name)
                csv_path = transform_file(raw_path)
            with csv_path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(len(rows), 24)
            self.assertEqual({row["city"] for row in rows}, {name})
            self.assertEqual({row["source_file"] for row in rows}, {raw_path.name})
            paths.append(raw_path)
        self.assertNotEqual(paths[0], paths[1])
        self.assertTrue(all(path.exists() for path in paths))

    def test_unknown_city_is_rejected_before_an_http_request(self):
        with patch("weather_etl.extract.requests.get") as request:
            with self.assertRaises(ValueError):
                extract_weather("2026-11-01", city="unknown")
        request.assert_not_called()

    def test_invalid_city_list_is_rejected_before_any_backfill_day(self):
        for cities in (["cebu", "unknown"], ["cebu", "CEBU"], []):
            with self.subTest(cities=cities), patch("weather_etl.backfill.extract_weather") as extract:
                with self.assertRaises(ValueError):
                    backfill("2026-11-01", "2026-11-01", cities=cities)
                extract.assert_not_called()


if __name__ == "__main__":
    unittest.main()
