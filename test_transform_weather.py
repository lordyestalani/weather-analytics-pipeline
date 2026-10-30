import unittest

from transform_weather import transform


class TransformTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "timezone": "Asia/Manila",
            "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm"},
            "hourly": {
                "time": ["2026-10-30T00:00", "2026-10-30T01:00"],
                "temperature_2m": [26.6, 27.0],
                "relative_humidity_2m": [84, 80],
                "precipitation": [0.9, 0.0],
            },
        }
        self.metadata = {"city": "Manila", "data_type": "forecast", "fetched_at_utc": "2026-10-30T11:53:27+00:00", "raw_file": "snapshot.json"}

    def test_pairs_hourly_values_and_preserves_provenance(self):
        rows = transform(self.payload, self.metadata)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1], {
            "city": "Manila", "weather_time_local": "2026-10-30T01:00",
            "timezone": "Asia/Manila", "temperature_c": 27.0,
            "relative_humidity_pct": 80, "precipitation_mm": 0.0,
            "fetched_at_utc": "2026-10-30T11:53:27+00:00",
            "data_type": "forecast", "source_file": "snapshot.json",
        })

    def test_rejects_arrays_that_would_silently_drop_hours(self):
        self.payload["hourly"]["precipitation"].pop()
        with self.assertRaises(ValueError):
            transform(self.payload, self.metadata)

    def test_rejects_missing_measurements_instead_of_treating_them_as_zero(self):
        self.payload["hourly"]["precipitation"][0] = None
        with self.assertRaises(ValueError):
            transform(self.payload, self.metadata)

    def test_rejects_units_that_would_mislabel_measurements(self):
        self.payload["hourly_units"]["temperature_2m"] = "°F"
        with self.assertRaises(ValueError):
            transform(self.payload, self.metadata)


if __name__ == "__main__":
    unittest.main()
