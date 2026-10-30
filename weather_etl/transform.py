"""Turn an extracted Open-Meteo forecast snapshot into validated CSV rows."""

import argparse
import csv
import json
import math
from pathlib import Path

PROJECT_DIRECTORY = Path(__file__).resolve().parents[1]


def transform(payload, metadata):
    """Validate parallel arrays before combining their matching positions."""
    expected_units = {
        "time": "iso8601", "temperature_2m": "°C",
        "relative_humidity_2m": "%", "precipitation": "mm",
    }
    for field, unit in expected_units.items():
        if payload["hourly_units"].get(field) != unit:
            raise ValueError(f"Unexpected unit for {field}; expected {unit}")

    hourly = payload["hourly"]
    arrays = [hourly[field] for field in expected_units]
    if not all(isinstance(values, list) for values in arrays):
        raise ValueError("Hourly fields must be arrays")
    if not arrays[0] or len({len(values) for values in arrays}) != 1:
        raise ValueError("Hourly arrays must be nonempty and have equal lengths")
    if len(set(hourly["time"])) != len(hourly["time"]):
        raise ValueError("Duplicate weather timestamps in this snapshot")

    rows = []
    for time, temperature, humidity, precipitation in zip(*arrays):
        for value in (temperature, humidity, precipitation):
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"Missing or invalid measurement at {time}")
        if not 0 <= humidity <= 100 or precipitation < 0:
            raise ValueError(f"Humidity or precipitation outside valid range at {time}")
        rows.append({
            "city": metadata["city"],
            "weather_time_local": time,
            "timezone": payload["timezone"],
            "temperature_c": temperature,
            "relative_humidity_pct": humidity,
            "precipitation_mm": precipitation,
            "fetched_at_utc": metadata["fetched_at_utc"],
            "data_type": metadata["data_type"],
            "source_file": metadata["raw_file"],
        })
    return rows


def transform_file(raw_file: str | Path) -> Path:
    raw_path = Path(raw_file).resolve()

    metadata_path = raw_path.with_suffix(".metadata.json")
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata["raw_file"] != raw_path.name:
        raise ValueError("Metadata does not refer to the selected raw file")
    rows = transform(payload, metadata)

    output_directory = PROJECT_DIRECTORY / "data" / "processed"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / (raw_path.stem + ".csv")
    # Reprocessing the same snapshot replaces its CSV; the raw input stays intact.
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} validated forecast rows: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_file", type=Path, help="Raw forecast JSON (not the metadata file)")
    args = parser.parse_args()
    transform_file(args.raw_file)


if __name__ == "__main__":
    main()
