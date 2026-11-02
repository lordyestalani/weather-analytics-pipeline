"""Fetch one requested Philippine city weather day and preserve its original JSON response."""

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from weather_etl import dates
from weather_etl.cities import CITIES, get_city

PROJECT_DIRECTORY = Path(__file__).resolve().parents[1]


def extract_weather(weather_date: str | date | None = None, city: str = "manila") -> Path:
    location = get_city(city)
    today = dates.today_in_manila()
    target = dates.resolve_weather_date(weather_date, today=today)
    data_type = "historical_forecast" if target < today else "forecast"
    host = "historical-forecast-api.open-meteo.com" if target < today else "api.open-meteo.com"
    parameters = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": "Asia/Manila",
        "start_date": target.isoformat(),
        "end_date": target.isoformat(),
    }
    url = f"https://{host}/v1/forecast"

    # Requests verifies HTTPS certificates by default. A timeout limits waiting.
    with requests.get(url, params=parameters, timeout=30) as response:
        response.raise_for_status()
        raw_response = response.content
        source_url = response.url

    payload = json.loads(raw_response)
    dates.validate_requested_day(payload, target)
    fetched_at = datetime.now(timezone.utc)
    snapshot_name = fetched_at.strftime("%Y%m%dT%H%M%S_%fZ")
    output_directory = PROJECT_DIRECTORY / "data" / "raw"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{location.key}_{data_type}_{target.isoformat()}_{snapshot_name}.json"

    with output_path.open("xb") as output_file:
        output_file.write(raw_response)

    metadata = {
        "city": location.name,
        "requested_latitude": location.latitude,
        "requested_longitude": location.longitude,
        "weather_date": target.isoformat(),
        "data_type": data_type,
        "fetched_at_utc": fetched_at.isoformat(),
        "source_url": source_url,
        "raw_file": output_path.name,
    }
    metadata_path = output_path.with_suffix(".metadata.json")
    with metadata_path.open("x", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file, indent=2)
        metadata_file.write("\n")

    print(f"Saved {len(payload['hourly']['time'])} hourly {data_type} records for {location.name} on {target}.")
    print(f"Raw data: {output_path}")
    print(f"Metadata: {metadata_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", dest="weather_date", help="Weather day in YYYY-MM-DD; defaults to today in Asia/Manila")
    parser.add_argument("--city", choices=tuple(CITIES), type=str.lower, default="manila", help="City to collect; defaults to manila")
    args = parser.parse_args()
    extract_weather(args.weather_date, city=args.city)


if __name__ == "__main__":
    main()
