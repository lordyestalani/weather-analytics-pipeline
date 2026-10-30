"""Fetch Manila's hourly forecast and save an unmodified JSON snapshot."""

import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


def main():
    # Query parameters describe the data we want from the API.
    parameters = {
        "latitude": 14.5995,
        "longitude": 120.9842,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": "Asia/Manila",
        "forecast_days": 1,
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urlencode(parameters)

    # A timeout prevents a request from waiting indefinitely.
    with urlopen(url, timeout=30) as response:
        raw_response = response.read()

    # Confirm it is JSON before saving it. Keep the original response bytes.
    payload = json.loads(raw_response)
    fetched_at = datetime.now(timezone.utc)
    snapshot_name = fetched_at.strftime("%Y%m%dT%H%M%S_%fZ")
    output_directory = Path(__file__).resolve().parent / "data" / "raw"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"manila_forecast_{snapshot_name}.json"

    # Exclusive creation protects earlier snapshots from accidental overwrites.
    with output_path.open("xb") as output_file:
        output_file.write(raw_response)

    # Store provenance separately so the provider's response stays unmodified.
    metadata = {
        "city": "Manila",
        "data_type": "forecast",
        "fetched_at_utc": fetched_at.isoformat(),
        "source_url": url,
        "raw_file": output_path.name,
    }
    metadata_path = output_path.with_suffix(".metadata.json")
    with metadata_path.open("x", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file, indent=2)
        metadata_file.write("\n")

    print(f"Saved {len(payload['hourly']['time'])} hourly forecast records.")
    print(f"Raw data: {output_path}")
    print(f"Metadata: {metadata_path}")


if __name__ == "__main__":
    main()
