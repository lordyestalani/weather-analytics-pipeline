"""Recover a date range by processing each requested weather day sequentially."""

import argparse
from datetime import date, timedelta

from weather_etl import dates
from weather_etl.cities import CITIES, select_cities
from weather_etl.extract import extract_weather
from weather_etl.load import load_file
from weather_etl.transform import transform_file


def dates_in_range(start_date: str | date, end_date: str | date) -> list[date]:
    today = dates.today_in_manila()
    start = dates.resolve_weather_date(start_date, today=today)
    end = dates.resolve_weather_date(end_date, today=today)
    if start > end:
        raise ValueError("Start date must be on or before end date")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def backfill(start_date: str | date, end_date: str | date, cities: list[str] | None = None) -> int:
    # Validate the entire range before making requests or writing any database rows.
    locations = select_cities(cities)
    days = dates_in_range(start_date, end_date)
    inserted = 0
    for weather_date in days:
        for location in locations:
            print(f"Processing {location.name} weather day {weather_date}.")
            raw_file = extract_weather(weather_date, city=location.key)
            csv_file = transform_file(raw_file)
            inserted += load_file(csv_file)
    print(f"Processed {len(days)} dates for {len(locations)} cities; inserted {inserted} new snapshot rows.")
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="First weather day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="Last weather day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--cities", nargs="+", choices=tuple(CITIES), type=str.lower, default=["manila"], help="Cities to process; defaults to manila")
    args = parser.parse_args()
    backfill(args.start_date, args.end_date, cities=args.cities)


if __name__ == "__main__":
    main()
