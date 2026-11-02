"""Configured Philippine cities; add locations here instead of copying ETL code."""

from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    key: str
    name: str
    latitude: float
    longitude: float


# Cebu City and Davao City coordinates were checked with Open-Meteo geocoding.
CITIES = {
    city.key: city
    for city in (
        City("manila", "Manila", 14.5995, 120.9842),
        City("cebu", "Cebu", 10.31672, 123.89071),
        City("davao", "Davao", 7.07306, 125.61278),
    )
}


def get_city(value: str) -> City:
    key = value.strip().lower() if isinstance(value, str) else ""
    if key not in CITIES:
        raise ValueError(f"Unknown city {value!r}; choose from {', '.join(CITIES)}")
    return CITIES[key]


def select_cities(values: list[str] | None = None) -> list[City]:
    locations = [get_city(value) for value in (["manila"] if values is None else values)]
    if not locations:
        raise ValueError("Select at least one city")
    if len({city.key for city in locations}) != len(locations):
        raise ValueError("Select each city only once")
    return locations
