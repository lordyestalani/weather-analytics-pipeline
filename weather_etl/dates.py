"""Philippine weather dates shared by the CLI, backfill command, and Airflow."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

MANILA = ZoneInfo("Asia/Manila")


def today_in_manila() -> date:
    return datetime.now(MANILA).date()


def resolve_weather_date(value: str | date | None, today: date | None = None) -> date:
    today = today if today is not None else today_in_manila()
    if value is None:
        return today
    if isinstance(value, date) and not isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = date.fromisoformat(value)
        if result.isoformat() != value:
            raise ValueError("Weather dates must use YYYY-MM-DD")
    else:
        raise ValueError("Weather dates must be a date or YYYY-MM-DD string")
    if result > today:
        raise ValueError("Weather dates cannot be in the future")
    return result


def weather_date_for_run(override, logical_date, run_after) -> date:
    """Keep the intended run date stable even if execution or retries are delayed."""
    if override is not None:
        return resolve_weather_date(override)
    run_time = logical_date if logical_date is not None else run_after
    if run_time is None or run_time.utcoffset() is None:
        raise ValueError("An aware logical date or trigger time is required")
    return resolve_weather_date(run_time.astimezone(MANILA).date())


def validate_requested_day(payload: dict, weather_date: date) -> None:
    """Do not label the wrong day or a partial response as a complete daily slice."""
    expected = [f"{weather_date.isoformat()}T{hour:02}:00" for hour in range(24)]
    if payload.get("timezone") != "Asia/Manila" or payload["hourly"]["time"] != expected:
        raise ValueError(f"Expected all 24 local weather hours for {weather_date}")
