"""Collect, validate, and load a Manila weather forecast daily at 6 AM."""

from datetime import timedelta

import pendulum

from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    dag_id="weather_etl",
    schedule=CronTriggerTimetable("0 6 * * *", timezone="Asia/Manila", run_immediately=False),
    start_date=pendulum.datetime(2026, 10, 31, 6, tz="Asia/Manila"),
    catchup=False,
    params={"weather_date": Param(None, type=["null", "string"], format="date", description="Optional weather day (YYYY-MM-DD); defaults to the run's date in Asia/Manila.")},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["weather", "etl"],
)
def weather_pipeline():
    @task
    def extract() -> str:
        from weather_etl.extract import extract_weather

        from airflow.sdk import get_current_context
        from weather_etl.dates import weather_date_for_run

        context = get_current_context()
        target = weather_date_for_run(
            context["params"].get("weather_date"),
            context.get("logical_date"),
            context["dag_run"].run_after,
        )
        return str(extract_weather(target))

    @task
    def transform(raw_file: str) -> str:
        from weather_etl.transform import transform_file

        return str(transform_file(raw_file))

    @task
    def load(csv_file: str) -> int:
        from weather_etl.load import load_file

        return load_file(csv_file)

    # TaskFlow passes each result to its dependent task through a small XCom.
    raw_file = extract()
    csv_file = transform(raw_file)
    load(csv_file)


weather_etl_dag = weather_pipeline()
