"""Manually collect, validate, and load a Manila weather forecast snapshot."""

from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="weather_etl",
    schedule=None,
    start_date=datetime(2026, 10, 30, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["weather", "etl"],
)
def weather_pipeline():
    @task
    def extract() -> str:
        from weather_etl.extract import extract_weather

        return str(extract_weather())

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
