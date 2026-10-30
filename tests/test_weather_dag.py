"""DAG checks run inside the Airflow image; local Python skips them."""

import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Run this check inside the Airflow image")
class WeatherDagTests(unittest.TestCase):
    def test_dag_connects_extract_transform_and_load(self):
        from airflow.configuration import conf
        from airflow.dag_processing.dagbag import DagBag

        bag = DagBag(dag_folder=conf.get("core", "dags_folder"))
        self.assertEqual(bag.import_errors, {})
        self.assertIn("weather_etl", bag.dags)
        dag = bag.dags["weather_etl"]
        self.assertEqual(set(dag.task_ids), {"extract", "transform", "load"})
        self.assertEqual(dag.get_task("extract").upstream_task_ids, set())
        self.assertEqual(dag.get_task("transform").upstream_task_ids, {"extract"})
        self.assertEqual(dag.get_task("load").upstream_task_ids, {"transform"})


    def test_daily_schedule_starts_at_six_in_manila_and_advances(self):
        from unittest.mock import patch

        import pendulum
        from airflow.configuration import conf
        from airflow.dag_processing.dagbag import DagBag
        from airflow.timetables.base import DataInterval, TimeRestriction

        dag = DagBag(dag_folder=conf.get("core", "dags_folder")).dags["weather_etl"]
        restriction = TimeRestriction(earliest=dag.start_date, latest=None, catchup=False)
        cases = [
            (pendulum.datetime(2026, 10, 30, 21, tz="Asia/Manila"), None, "2026-10-31T06:00:00+08:00"),
            (pendulum.datetime(2026, 10, 31, 7, tz="Asia/Manila"), DataInterval.exact(pendulum.datetime(2026, 10, 31, 6, tz="Asia/Manila")), "2026-11-01T06:00:00+08:00"),
        ]
        for now, last_interval, expected in cases:
            with self.subTest(now=now), patch("airflow.timetables.trigger.utcnow", return_value=now):
                info = dag.timetable.next_dagrun_info(last_automated_data_interval=last_interval, restriction=restriction)
                self.assertIsNotNone(info)
                self.assertEqual(info.run_after.in_timezone("Asia/Manila").isoformat(), expected)

if __name__ == "__main__":
    unittest.main()
