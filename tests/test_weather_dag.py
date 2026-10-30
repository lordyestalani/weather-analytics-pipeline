"""DAG checks run inside the Airflow image; local Python skips them."""

import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Run this check inside the Airflow image")
class WeatherDagTests(unittest.TestCase):
    def test_manual_dag_connects_extract_transform_and_load(self):
        from airflow.configuration import conf
        from airflow.dag_processing.dagbag import DagBag

        bag = DagBag(dag_folder=conf.get("core", "dags_folder"))
        self.assertEqual(bag.import_errors, {})
        self.assertIn("weather_etl", bag.dags)
        dag = bag.dags["weather_etl"]
        self.assertIsNone(dag.schedule)
        self.assertEqual(set(dag.task_ids), {"extract", "transform", "load"})
        self.assertEqual(dag.get_task("extract").upstream_task_ids, set())
        self.assertEqual(dag.get_task("transform").upstream_task_ids, {"extract"})
        self.assertEqual(dag.get_task("load").upstream_task_ids, {"transform"})


if __name__ == "__main__":
    unittest.main()
