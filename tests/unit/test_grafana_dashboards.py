"""Keep generated Grafana assets aligned with the implemented Gold contract."""

import json
import unittest
from pathlib import Path

import yaml

from dashboard.grafana.build_dashboards import CLICKHOUSE, TRINO, build


ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = ROOT / "dashboard" / "grafana" / "dashboards"


class GrafanaDashboardTests(unittest.TestCase):
    def test_generated_files_are_current_and_queries_are_live(self):
        definitions = build()
        self.assertEqual(set(definitions), {path.name for path in DASHBOARDS.glob("*.json")})
        for filename, expected in definitions.items():
            actual = json.loads((DASHBOARDS / filename).read_text(encoding="utf-8"))
            self.assertEqual(actual, expected)
            self.assertTrue(actual["panels"])
            for panel in actual["panels"]:
                self.assertIn(panel["datasource"], (TRINO, CLICKHOUSE))
                query = panel["targets"][0]
                sql = query.get("rawSQL", query.get("rawSql"))
                self.assertIn("SELECT", sql)
                self.assertIn("FROM", sql)
                self.assertNotIn("SELECT 1", sql)

    def test_historical_queries_reference_existing_gold_tables(self):
        ddl = (ROOT / "sql" / "iceberg" / "create_gold_tables.sql").read_text(encoding="utf-8")
        historical = build()["historical-analytics.json"]
        self.assertEqual(len(historical["panels"]), 6)
        for panel in historical["panels"]:
            sql = panel["targets"][0]["rawSQL"]
            table = sql.split("FROM iceberg.gold.", 1)[1].split()[0]
            self.assertIn(f"iceberg.gold.{table} (", ddl)

    def test_overview_covers_plan_panels(self):
        titles = [panel["title"] for panel in build()["hospital-overview.json"]["panels"]]
        for expected in ("Current patients", "ICU occupancy", "Available ICU beds",
                         "Admissions (latest Gold day)", "Discharges (latest Gold day)",
                         "Admissions by hour", "Department occupancy"):
            self.assertIn(expected, titles)

    def test_panel_ids_are_unique_and_layout_fits_grid(self):
        for name, definition in build().items():
            ids = [panel["id"] for panel in definition["panels"]]
            self.assertEqual(len(ids), len(set(ids)), name)
            for panel in definition["panels"]:
                self.assertLessEqual(panel["gridPos"]["x"] + panel["gridPos"]["w"], 24, panel["title"])

    def test_every_query_declares_its_result_format(self):
        for definition in build().values():
            for panel in definition["panels"]:
                expected = 0 if panel["type"] == "timeseries" else 1
                self.assertEqual(panel["targets"][0]["format"], expected, panel["title"])

    def test_batch_dashboards_cover_the_generated_data_window(self):
        config = yaml.safe_load((ROOT / "generators" / "batch_generator" / "config.yaml").read_text(encoding="utf-8"))
        dashboards = build()
        historical = dashboards["historical-analytics.json"]["time"]
        self.assertEqual(historical["from"], f'{config["start_date"]}T00:00:00Z')
        self.assertEqual(historical["to"], f'{config["end_date"]}T00:00:00Z')
        self.assertEqual(dashboards["hospital-overview.json"]["time"]["to"], historical["to"])

    def test_datasource_uids_match_provisioning(self):
        provisioned = yaml.safe_load(
            (ROOT / "dashboard" / "grafana" / "provisioning" / "datasources" / "hospital.yaml").read_text(encoding="utf-8")
        )["datasources"]
        by_uid = {source["uid"]: source["type"] for source in provisioned}
        for source in (TRINO, CLICKHOUSE):
            self.assertEqual(by_uid[source["uid"]], source["type"])

    def test_grafana_never_blocks_compose_for_older_env_files(self):
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        grafana = compose["services"]["grafana"]
        self.assertNotIn(":?", json.dumps(grafana["environment"]))
        self.assertTrue(all(port.startswith("127.0.0.1:") for port in grafana["ports"]))


if __name__ == "__main__":
    unittest.main()
