import contextlib
import io
import unittest
from unittest.mock import patch

from scripts.grafana_smoke import EXPECTED, env_file, main


def dashboard(uid, datasource_uid="hospital-trino", rows=2, kind="stat"):
    return {"dashboard": {"uid": uid, "panels": [{
        "title": f"{uid} panel", "type": kind, "datasource": {"uid": datasource_uid},
        "targets": [{"refId": "A", "rawSQL": "SELECT x FROM t", "format": 1}],
    }]}}, rows


def fake_request(dashboards, rows=2, search=None, field_type="number", missing_plugin=None):
    def request(_base, _auth, path, body=None):
        if path.startswith("/api/plugins/"):
            if missing_plugin and missing_plugin in path:
                raise ValueError("HTTP 404: Plugin not found")
            return {}
        if path.startswith("/api/search"):
            return [{"uid": uid} for uid in (search if search is not None else EXPECTED)]
        if path.startswith("/api/dashboards/uid/"):
            return dashboards[path.rsplit("/", 1)[1]]
        return {"results": {"A": {"frames": [{
            "schema": {"fields": [{"name": "value", "type": field_type}]},
            "data": {"values": [list(range(rows))]},
        }]}}}
    return request


def run(fake, argv=()):
    output = io.StringIO()
    with patch("scripts.grafana_smoke.request", fake), patch("scripts.grafana_smoke.env_file", return_value={}), \
            contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        code = main(list(argv))
    return code, output.getvalue()


class GrafanaSmokeTests(unittest.TestCase):
    def setUp(self):
        self.dashboards = {uid: dashboard(uid)[0] for uid in EXPECTED}

    def test_passes_when_dashboards_load_and_trino_panels_return_rows(self):
        code, output = run(fake_request(self.dashboards))
        self.assertEqual(code, 0, output)
        self.assertIn("PASS provisioned", output)

    def test_fails_when_a_dashboard_is_not_provisioned(self):
        code, output = run(fake_request(self.dashboards, search=EXPECTED[:2]))
        self.assertEqual(code, 1)
        self.assertIn(EXPECTED[2], output)

    def test_fails_when_a_trino_panel_returns_no_rows(self):
        code, output = run(fake_request(self.dashboards, rows=0))
        self.assertEqual(code, 1)
        self.assertIn("FAIL", output)

    def test_clickhouse_panels_are_reported_as_skipped_not_passed(self):
        self.dashboards[EXPECTED[0]] = dashboard(EXPECTED[0], "hospital-clickhouse")[0]
        code, output = run(fake_request(self.dashboards))
        self.assertEqual(code, 0, output)
        self.assertIn("SKIP", output)

    def test_query_errors_fail_with_the_panel_title(self):
        def failing(base, auth, path, body=None):
            if body is not None:
                raise ValueError("HTTP 400: table not found")
            return fake_request(self.dashboards)(base, auth, path)
        code, output = run(failing)
        self.assertEqual(code, 1)
        self.assertIn("table not found", output)
        self.assertIn(f"{EXPECTED[0]} panel", output)

    def test_plot_panels_fail_without_a_numeric_field(self):
        code, output = run(fake_request(self.dashboards, field_type="string"))
        self.assertEqual(code, 1)
        self.assertIn("no numeric field", output)

    def test_table_panels_may_hold_only_text(self):
        self.dashboards = {uid: dashboard(uid, kind="table")[0] for uid in EXPECTED}
        code, output = run(fake_request(self.dashboards, field_type="string"))
        self.assertEqual(code, 0, output)

    def test_missing_plugin_fails_once_the_wait_expires(self):
        code, output = run(fake_request(self.dashboards, missing_plugin="trino-datasource"), ["--plugin-timeout", "0"])
        self.assertEqual(code, 1)
        self.assertIn("plugins not installed", output)

    def test_dashboard_fetch_error_is_labelled_instead_of_raised(self):
        def failing(base, auth, path, body=None):
            if path.endswith(EXPECTED[1]):
                raise ValueError("HTTP 500: boom")
            return fake_request(self.dashboards)(base, auth, path, body)
        code, output = run(failing)
        self.assertEqual(code, 1)
        self.assertIn(f"FAIL {EXPECTED[1]}: HTTP 500", output)

    def test_panels_without_a_datasource_are_skipped(self):
        self.dashboards[EXPECTED[0]]["dashboard"]["panels"] = [{"type": "row", "title": "Section"}]
        code, output = run(fake_request(self.dashboards))
        self.assertEqual(code, 0, output)
        self.assertIn("SKIP", output)

    def test_env_file_parser_ignores_comments_and_missing_files(self):
        with patch("pathlib.Path.read_text", return_value="# note\nA=1\n\nB=x=y\n"), \
                patch("pathlib.Path.is_file", return_value=True):
            from pathlib import Path
            self.assertEqual(env_file(Path(".env")), {"A": "1", "B": "x=y"})
        with patch("pathlib.Path.is_file", return_value=False):
            from pathlib import Path
            self.assertEqual(env_file(Path(".env")), {})


if __name__ == "__main__":
    unittest.main()
