import contextlib
import io
import unittest
from unittest.mock import patch

from scripts.grafana_smoke import EXPECTED, env_file, main


def dashboard(uid, datasource_uid="hospital-trino", rows=2):
    return {"dashboard": {"uid": uid, "panels": [{
        "title": f"{uid} panel", "datasource": {"uid": datasource_uid},
        "targets": [{"refId": "A", "rawSQL": "SELECT x FROM t", "format": 1}],
    }]}}, rows


def fake_request(dashboards, rows=2, search=None):
    def request(_base, _auth, path, body=None):
        if path.startswith("/api/search"):
            return [{"uid": uid} for uid in (search if search is not None else EXPECTED)]
        if path.startswith("/api/dashboards/uid/"):
            return dashboards[path.rsplit("/", 1)[1]]
        return {"results": {"A": {"frames": [{"data": {"values": [list(range(rows))]}}]}}}
    return request


def run(fake):
    output = io.StringIO()
    with patch("scripts.grafana_smoke.request", fake), patch("scripts.grafana_smoke.env_file", return_value={}), \
            contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        code = main([])
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
