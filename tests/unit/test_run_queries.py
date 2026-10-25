"""Unit tests for scripts/run_queries.py (make query) without a Trino service."""

import io
import subprocess
import unittest
from contextlib import redirect_stdout
from unittest import mock

from scripts import run_queries

SQL = """
-- 1. First.
SELECT 1;

-- 2. Second.
SELECT 2;
"""


def fake_run(outputs):
    def run(command, **kwargs):
        sql = command[command.index("--execute") + 1]
        returncode, stdout = outputs[sql.splitlines()[-1]]
        return subprocess.CompletedProcess(command, returncode, stdout, "boom" if returncode else "")
    return run


class RunQueriesTests(unittest.TestCase):
    def test_load_queries_requires_titles(self):
        self.assertEqual([title for title, _ in run_queries.load_queries(SQL)], ["1. First.", "2. Second."])
        with self.assertRaises(ValueError):
            run_queries.load_queries("SELECT 1;")
        with self.assertRaises(ValueError):
            run_queries.load_queries("  ;  ")

    def test_all_queries_with_rows_pass_and_use_csv_header(self):
        calls = []
        outputs = {"SELECT 1": (0, '"a"\n"1"\n'), "SELECT 2": (0, '"b","c"\n"2","x"\n"3","y"\n')}
        runner = fake_run(outputs)

        def record(command, **kwargs):
            calls.append(command)
            return runner(command, **kwargs)

        out = io.StringIO()
        with mock.patch.object(run_queries.subprocess, "run", side_effect=record), \
                mock.patch.object(run_queries.Path, "read_text", return_value=SQL), redirect_stdout(out):
            code = run_queries.main(["--server", "http://trino:8080", "--sql", "queries.sql"])
        self.assertEqual(code, 0)
        self.assertIn("2/2 queries returned rows", out.getvalue())
        self.assertIn("(2 rows", out.getvalue())
        self.assertTrue(all(c[c.index("--output-format") + 1] == "CSV_HEADER" for c in calls))

    def test_empty_result_or_error_fails(self):
        for second in ((0, '"b"\n'), (1, "")):
            out = io.StringIO()
            outputs = {"SELECT 1": (0, '"a"\n"1"\n'), "SELECT 2": second}
            with mock.patch.object(run_queries.subprocess, "run", side_effect=fake_run(outputs)), \
                    mock.patch.object(run_queries.Path, "read_text", return_value=SQL), redirect_stdout(out):
                code = run_queries.main(["--server", "http://trino:8080", "--sql", "queries.sql"])
            self.assertEqual(code, 1, second)
            self.assertIn("FAIL 2. Second.", out.getvalue())
            self.assertIn("1/2 queries returned rows", out.getvalue())


if __name__ == "__main__":
    unittest.main()
