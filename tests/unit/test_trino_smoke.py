import contextlib
import io
import subprocess
import unittest
from unittest.mock import patch

from scripts.trino_smoke import main


class TrinoSmokeTests(unittest.TestCase):
    def invoke(self, *extra):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            code = main(["--table", "iceberg.bronze.spark_smoke", "--expected-rows", "3", *extra])
        return code, output.getvalue()

    @patch("scripts.trino_smoke.subprocess.run")
    def test_success_checks_exact_count_and_read_only_query(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '"3"\n', '')
        code, output = self.invoke("--server", "http://localhost:8080", "--timeout", "5")
        self.assertEqual(code, 0)
        self.assertIn("PASS", output)
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("--execute") + 1],
                         'SELECT count(*) FROM "iceberg"."bronze"."spark_smoke"')
        self.assertEqual(run.call_args.kwargs["timeout"], 5)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_wrong_or_malformed_results_fail(self, run):
        for result in ('"2"\n', '', '"3"\n"4"\n', '"3","4"\n', 'NULL\n', '"3.0"\n'):
            with self.subTest(result=result):
                run.return_value = subprocess.CompletedProcess([], 0, result, '')
                self.assertEqual(self.invoke()[0], 1)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_query_failure_preserves_diagnostic(self, run):
        run.return_value = subprocess.CompletedProcess([], 1, '', 'Table does not exist')
        code, output = self.invoke()
        self.assertEqual(code, 1)
        self.assertIn("Table does not exist", output)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_missing_cli_and_timeout_fail(self, run):
        for error in (FileNotFoundError("trino missing"), subprocess.TimeoutExpired("trino", 5)):
            with self.subTest(error=error):
                run.side_effect = error
                code, output = self.invoke()
                self.assertEqual(code, 1)
                self.assertIn("FAIL", output)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_invalid_arguments_never_launch_cli(self, run):
        for args in (("--table", "iceberg.bronze.x;DROP TABLE x"),
                     ("--table", "bronze.x"), ("--expected-rows", "-1"),
                     ("--timeout", "0")):
            with self.subTest(args=args), self.assertRaises(SystemExit) as error:
                self.invoke(*args)
            self.assertEqual(error.exception.code, 2)
        run.assert_not_called()

    @patch("scripts.trino_smoke.subprocess.run")
    def test_empty_table_allowed_only_when_explicitly_expected(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '"0"\n', '')
        self.assertEqual(self.invoke("--expected-rows", "0")[0], 0)


if __name__ == "__main__":
    unittest.main()
