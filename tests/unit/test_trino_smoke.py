import contextlib
import io
import subprocess
import unittest
from pathlib import Path
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

    @patch("scripts.trino_smoke.subprocess.run")
    def test_stack_checks_engine_catalog_count_and_data(self, run):
        run.side_effect = [subprocess.CompletedProcess([], 0, value, '') for value in
                           ('"1"\n', '"bronze"\n"information_schema"\n', '"3"\n', '"patient-1"\n')]
        code, output = self.invoke("--check-stack")
        self.assertEqual(code, 0)
        queries = [call.args[0][-1] for call in run.call_args_list]
        self.assertEqual(queries, ['SELECT 1', 'SHOW SCHEMAS FROM "iceberg"',
                                   'SELECT count(*) FROM "iceberg"."bronze"."spark_smoke"',
                                   'SELECT * FROM "iceberg"."bronze"."spark_smoke" LIMIT 1'])
        for stage in ("engine", "catalog", "count", "data"):
            self.assertIn(f"PASS {stage}", output)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_stack_stops_at_failed_stage(self, run):
        for index, stage in enumerate(("engine", "catalog", "count", "data")):
            values = ('"1"\n', '"bronze"\n', '"3"\n')
            run.reset_mock()
            run.side_effect = [subprocess.CompletedProcess([], 0, value, '') for value in values[:index]] + [
                subprocess.CompletedProcess([], 1, '', 'dependency unavailable')]
            with self.subTest(stage=stage):
                code, output = self.invoke("--check-stack")
                self.assertEqual(code, 1)
                self.assertIn(f"FAIL {stage}", output)
                self.assertEqual(run.call_count, index + 1)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_stack_rejects_missing_schema_and_empty_sample(self, run):
        for values, failed_stage in ((('"1"\n', '"silver"\n'), "catalog"),
                                     (('"1"\n', '"bronze"\n', '"3"\n', ''), "data")):
            run.side_effect = [subprocess.CompletedProcess([], 0, value, '') for value in values]
            code, output = self.invoke("--check-stack")
            self.assertEqual(code, 1)
            self.assertIn(f"FAIL {failed_stage}", output)

    @patch("scripts.trino_smoke.subprocess.run")
    def test_compose_uses_container_port_and_absolute_config(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '"3"\n', '')
        with patch.dict("os.environ", {"TRINO_PORT": "9090"}):
            self.assertEqual(self.invoke("--compose")[0], 0)
        command = run.call_args.args[0]
        self.assertEqual(command[:2], ["docker", "compose"])
        self.assertTrue(Path(command[command.index("-f") + 1]).is_absolute())
        self.assertIn("-T", command)
        self.assertEqual(command[command.index("--server") + 1], "http://localhost:8080")


if __name__ == "__main__":
    unittest.main()
