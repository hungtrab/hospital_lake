"""Contract checks for the TV4 Bronze verifier; live Trino is opt-in via its CLI."""

import csv
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.verify_bronze import main


class VerifyBronzeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.source = Path(self.temporary.name)
        with (self.source / "patients.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=("patient_id", "gender"))
            writer.writeheader()
            writer.writerows(({"patient_id": "p2", "gender": "F"},
                              {"patient_id": "p1", "gender": "M"}))

    def invoke(self, *extra):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return main(["--input-dir", str(self.source), "--batch-id", "batch-1",
                         "--entities", "patients", *extra])

    @patch("scripts.verify_bronze.query")
    def test_matching_count_and_ids_pass_with_batch_scope(self, query):
        query.side_effect = [ [["2"]], [["p1"], ["p2"]] ]
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(query.call_count, 2)
        self.assertIn('"iceberg"."bronze"."patients_raw"', query.call_args_list[0].args[1])
        self.assertIn("_batch_id = 'batch-1'", query.call_args_list[0].args[1])
        self.assertIn('"patient_id"', query.call_args_list[1].args[1])

    @patch("scripts.verify_bronze.query")
    def test_missing_row_fails(self, query):
        query.return_value = [["1"]]
        self.assertEqual(self.invoke(), 1)
        self.assertEqual(query.call_count, 1)

    @patch("scripts.verify_bronze.query")
    def test_same_count_wrong_id_fails(self, query):
        query.side_effect = [ [["2"]], [["p1"], ["p3"]] ]
        self.assertEqual(self.invoke(), 1)

    @patch("scripts.verify_bronze.query")
    def test_duplicate_id_fails_even_when_count_matches(self, query):
        query.side_effect = [ [["2"]], [["p1"], ["p1"]] ]
        self.assertEqual(self.invoke(), 1)

    @patch("scripts.verify_bronze.query")
    def test_invalid_source_id_and_missing_source_fail_before_query(self, query):
        with (self.source / "patients.csv").open("a", encoding="utf-8") as handle:
            handle.write(",F\n")
        self.assertEqual(self.invoke(), 1)
        (self.source / "patients.csv").unlink()
        self.assertEqual(self.invoke(), 1)
        query.assert_not_called()

    @patch("scripts.verify_bronze.query")
    def test_bad_result_and_query_failure_fail(self, query):
        query.return_value = [["not-an-integer"]]
        self.assertEqual(self.invoke(), 1)
        query.side_effect = ValueError("table unavailable")
        self.assertEqual(self.invoke(), 1)

    @patch("scripts.verify_bronze.query")
    def test_parquet_input(self, query):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            self.skipTest("pyarrow not installed")
        (self.source / "patients.csv").unlink()
        pq.write_table(pa.table({"patient_id": ["p1", "p2"]}), self.source / "patients.parquet")
        query.side_effect = [ [["2"]], [["p2"], ["p1"]] ]
        self.assertEqual(self.invoke(), 0)

    @patch("scripts.verify_bronze.query")
    def test_batch_id_is_escaped_as_sql_string(self, query):
        query.side_effect = [ [["2"]], [["p1"], ["p2"]] ]
        self.assertEqual(self.invoke("--batch-id", "run'42"), 0)
        self.assertIn("_batch_id = 'run''42'", query.call_args_list[0].args[1])


if __name__ == "__main__":
    unittest.main()
