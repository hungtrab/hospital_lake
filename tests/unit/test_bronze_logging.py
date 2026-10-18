import json
import unittest
from unittest.mock import MagicMock, patch

from spark.batch.ingest_bronze import main


class BronzeFailureLoggingTests(unittest.TestCase):
    def test_startup_failure_is_structured_and_preserves_original_error(self):
        error = RuntimeError("connection failed with sensitive-credential")
        with patch("spark.batch.ingest_bronze.SparkConfig.from_env"), patch(
                "spark.batch.ingest_bronze.create_spark_session", side_effect=error):
            with self.assertLogs("spark.batch.ingest_bronze", level="ERROR") as logs:
                with self.assertRaises(RuntimeError) as raised:
                    main(["--batch-id", "startup-001"])
        self.assertIs(raised.exception, error)
        event = json.loads(logs.records[0].getMessage())
        self.assertEqual((event["job"], event["batch_id"], event["status"], event["phase"]),
                         ("ingest_bronze", "startup-001", "failed", "startup"))
        self.assertEqual(event["error_type"], "RuntimeError")
        self.assertIsNone(event["table"])
        self.assertGreaterEqual(event["duration_seconds"], 0)
        self.assertNotIn("sensitive-credential", logs.records[0].getMessage())

    def test_job_failure_still_stops_session_and_propagates(self):
        spark = MagicMock()
        error = ValueError("bad input")
        with patch("spark.batch.ingest_bronze.SparkConfig.from_env"), patch(
                "spark.batch.ingest_bronze.create_spark_session", return_value=spark), patch(
                "spark.batch.ingest_bronze.ingest_bronze", side_effect=error):
            with self.assertRaises(ValueError) as raised:
                main(["--batch-id", "failed-001"])
        self.assertIs(raised.exception, error)
        spark.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
