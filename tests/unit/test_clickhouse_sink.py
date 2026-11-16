"""Retry failures must be visible and retries must keep exactly the same body."""
import io
import importlib.util
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from spark.streaming.clickhouse_sink import ClickHouseClient, ClickHouseConfig


class ClickHouseClientTests(unittest.TestCase):
    def config(self, **kwargs):
        return ClickHouseConfig(password="test-secret", **kwargs)

    def test_ambiguous_failure_retries_identical_snapshot(self):
        row = {"window_start": "2026-11-16 12:00:00.000", "abnormal_labs": 3, "revision": 7}
        with patch("spark.streaming.clickhouse_sink.urlopen") as send, \
                patch("spark.streaming.clickhouse_sink.time.sleep"):
            send.side_effect = [URLError("lost response"), io.BytesIO(b"")]
            ClickHouseClient(self.config()).insert("realtime_lab_metrics", [row])
        self.assertEqual(send.call_count, 2)
        self.assertEqual(send.call_args_list[0].args[0].data, send.call_args_list[1].args[0].data)
        self.assertEqual(json.loads(send.call_args.args[0].data), row)

    def test_auth_failure_not_retried_and_body_not_exposed(self):
        with patch("spark.streaming.clickhouse_sink.urlopen", side_effect=HTTPError(
                "http://localhost", 403, "secret detail", {}, io.BytesIO(b"private"))) as send:
            with self.assertRaisesRegex(RuntimeError, "^ClickHouse HTTP 403$"):
                ClickHouseClient(self.config()).execute("SELECT 1")
        self.assertEqual(send.call_count, 1)
        self.assertNotIn("test-secret", repr(self.config()))

    def test_exhausted_retry_fails_and_does_not_ack_batch(self):
        with patch("spark.streaming.clickhouse_sink.urlopen", side_effect=URLError("down")) as send, \
                patch("spark.streaming.clickhouse_sink.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "after retries"):
                ClickHouseClient(self.config(attempts=2)).execute("SELECT 1")
        self.assertEqual(send.call_count, 2)

    def test_bad_metric_does_not_reach_server(self):
        with patch("spark.streaming.clickhouse_sink.urlopen") as send:
            for value in (-1, None, 2**64, True, 2.5):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    ClickHouseClient(self.config()).insert("realtime_lab_metrics", [{
                        "window_start": "2026-11-16 12:00:00.000", "abnormal_labs": value, "revision": 0}])
        send.assert_not_called()

    def test_config_rejects_credentials_in_url_and_empty_password(self):
        for kwargs in ({"endpoint": "http://user:secret@localhost:8123"}, {"attempts": 0}):
            with self.assertRaises(ValueError):
                self.config(**kwargs)
        with self.assertRaises(ValueError):
            ClickHouseConfig()


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark not installed")
class ClickHouseSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("serving-contract-test")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, values):
        from datetime import datetime, timezone
        instant = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
        return self.spark.createDataFrame([(instant, count, revision) for count, revision in values],
                                         "window_start timestamp, abnormal_labs long, revision long")

    def test_chunks_and_utc_are_independent_of_host_timezone(self):
        from spark.streaming.clickhouse_sink import write_microbatch
        config = ClickHouseConfig(password="secret", chunk_rows=1)
        with patch("spark.streaming.clickhouse_sink.ClickHouseClient.execute") as send:
            write_microbatch(self.frame([(2, 1), (3, 2)]), 0,
                             table="realtime_lab_metrics", config=config)
        self.assertEqual(send.call_count, 2)
        rows = [json.loads(call.args[1]) for call in send.call_args_list]
        self.assertEqual([row["revision"] for row in rows], [1, 2])
        self.assertEqual(rows[0]["window_start"], "2026-11-16 12:00:00.000")

    def test_conflicting_key_revision_fails_before_writing(self):
        from spark.streaming.clickhouse_sink import write_microbatch
        with patch("spark.streaming.clickhouse_sink.ClickHouseClient.execute") as send:
            with self.assertRaisesRegex(ValueError, "conflicting"):
                write_microbatch(self.frame([(2, 1), (3, 1)]), 0,
                                 table="realtime_lab_metrics", config=ClickHouseConfig(password="secret"))
        send.assert_not_called()
