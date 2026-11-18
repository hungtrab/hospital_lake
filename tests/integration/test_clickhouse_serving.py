"""Opt-in Spark → ClickHouse snapshot checks on a disposable local service.

Export credentials and RUN_CLICKHOUSE_TESTS=1. This writes fixture rows under a
unique timestamp, then deletes them. No Kafka or upstream aggregate is mocked.
It proves serving behavior, not the week 5 Kafka-to-dashboard integration gate.
"""
import os
from datetime import datetime, timezone, timedelta
import unittest
import uuid

from spark.streaming.clickhouse_sink import ClickHouseConfig, ClickHouseClient, write_microbatch


@unittest.skipUnless(os.environ.get("RUN_CLICKHOUSE_TESTS") == "1", "live ClickHouse opt-in")
class ClickHouseSinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("clickhouse-sink-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.config = ClickHouseConfig.from_env()
        cls.client = ClickHouseClient(cls.config)
        cls.instant = datetime(2000, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=uuid.uuid4().int % 500000000)
        cls.stamp = cls.instant.strftime("%Y-%m-%d %H:%M:%S.000000")

    @classmethod
    def tearDownClass(cls):
        cls.client.execute("ALTER TABLE hospital.lab_metric_versions DELETE WHERE window_start IN ("
                           + ",".join(f"toDateTime64('{cls.stamp[:-6]}{micro:06d}', 6, 'UTC')"
                                      for micro in (0, 1, 2, 3)) + ") SETTINGS mutations_sync=1")
        cls.spark.stop()

    def test_retry_and_out_of_order_correction_are_query_safe_before_merges(self):
        def frame(value, revision):
            return self.spark.createDataFrame([(self.instant, value, revision)],
                                             "window_start timestamp, abnormal_labs long, revision long")
        write_microbatch(frame(2, 1), 0, table="realtime_lab_metrics", config=self.config)
        # New client/batch simulates replay after process restart, not Spark recovery.
        write_microbatch(frame(2, 1), 0, table="realtime_lab_metrics", config=self.config)
        write_microbatch(frame(5, 2), 1, table="realtime_lab_metrics", config=self.config)
        write_microbatch(frame(2, 1), 0, table="realtime_lab_metrics", config=self.config)
        query = ("SELECT count(), sum(abnormal_labs) FROM hospital.realtime_lab_metrics "
                 f"WHERE window_start=toDateTime64('{self.stamp}', 6, 'UTC') FORMAT TabSeparated")
        self.assertEqual(self.client.execute(query).decode().strip(), "1\t5")

    def test_microsecond_keys_do_not_collapse_in_public_view(self):
        frame = self.spark.createDataFrame([
            (self.instant.replace(microsecond=1), 7, 1),
            (self.instant.replace(microsecond=2), 8, 1),
        ], "window_start timestamp, abnormal_labs long, revision long")
        write_microbatch(frame, 2, table="realtime_lab_metrics", config=self.config)
        query = ("SELECT count(), sum(abnormal_labs) FROM hospital.realtime_lab_metrics "
                 f"WHERE window_start IN (toDateTime64('{self.stamp[:-6]}000001', 6, 'UTC'), "
                 f"toDateTime64('{self.stamp[:-6]}000002', 6, 'UTC')) FORMAT TabSeparated")
        self.assertEqual(self.client.execute(query).decode().strip(), "2\t15")

    def test_stream_checkpoint_restart_replays_without_duplicate_view_rows(self):
        import json
        from pathlib import Path
        import tempfile
        from spark.streaming.clickhouse_sink import start_stream
        with tempfile.TemporaryDirectory(prefix="hospital-clickhouse-stream-") as root:
            source = Path(root) / "input"
            source.mkdir()
            checkpoint = str(Path(root) / "checkpoint")
            instant = self.instant.replace(microsecond=3)
            def add_file(number, value, revision):
                (source / f"{number}.json").write_text(json.dumps({
                    "window_start": instant.isoformat(), "abnormal_labs": value, "revision": revision}))
            def start():
                frame = (self.spark.readStream
                         .schema("window_start timestamp, abnormal_labs long, revision long")
                         .json(str(source)))
                return start_stream(frame, table="realtime_lab_metrics", config=self.config,
                                    checkpoint_location=checkpoint, output_mode="append")
            add_file(1, 2, 1)
            query = start()
            try:
                query.processAllAvailable()
            finally:
                query.stop()
            # Restart a real query against the same checkpoint, plus an explicit
            # replay file and a corrected snapshot. Neither should inflate SUM.
            add_file(2, 2, 1)
            add_file(3, 5, 2)
            query = start()
            try:
                query.processAllAvailable()
            finally:
                query.stop()
            sql = ("SELECT count(), sum(abnormal_labs) FROM hospital.realtime_lab_metrics "
                   f"WHERE window_start=toDateTime64('{self.stamp[:-6]}000003', 6, 'UTC') FORMAT TabSeparated")
            self.assertEqual(self.client.execute(sql).decode().strip(), "1\t5")
