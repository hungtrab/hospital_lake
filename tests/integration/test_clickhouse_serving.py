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
        cls.stamp = cls.instant.strftime("%Y-%m-%d %H:%M:%S.000")

    @classmethod
    def tearDownClass(cls):
        cls.client.execute("ALTER TABLE hospital.lab_metric_versions DELETE WHERE window_start = "
                           f"toDateTime64('{cls.stamp}', 3, 'UTC') SETTINGS mutations_sync=1")
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
                 f"WHERE window_start=toDateTime64('{self.stamp}', 3, 'UTC') FORMAT TabSeparated")
        self.assertEqual(self.client.execute(query).decode().strip(), "1\t5")
