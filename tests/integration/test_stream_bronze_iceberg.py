"""Real local Spark/Iceberg sink test; no Kafka or Docker service needed.

Install requirements-spark.txt, then set RUN_STREAM_BRONZE_TESTS=1. The first
run downloads the Iceberg Spark runtime from Maven.
"""

import os
import tempfile
import unittest
from datetime import datetime, timezone

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType,
                                   StructField, StructType, TimestampType)
except ImportError:
    SparkSession = None

from spark.common.iceberg import ICEBERG_VERSION
from spark.streaming.bronze_sink import ensure_tables, write_microbatch


@unittest.skipUnless(os.environ.get("RUN_STREAM_BRONZE_TESTS") == "1" and SparkSession,
                     "set RUN_STREAM_BRONZE_TESTS=1 with requirements-spark.txt installed")
class StreamBronzeIcebergTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.storage = tempfile.TemporaryDirectory(prefix="hospital-stream-bronze-")
        builder = (SparkSession.builder.master("local[2]").appName("stream-bronze-integration")
                   .config("spark.ui.enabled", "false")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                   .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                   .config("spark.sql.catalog.iceberg.type", "hadoop")
                   .config("spark.sql.catalog.iceberg.warehouse", cls.storage.name + "/warehouse"))
        local_jar = os.environ.get("ICEBERG_RUNTIME_JAR")
        if local_jar:
            builder = builder.config("spark.jars", local_jar)
        else:
            builder = builder.config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}")
        cls.spark = builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        ensure_tables(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.storage.cleanup()

    def setUp(self):
        self.spark.sql("DELETE FROM iceberg.bronze.stream_events_raw WHERE true")
        self.spark.sql("DELETE FROM iceberg.bronze.stream_events_quarantine WHERE true")

    def fixture(self):
        instant = datetime(2026, 10, 18, 8, tzinfo=timezone.utc)
        schema = StructType([
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("value", BinaryType()),
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("source", StringType()),
            StructField("version", IntegerType()),
            StructField("validation_error", StringType()),
        ])
        return self.spark.createDataFrame([
            ("hospital.encounter", 0, 1, instant, b'{"event_id":"same"}', "same", instant, "sim", 1, None),
            ("hospital.encounter", 0, 2, instant, b"{broken", None, None, None, None, "malformed JSON"),
            ("hospital.encounter", 0, 3, instant, b'{"event_id":"same"}', "same", instant, "sim", 1, None),
        ], schema)

    def test_raw_bytes_quarantine_and_retry_by_kafka_offset(self):
        frame = self.fixture()
        write_microbatch(frame, 7)
        write_microbatch(frame, 7)
        write_microbatch(frame, 8)
        raw = self.spark.table("iceberg.bronze.stream_events_raw").orderBy("_kafka_offset").collect()
        bad = self.spark.table("iceberg.bronze.stream_events_quarantine").collect()
        self.assertEqual([row._kafka_offset for row in raw], [1, 3])
        self.assertEqual([bytes(row.raw_payload) for row in raw],
                         [b'{"event_id":"same"}', b'{"event_id":"same"}'])
        self.assertEqual([row._event_id for row in raw], ["same", "same"])
        self.assertEqual(len(bad), 1)
        self.assertEqual((bad[0]._kafka_offset, bytes(bad[0].raw_payload), bad[0].error_reason),
                         (2, b"{broken", "malformed JSON"))

    def test_retry_after_raw_table_commits(self):
        frame = self.fixture()
        write_microbatch(frame.where("validation_error IS NULL"), 9)
        write_microbatch(frame, 9)
        self.assertEqual(self.spark.table("iceberg.bronze.stream_events_raw").count(), 2)
        self.assertEqual(self.spark.table("iceberg.bronze.stream_events_quarantine").count(), 1)


if __name__ == "__main__":
    unittest.main()
