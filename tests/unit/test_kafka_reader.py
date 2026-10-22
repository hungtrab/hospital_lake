"""Spark-local checks for Kafka envelope parsing and event-time deduplication."""

from datetime import datetime, timezone
import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class KafkaReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("hospital-lake-kafka-reader-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.ansi.enabled", "true")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_envelope_parse_preserves_raw_record_and_quarantines_bad_values(self):
        from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType,
                                       StructField, StructType, TimestampType)

        from spark.streaming.kafka_reader import parse_kafka_events

        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("value", BinaryType(), True),
        ])
        now = datetime(2026, 10, 22, 7, 0, tzinfo=timezone.utc)
        rows = [
            ("hospital.encounter", 0, 1, now,
             b'{"event_id":"e1","event_type":"ADMISSION","event_time":"2026-10-22T07:00:00Z","source":"sim","version":1,"payload":{"patient_id":"p1"}}'),
            ("hospital.encounter", 0, 2, now,
             b'{"event_id":"e2","event_type":"NOT_SUPPORTED","event_time":"bad-time","source":"sim","version":"bad","payload":[]}'),
            ("hospital.encounter", 0, 3, now, b"{"),
        ]
        source = self.spark.createDataFrame(rows, schema)

        parsed_frame = parse_kafka_events(source)
        parsed = {row.offset: row for row in parsed_frame.collect()}
        valid = parsed[1]
        self.assertIsNone(valid.validation_error)
        self.assertEqual(valid.event_id, "e1")
        self.assertEqual(valid.event_type, "ADMISSION")
        self.assertEqual(valid.version, 1)
        self.assertEqual(valid.value, rows[0][4])
        utc_event_time = (parsed_frame.where("offset = 1")
                          .selectExpr("date_format(event_time, 'yyyy-MM-dd HH:mm:ss') as event_time")
                          .first().event_time)
        self.assertEqual(utc_event_time, "2026-10-22 07:00:00")

        invalid = parsed[2]
        self.assertCountEqual(invalid.validation_error.split("|"), [
            "UNSUPPORTED_EVENT_TYPE", "INVALID_EVENT_TIME", "INVALID_VERSION", "INVALID_PAYLOAD",
        ])
        self.assertIn("INVALID_JSON", parsed[3].validation_error.split("|"))

    def test_static_dedup_discards_invalid_rows_and_duplicate_event_ids(self):
        from pyspark.sql.types import (StringType, StructField, StructType,
                                       TimestampType)

        from spark.streaming.kafka_reader import deduplicate_events

        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("validation_error", StringType(), True),
        ])
        event_time = datetime(2026, 10, 22, 7, 0, tzinfo=timezone.utc)
        frame = self.spark.createDataFrame([
            ("same", event_time, None),
            ("same", event_time, None),
            ("bad", event_time, "INVALID_EVENT_TIME"),
        ], schema)

        result = deduplicate_events(frame)
        self.assertEqual([row.event_id for row in result.collect()], ["same"])

    def test_streaming_dedup_attaches_watermark_state(self):
        from spark.streaming.kafka_reader import deduplicate_events

        rate = self.spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        events = rate.selectExpr(
            "cast(value as string) as event_id",
            "timestamp as event_time",
            "cast(null as string) as validation_error",
        )

        result = deduplicate_events(events, watermark_delay="10 minutes")
        self.assertTrue(result.isStreaming)
        self.assertIn("EventTimeWatermark", result._jdf.logicalPlan().toString())

    def test_package_coordinates_match_supported_spark_lines(self):
        from spark.streaming.kafka_reader import kafka_connector_package

        self.assertEqual(kafka_connector_package("3.5.5"),
                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        self.assertEqual(kafka_connector_package("4.0.2"),
                         "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2")
        with self.assertRaisesRegex(ValueError, "Spark 3.5.x and 4.0.x"):
            kafka_connector_package("3.4.3")


if __name__ == "__main__":
    unittest.main()
