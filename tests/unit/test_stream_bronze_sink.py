"""Contract checks for the Iceberg micro-batch sink."""

import unittest
from unittest.mock import MagicMock, patch

from spark.streaming.bronze_sink import merge_sql, start_stream, table_name
from spark.streaming.kafka_reader import read_kafka_events


class StreamBronzeSinkTests(unittest.TestCase):
    def test_pruning_keeps_each_partition_range_and_retry_key(self):
        statement = merge_sql("iceberg", "stream_events_raw", "batch", [
            ("hospital.events", 0, 100, 199), ("hospital.events", 1, 300, 350)])
        self.assertIn("t._kafka_partition = 0 AND t._kafka_offset BETWEEN 100 AND 199", statement)
        self.assertIn("t._kafka_partition = 1 AND t._kafka_offset BETWEEN 300 AND 350", statement)
        self.assertIn(" OR ", statement)
        self.assertIn("t._kafka_offset = s._kafka_offset", statement)

    def test_kafka_reader_bounds_each_trigger(self):
        spark = MagicMock()
        reader = spark.readStream.format.return_value
        reader.option.return_value = reader
        with patch.dict("os.environ", {"STREAM_MAX_OFFSETS": "1500"}):
            read_kafka_events(spark, bootstrap_servers="kafka:9092", topics="hospital.events")
        reader.option.assert_any_call("maxOffsetsPerTrigger", 1500)
        for value in ("0", "-1", "invalid"):
            with self.subTest(value=value), patch.dict("os.environ", {"STREAM_MAX_OFFSETS": value}), self.assertRaises(ValueError):
                read_kafka_events(spark, bootstrap_servers="kafka:9092", topics="hospital.events")

    def test_merge_uses_kafka_coordinates_not_event_id(self):
        statement = merge_sql("iceberg", "stream_events_raw", "bronze_batch")
        self.assertIn('t._kafka_topic = s._kafka_topic', statement)
        self.assertIn('t._kafka_partition = s._kafka_partition', statement)
        self.assertIn('t._kafka_offset = s._kafka_offset', statement)
        self.assertIn('WHEN NOT MATCHED THEN INSERT *', statement)
        self.assertNotIn('t._event_id = s._event_id', statement)

    def test_catalog_and_table_identifiers_are_restricted(self):
        self.assertEqual(table_name("iceberg", "stream_events_quarantine"),
                         "iceberg.bronze.stream_events_quarantine")
        for catalog in ("iceberg; DROP TABLE x", "a.b", "1invalid"):
            with self.subTest(catalog=catalog), self.assertRaises(ValueError):
                table_name(catalog, "stream_events_raw")

    def test_stream_requires_persistent_checkpoint(self):
        with self.assertRaisesRegex(ValueError, "checkpoint"):
            start_stream(None, checkpoint_location="")


if __name__ == "__main__":
    unittest.main()
