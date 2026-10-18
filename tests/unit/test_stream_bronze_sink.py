"""Contract checks for the Iceberg micro-batch sink."""

import unittest

from spark.streaming.bronze_sink import merge_sql, start_stream, table_name


class StreamBronzeSinkTests(unittest.TestCase):
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
