"""Orchestration checks for the streaming Kafka → Bronze entry point."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from spark.streaming import kafka_to_bronze


class KafkaToBronzeTests(unittest.TestCase):
    @patch("spark.streaming.kafka_to_bronze.start_stream")
    @patch("spark.streaming.kafka_to_bronze.parse_kafka_events")
    @patch("spark.streaming.kafka_to_bronze.read_kafka_events")
    def test_start_wires_raw_reader_through_validation_to_sink(
        self, read_events, parse_events, sink
    ):
        spark = object()
        raw = object()
        validated = object()
        query = object()
        read_events.return_value = raw
        parse_events.return_value = validated
        sink.return_value = query

        result = kafka_to_bronze.start_bronze_stream(
            spark,
            bootstrap_servers="kafka:9092",
            topics="hospital.events",
            checkpoint_location="s3a://checkpoints/hospital-stream",
            starting_offsets="earliest",
        )

        self.assertIs(result, query)
        read_events.assert_called_once_with(
            spark,
            bootstrap_servers="kafka:9092",
            topics="hospital.events",
            starting_offsets="earliest",
        )
        parse_events.assert_called_once_with(raw)
        sink.assert_called_once_with(
            validated,
            checkpoint_location="s3a://checkpoints/hospital-stream",
            catalog="iceberg",
        )

    @patch("spark.streaming.kafka_to_bronze.start_bronze_stream")
    @patch("spark.streaming.kafka_to_bronze.create_spark_session")
    @patch("spark.streaming.kafka_to_bronze.SparkConfig.from_env")
    def test_main_adds_matching_kafka_connector_and_waits(
        self, config_from_env, create_session, start_stream
    ):
        spark = MagicMock()
        query = MagicMock()
        create_session.return_value = spark
        start_stream.return_value = query
        config = object()
        config_from_env.return_value = config
        with patch.dict(sys.modules, {"pyspark": SimpleNamespace(__version__="4.0.2")}):
            kafka_to_bronze.main([
                "--bootstrap-servers", "kafka:9092",
                "--topics", "hospital.events",
                "--starting-offsets", "earliest",
                "--checkpoint-location", "s3a://checkpoints/hospital-stream",
            ])

        create_session.assert_called_once_with(
            "HospitalKafkaToBronze",
            config,
            extra_packages=("org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",),
        )
        start_stream.assert_called_once_with(
            spark,
            bootstrap_servers="kafka:9092",
            topics="hospital.events",
            checkpoint_location="s3a://checkpoints/hospital-stream",
            starting_offsets="earliest",
        )
        query.awaitTermination.assert_called_once_with()
        spark.stop.assert_called_once_with()

    def test_main_requires_persistent_checkpoint(self):
        with self.assertRaises(SystemExit):
            kafka_to_bronze.main(["--bootstrap-servers", "kafka:9092"])


if __name__ == "__main__":
    unittest.main()
