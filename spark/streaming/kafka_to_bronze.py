"""Run the Kafka reader against the shared Iceberg Bronze/quarantine sink."""

from __future__ import annotations

import argparse
import os
import sys

from spark.common.config import SparkConfig
from spark.common.iceberg import create_spark_session
from spark.streaming.bronze_sink import start_stream
from spark.streaming.kafka_reader import (
    kafka_connector_package,
    parse_kafka_events,
    read_kafka_events,
)


def start_bronze_stream(
    spark,
    *,
    bootstrap_servers: str,
    topics: str,
    checkpoint_location: str,
    starting_offsets: str = "latest",
    catalog: str = "iceberg",
):
    """Connect Kafka → envelope validation → idempotent Bronze/quarantine sink."""
    records = read_kafka_events(
        spark,
        bootstrap_servers=bootstrap_servers,
        topics=topics,
        starting_offsets=starting_offsets,
    )
    validated = parse_kafka_events(records)
    return start_stream(validated, checkpoint_location=checkpoint_location, catalog=catalog)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
        help="Kafka bootstrap addresses (or KAFKA_BOOTSTRAP_SERVERS)",
    )
    parser.add_argument(
        "--topics",
        default=os.environ.get("KAFKA_TOPICS", "hospital.events"),
        help="comma-separated topics (or KAFKA_TOPICS)",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default=os.environ.get("KAFKA_STARTING_OFFSETS", "latest"),
    )
    parser.add_argument(
        "--checkpoint-location",
        default=os.environ.get("STREAM_CHECKPOINT_LOCATION", ""),
        help="persistent Spark checkpoint URI (or STREAM_CHECKPOINT_LOCATION)",
    )
    args = parser.parse_args(argv)
    if not args.bootstrap_servers.strip():
        parser.error("--bootstrap-servers or KAFKA_BOOTSTRAP_SERVERS is required")
    if not args.checkpoint_location.strip():
        parser.error("--checkpoint-location or STREAM_CHECKPOINT_LOCATION is required")

    import pyspark

    spark = create_spark_session(
        "HospitalKafkaToBronze",
        SparkConfig.from_env(),
        extra_packages=(kafka_connector_package(pyspark.__version__),),
    )
    try:
        query = start_bronze_stream(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topics=args.topics,
            checkpoint_location=args.checkpoint_location,
            starting_offsets=args.starting_offsets,
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
