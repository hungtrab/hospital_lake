"""Idempotent Iceberg Bronze and quarantine writes for validated Kafka micro-batches.

TV3's reader passes a DataFrame with Kafka topic/partition/offset/timestamp/value
and parsed event_id/event_time/source/version/validation_error columns. A null
validation_error marks a valid envelope. The original Kafka bytes are preserved.
"""

import logging
import re
from pathlib import Path


LOG = logging.getLogger(__name__)
DDL = Path(__file__).resolve().parents[2] / "sql" / "iceberg" / "create_stream_bronze_tables.sql"
INPUT_COLUMNS = frozenset({
    "topic", "partition", "offset", "timestamp", "value", "event_id",
    "event_time", "source", "version", "validation_error",
})
TABLES = {"valid": "stream_events_raw", "invalid": "stream_events_quarantine"}


def table_name(catalog, name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("catalog must be a simple SQL identifier")
    if name not in TABLES.values():
        raise ValueError("unknown streaming Bronze table")
    return f"{catalog}.bronze.{name}"


def merge_sql(catalog, name, view):
    """Use Kafka coordinates for retry safety while retaining duplicate event IDs."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", view):
        raise ValueError("view must be a simple SQL identifier")
    target = table_name(catalog, name)
    return (f"MERGE INTO {target} t USING {view} s ON "
            "t._kafka_topic = s._kafka_topic AND "
            "t._kafka_partition = s._kafka_partition AND "
            "t._kafka_offset = s._kafka_offset "
            "WHEN NOT MATCHED THEN INSERT *")


def ensure_tables(spark, catalog="iceberg"):
    """Create the two append-only Iceberg targets before starting a stream."""
    table_name(catalog, TABLES["valid"])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")
    ddl = DDL.read_text(encoding="utf-8").replace("iceberg.bronze.", f"{catalog}.bronze.")
    for statement in ddl.split(";"):
        if statement.strip():
            spark.sql(statement.strip())


def write_microbatch(frame, batch_id, *, catalog="iceberg"):
    """Write valid raw bytes to Bronze and invalid raw bytes with reason to quarantine.

    Pass this callable to ``writeStream.foreachBatch`` after the reader has
    validated envelopes. The two MERGEs are individually retry-safe by Kafka
    coordinate, including a retry after one table succeeds and the other fails.
    """
    from pyspark.sql import functions as F

    table_name(catalog, TABLES["valid"])
    missing = INPUT_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError("streaming input missing: " + ", ".join(sorted(missing)))
    if batch_id < 0:
        raise ValueError("batch_id must be nonnegative")
    if frame.where(F.col("topic").isNull() | F.col("partition").isNull()
                   | F.col("offset").isNull()).limit(1).count():
        raise ValueError("Kafka topic, partition, and offset are required")

    shared = [
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").cast("int").alias("_kafka_partition"),
        F.col("offset").cast("long").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.col("value").cast("binary").alias("raw_payload"),
        F.col("source").alias("_source"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(batch_id).cast("long").alias("_batch_id"),
        F.col("event_id").alias("_event_id"),
        F.col("version").cast("int").alias("_schema_version"),
        F.col("event_time"),
    ]
    for kind, predicate, columns in (
        ("valid", F.col("validation_error").isNull(), shared),
        ("invalid", F.col("validation_error").isNotNull(),
         [*shared, F.col("validation_error").alias("error_reason")]),
    ):
        rows = (frame.where(predicate).select(*columns)
                .dropDuplicates(["_kafka_topic", "_kafka_partition", "_kafka_offset"])
                .persist())
        try:
            count = rows.count()
            if count:
                view = f"_hospital_{kind}_batch"
                rows.createOrReplaceTempView(view)
                frame.sparkSession.sql(merge_sql(catalog, TABLES[kind], view))
            LOG.info("job=stream_bronze batch_id=%d kind=%s rows=%d", batch_id, kind, count)
        finally:
            rows.unpersist()


def start_stream(validated, *, checkpoint_location, catalog="iceberg"):
    """Attach the Bronze/quarantine sink to TV3's validated Kafka stream."""
    if not checkpoint_location:
        raise ValueError("checkpoint_location must be a persistent path")
    table_name(catalog, TABLES["valid"])
    ensure_tables(validated.sparkSession, catalog)
    return (validated.writeStream
            .foreachBatch(lambda frame, batch_id:
                          write_microbatch(frame, batch_id, catalog=catalog))
            .option("checkpointLocation", checkpoint_location)
            .start())
