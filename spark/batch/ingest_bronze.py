"""Append local raw Parquet to existing Iceberg Bronze tables, once per batch ID.

Run from the repo root: python -m spark.batch.ingest_bronze --batch-id demo-001
Inputs must remain immutable and runs for the same table must be serialized.
Each table append is atomic; a retry skips tables already committed by the batch.
"""

import argparse
import json
import logging
from pathlib import Path
from time import monotonic

from spark.common.config import SparkConfig
from spark.common.iceberg import create_spark_session

LOGGER = logging.getLogger(__name__)
ENTITIES = ("patients", "encounters", "lab_results", "medications", "vitals", "bed_events")
METADATA_TYPES = {
    "_source": "string", "_ingested_at": "timestamp", "_batch_id": "string",
    "_event_id": "string", "_schema_version": "int",
}


def source_path(input_dir: Path, entity: str) -> Path:
    """Accept one .parquet file/Spark directory, or an entity partition directory."""
    candidates = [path for path in (input_dir / f"{entity}.parquet", input_dir / entity)
                  if path.exists()]
    if len(candidates) != 1:
        raise ValueError(f"Expected exactly one input for {entity}: "
                         f"{input_dir / (entity + '.parquet')} or {input_dir / entity}")
    return candidates[0].resolve()


def validate_schema(source, target, table: str) -> None:
    """Never silently cast, drop extra fields, or overwrite source metadata."""
    names = [field.name.casefold() for field in source.fields]
    if len(set(names)) != len(names) or set(names) & METADATA_TYPES.keys():
        raise ValueError(f"{table}: duplicate or reserved source columns")
    actual = {field.name: field.dataType.simpleString() for field in source.fields}
    expected = {field.name: field.dataType.simpleString() for field in target.fields}
    if expected != {**actual, **METADATA_TYPES}:
        raise ValueError(f"{table}: source/target schema mismatch; "
                         f"source={actual}, target={expected}, metadata={METADATA_TYPES}")


def ingest_bronze(spark, input_dir: Path, batch_id: str, entities=ENTITIES) -> list[dict]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id must be non-empty")
    entities = tuple(entities)
    if not entities or len(set(entities)) != len(entities) or set(entities) - set(ENTITIES):
        raise ValueError("entities must be a non-empty, unique selection of core entities")
    # Preflight every input and target before committing the first table.
    sources = []
    for entity in entities:
        path = source_path(Path(input_dir), entity)
        table = f"iceberg.bronze.{entity}_raw"
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Missing {table}; run TV2's Bronze table bootstrap first")
        details = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        provider = next((row.data_type for row in details if row.col_name == "Provider"), None)
        if provider != "iceberg":
            raise ValueError(f"{table} must be an Iceberg table")
        raw = spark.read.option("mergeSchema", "true").parquet(str(path))
        validate_schema(raw.schema, spark.table(table).schema, table)
        sources.append((raw, table))

    reports = []
    for raw, table in sources:
        started = monotonic()
        # Capture filename before caching; ingestion metadata remains separate from raw fields.
        incoming = raw.withColumn("_source", F.input_file_name()).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            input_rows = incoming.count()
            previous = spark.table(table).where(F.col("_batch_id") == F.lit(batch_id))
            committed = previous.limit(1).count()
            if committed:
                # Compare multisets, not IDs/counts: changed values or duplicate
                # multiplicities must never be silently accepted under an old ID.
                previous_raw = previous.select(*raw.columns)
                incoming_raw = incoming.select(*raw.columns)
                if (previous_raw.exceptAll(incoming_raw).limit(1).count()
                        or incoming_raw.exceptAll(previous_raw).limit(1).count()):
                    raise ValueError(f"{table}: batch {batch_id!r} already exists with different raw rows; "
                                     "use a new batch ID for changed inputs")
            output_rows = 0
            status = "skipped" if committed else "empty"
            if not committed and input_rows:
                data = (incoming.withColumn("_ingested_at", F.current_timestamp())
                        .withColumn("_batch_id", F.lit(batch_id))
                        .withColumn("_event_id", F.col("event_id").cast("string")
                                    if "event_id" in raw.columns else F.lit(None).cast("string"))
                        .withColumn("_schema_version", F.lit(1)))
                data.select(*spark.table(table).columns).writeTo(table).append()
                output_rows = input_rows
                status = "written"
            report = {
                "job": "ingest_bronze", "table": table, "batch_id": batch_id,
                "status": status, "input_rows": input_rows, "output_rows": output_rows,
                # Bronze preserves bad/duplicate domain rows; Silver evaluates these metrics.
                "invalid_rows": None, "duplicate_rows": None,
                "duration_seconds": round(monotonic() - started, 3),
            }
            LOGGER.info(json.dumps(report, sort_keys=True))
            reports.append(report)
        finally:
            incoming.unpersist()
    return reports


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--batch-id", required=True, help="Stable ID for one immutable input batch")
    parser.add_argument("--entities", nargs="+", choices=ENTITIES, default=ENTITIES)
    args = parser.parse_args(argv)
    if not args.batch_id.strip():
        parser.error("--batch-id must be non-empty")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = create_spark_session("ingest_bronze", SparkConfig.from_env())
    try:
        ingest_bronze(spark, args.input_dir, args.batch_id, args.entities)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
