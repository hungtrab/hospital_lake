"""Live Bronze contract fixture; never writes to canonical Bronze tables."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import re

from pyspark.sql import functions as F

from spark.common.bronze import bootstrap_bronze, load_contracts, validate_table
from spark.common.config import SparkConfig
from spark.common.iceberg import create_spark_session
from scripts.verify_bronze import ENTITY_IDS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--action", choices=("populate", "drift", "cleanup"), default="populate")
    args = parser.parse_args()
    if not re.fullmatch(r"bronze_smoke_[a-f0-9]{32}", args.namespace):
        parser.error("Only an isolated bronze_smoke_<uuid hex> namespace is allowed")
    if args.action == "populate" and not args.input_dir:
        parser.error("--input-dir is required for populate")
    config = SparkConfig.from_env()
    spark = create_spark_session("bronze-contract-smoke", config)
    try:
        contracts = load_contracts()
        if args.action == "cleanup":
            for contract in contracts:
                spark.sql(f"DROP TABLE IF EXISTS iceberg.{args.namespace}.{contract.name} PURGE")
            spark.sql(f"DROP NAMESPACE IF EXISTS iceberg.{args.namespace}")
            print("PASS: isolated Bronze smoke namespace purged", flush=True)
            return
        if args.action == "drift":
            name = f"iceberg.{args.namespace}.patients_raw"

            def reject_drift(table_name, message):
                load = lambda: spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_name)
                before = load().operations().current().metadataFileLocation()
                count = spark.table(table_name).count()
                try:
                    bootstrap_bronze(spark, args.namespace, config.warehouse)
                except ValueError as error:
                    if message not in str(error):
                        raise
                    if load().operations().current().metadataFileLocation() != before or spark.table(table_name).count() != count:
                        raise AssertionError("Rejected bootstrap changed an existing table")
                    print(f"PASS: bootstrap rejects {message} without changing data/metadata", flush=True)
                    return
                raise AssertionError(f"{message} was silently accepted")

            spark.sql(f"ALTER TABLE {name} ADD COLUMN unexpected_field STRING")
            reject_drift(name, "schema drift")
            spark.sql(f"ALTER TABLE {name} DROP COLUMN unexpected_field")
            encounter = f"iceberg.{args.namespace}.encounters_raw"
            spark.sql(f"ALTER TABLE {encounter} DROP PARTITION FIELD days(admission_time)")
            reject_drift(encounter, "partition drift")
            spark.sql(f"ALTER TABLE {encounter} ADD PARTITION FIELD days(admission_time)")
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.format.default'='avro')")
            reject_drift(name, "table property drift")
            return
        bootstrap_bronze(spark, args.namespace, config.warehouse)
        for contract in contracts:
            entity = contract.name[:-4]
            source = spark.read.parquet(str(args.input_dir / f"{entity}.parquet"))
            sample = source.orderBy(ENTITY_IDS[entity]).limit(1)
            raw = source.unionByName(sample)  # Intentional duplicate must survive Bronze.
            if entity == "vitals":
                raw = raw.unionByName(sample.withColumn("heart_rate", F.lit(-1)))
            frame = (raw.withColumn("_source", F.lit("synthetic-contract-fixture"))
                     .withColumn("_ingested_at", F.lit(datetime(2026, 10, 12, tzinfo=timezone.utc)))
                     .withColumn("_batch_id", F.lit(args.namespace))
                     .withColumn("_event_id", F.col("event_id") if entity == "bed_events" else F.lit(None).cast("string"))
                     .withColumn("_schema_version", F.lit(1)))
            name = f"iceberg.{args.namespace}.{contract.name}"
            frame.writeTo(name).append()
            actual = spark.table(name)
            if actual.exceptAll(frame).limit(1).count() or frame.exceptAll(actual).limit(1).count():
                raise AssertionError(f"{name}: source values or duplicates changed")
            print(f"PASS: {name} preserves raw Parquet values and duplicate rows", flush=True)
        before = [validate_table(spark, contract, args.namespace, config.warehouse) for contract in contracts]
        bootstrap_bronze(spark, args.namespace, config.warehouse)
        after = [validate_table(spark, contract, args.namespace, config.warehouse) for contract in contracts]
        if before != after:
            raise AssertionError("Bootstrap rerun changed existing table metadata")
        print("PASS: bootstrap rerun preserves data and metadata", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
