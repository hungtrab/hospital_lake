"""Week 1 fixture job, run in the Compose spark-smoke profile."""

import argparse
import re

from pyspark.sql import SparkSession
from spark.common.config import SparkConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="Simple name in bronze namespace")
    parser.add_argument("--action", choices=["write", "read", "drop"], default="write")
    args = parser.parse_args()
    if not re.fullmatch(r"w1_smoke_[a-f0-9]{32}", args.table):
        parser.error("Use a dedicated w1_smoke_<uuid hex> table")
    config = SparkConfig.from_env()
    spark = (SparkSession.builder.master(config.master).appName("week1-iceberg-smoke")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.iceberg.type", "rest")
             .config("spark.sql.catalog.iceberg.uri", config.catalog_uri)
             .config("spark.sql.catalog.iceberg.warehouse", config.warehouse)
             .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.iceberg.s3.endpoint", config.minio_endpoint)
             .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
             .config("spark.sql.catalog.iceberg.s3.access-key-id", config.minio_access_key)
             .config("spark.sql.catalog.iceberg.s3.secret-access-key", config.minio_secret_key)
             .config("spark.sql.catalog.iceberg.client.region", config.minio_region)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    table = f"iceberg.bronze.{args.table}"
    expected = [(1, "synthetic-1"), (2, "synthetic-2"), (3, "synthetic-3")]
    try:
        if args.action == "drop":
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
            print("PASS: temporary Iceberg fixture removed")
            return
        if args.action == "write":
            for namespace in ("bronze", "silver", "gold"):
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
            spark.createDataFrame(expected, "id long, label string").writeTo(table).using("iceberg").create()
        rows = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
        if rows != expected:
            raise AssertionError(f"Unexpected Iceberg fixture: {rows}")
        print(f"PASS: Spark {args.action} {table}: 3 exact rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
