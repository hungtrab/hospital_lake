"""Shared SparkSession setup for local Iceberg REST/S3 jobs."""

from spark.common.config import SparkConfig


def create_spark_session(app_name, config=None):
    # Import lazily so config/DDL checks do not require host PySpark.
    from pyspark.sql import SparkSession

    config = config or SparkConfig.from_env()
    spark = (SparkSession.builder.master(config.master).appName(app_name)
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
    return spark
