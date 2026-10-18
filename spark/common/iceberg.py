"""Shared REST/S3FileIO session for the existing Spark 3.5 stack and Spark 4.0 hosts."""

from spark.common.config import SparkConfig

ICEBERG_VERSION = "1.10.1"
ICEBERG_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
)


def create_spark_session(app_name, config=None):
    import pyspark
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    config = config or SparkConfig.from_env()

    prefix = "spark.sql.catalog.iceberg"
    settings = {
        "spark.ui.enabled": "false",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.redaction.regex": "(?i)secret|password|token|access.?key",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": config.catalog_uri,
        f"{prefix}.warehouse": config.warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": config.minio_endpoint,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": config.minio_access_key,
        f"{prefix}.s3.secret-access-key": config.minio_secret_key,
        f"{prefix}.client.region": config.minio_region,
    }
    # Preserve packages supplied by the existing Compose spark-submit jobs.
    packages = SparkConf().get("spark.jars.packages", "")
    if not packages:
        if pyspark.__version__.startswith("4.0."):
            packages = ICEBERG_PACKAGES
        elif pyspark.__version__.startswith("3.5."):
            packages = ("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,"
                        "org.apache.iceberg:iceberg-aws-bundle:1.9.0")
        else:
            raise ValueError("Supported Spark versions: 3.5 and 4.0")
    settings["spark.jars.packages"] = packages
    builder = SparkSession.builder.master(config.master).appName(app_name)
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
