"""Submit the Week 3 Gold batch job with the shared Iceberg/MinIO settings."""

import os
import subprocess
from pathlib import Path

from spark.common.config import SparkConfig


def command(config, *, submit="spark-submit"):
    root = Path(__file__).resolve().parents[1]
    version = os.environ.get("ICEBERG_VERSION", "1.9.2")
    packages = (f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{version},"
                f"org.apache.iceberg:iceberg-aws-bundle:{version}")
    return [
        submit, "--master", config.master, "--packages", packages,
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.iceberg.type=rest",
        "--conf", f"spark.sql.catalog.iceberg.uri={config.catalog_uri}",
        "--conf", f"spark.sql.catalog.iceberg.warehouse={config.warehouse}",
        "--conf", "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf", f"spark.sql.catalog.iceberg.s3.endpoint={config.minio_endpoint}",
        "--conf", "spark.sql.catalog.iceberg.s3.path-style-access=true",
        "--conf", f"spark.sql.catalog.iceberg.client.region={config.minio_region}",
        "--conf", "spark.sql.session.timeZone=UTC",
        str(root / "spark" / "batch" / "silver_to_gold.py"),
    ]


def submit_env(config, base=None):
    """Credentials via environment, and the repo root on PYTHONPATH for spark.common imports."""
    env = dict(os.environ if base is None else base)
    env["AWS_ACCESS_KEY_ID"] = config.minio_access_key
    env["AWS_SECRET_ACCESS_KEY"] = config.minio_secret_key
    env["AWS_REGION"] = config.minio_region
    root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(p for p in (root, env.get("PYTHONPATH")) if p)
    return env


def main():
    config = SparkConfig.from_env()
    if not os.environ.get("GOLD_AS_OF") or not os.environ.get("GOLD_BED_CAPACITIES"):
        raise SystemExit("GOLD_AS_OF and GOLD_BED_CAPACITIES must be set")
    env = submit_env(config)
    subprocess.run(command(config, submit=env.get("SPARK_SUBMIT", "spark-submit")),
                   env=env, check=True)


if __name__ == "__main__":
    main()
