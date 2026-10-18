"""Create/validate Bronze Iceberg tables without rewriting existing data."""

import argparse
import json
import time

from spark.common.bronze import bootstrap_bronze
from spark.common.config import SparkConfig
from spark.common.iceberg import create_spark_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="bronze", help="bronze or isolated smoke namespace")
    args = parser.parse_args()
    config = SparkConfig.from_env()
    started = time.monotonic()
    spark = create_spark_session("bootstrap-bronze", config)
    try:
        tables = bootstrap_bronze(spark, args.namespace, config.warehouse)
        print(json.dumps({"job": "bootstrap-bronze", "namespace": args.namespace,
                          "tables_validated": len(tables), "duration_seconds": round(time.monotonic() - started, 3)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
