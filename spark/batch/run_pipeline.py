"""One SparkApplication executing the existing batch stages on shared input storage."""
from __future__ import annotations
import argparse
import json
import re
import time
from pathlib import Path

from generators.batch_generator.generate import main as generate
from spark.common.bronze import bootstrap_bronze
from spark.common.config import SparkConfig
from spark.common.iceberg import create_spark_session
from spark.batch.ingest_bronze import ingest_bronze
from spark.batch.bronze_to_silver import run as silver
from spark.batch.silver_to_gold import main as gold


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--patients', type=int, default=1000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--batch-id', default='k8s-demo-1000-42')
    parser.add_argument('--input-root', type=Path, default=Path('/state/generated'))
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.batch_id):
        parser.error('batch-id must contain only letters, digits, underscores or hyphens')
    start = time.monotonic()
    path = args.input_root/args.batch_id
    generate(['--patients', str(args.patients), '--seed', str(args.seed), '--output-dir', str(path)])
    config = SparkConfig.from_env()
    spark = create_spark_session('HospitalBatchPipeline', config)
    try:
        bootstrap_bronze(spark, 'bronze', config.warehouse)
        bronze = ingest_bronze(spark, path, args.batch_id)
        silver(spark)
        gold([])  # Reuses the existing session; final stage stops it on completion.
        print(json.dumps(dict(job='batch_pipeline', status='PASS', batch_id=args.batch_id,
            patients=args.patients, input_rows=sum(r['input_rows'] for r in bronze),
            duration_seconds=round(time.monotonic()-start, 3))))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
