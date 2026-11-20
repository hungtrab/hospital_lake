"""Measured Iceberg file compaction; run on a quiet table with writers stopped."""
from __future__ import annotations
import argparse
import json
import re
import time
from pathlib import Path
from spark.common.iceberg import create_spark_session


def compact(spark, table, target_mb=128):
    if not re.fullmatch(r'iceberg\.(bronze|silver|gold)\.[A-Za-z_][A-Za-z0-9_]*', table):
        raise ValueError('table must be a simple iceberg.bronze/silver/gold identifier')
    if not 8 <= target_mb <= 1024:
        raise ValueError('target-mb must be between 8 and 1024')
    def measure():
        from pyspark.sql import functions as F
        files=spark.sql(f'SELECT count(*) files, coalesce(sum(file_size_in_bytes),0) bytes '
                        f'FROM {table}.files WHERE content=0').first().asDict()
        # Count a non-partition data column to exercise data reads. Disable
        # Iceberg aggregate pushdown so this is not merely a metadata lookup.
        frame=spark.table(table)
        spark.catalog.clearCache()
        start=time.monotonic()
        rows=frame.agg(F.count('*').alias('n'), F.sum(F.xxhash64(*frame.columns).cast('decimal(38,0)'))
                       .alias('checksum')).first().asDict()
        rows['checksum']=str(rows['checksum'])
        return dict(**files,**rows,query_seconds=round(time.monotonic()-start,4))
    spark.conf.set('spark.sql.iceberg.aggregate-push-down.enabled','false')
    before=measure()
    start=time.monotonic()
    result=spark.sql(f"""CALL iceberg.system.rewrite_data_files(
      table => '{table[len('iceberg.') :]}',
      options => map('target-file-size-bytes','{target_mb*1024*1024}',
                     'min-input-files','2','rewrite-all','true'))""").collect()
    duration=time.monotonic()-start
    after=measure()
    if (before['n'],before['checksum']) != (after['n'],after['checksum']):
        raise RuntimeError('rows/checksum changed; stop writers and investigate before accepting compaction')
    return dict(job='iceberg_compaction',table=table,before=before,after=after,
                rewrite=[row.asDict() for row in result],duration_seconds=round(duration,4))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table',default='iceberg.bronze.stream_events_raw')
    parser.add_argument('--target-mb',type=int,default=128)
    parser.add_argument('--report',type=Path)
    args=parser.parse_args()
    spark=create_spark_session('IcebergCompaction')
    try:
        report=compact(spark,args.table,args.target_mb)
        text=json.dumps(report,indent=2)+'\n'
        if args.report:
            args.report.parent.mkdir(parents=True,exist_ok=True)
            args.report.write_text(text)
        print(json.dumps(report),flush=True)
    finally:
        spark.stop()


if __name__=='__main__':
    main()
