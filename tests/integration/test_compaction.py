"""Real Iceberg compaction preserves rows/checksum and reduces tiny files."""
import os
import tempfile
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('RUN_MAINTENANCE_TESTS')!='1',reason='set RUN_MAINTENANCE_TESTS=1')


def test_small_files_are_compacted_without_changing_data():
    from pyspark.sql import SparkSession, functions as F
    from spark.common.iceberg import ICEBERG_VERSION
    from spark.batch.compact_tables import compact
    with tempfile.TemporaryDirectory(prefix='hospital-compaction-') as path:
        spark=(SparkSession.builder.master('local[2]').appName('compaction-test')
            .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
            .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .config('spark.sql.catalog.iceberg','org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.iceberg.type','hadoop')
            .config('spark.sql.catalog.iceberg.warehouse',path)
            .config('spark.jars.packages',f'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        try:
            spark.sql('CREATE NAMESPACE iceberg.bronze')
            (spark.range(200).withColumn('value',F.concat(F.lit('synthetic-'),F.col('id'))).repartition(5)
             .writeTo('iceberg.bronze.compaction_fixture').using('iceberg').create())
            report=compact(spark,'iceberg.bronze.compaction_fixture',8)
            assert report['before']['files']>=5
            assert report['after']['files']<report['before']['files']
            assert report['before']['n']==report['after']['n']==200
            assert report['before']['checksum']==report['after']['checksum']
        finally:
            spark.stop()
