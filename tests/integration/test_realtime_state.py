"""Real local Iceberg state transitions; opt in with RUN_REALTIME_TESTS=1."""
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('RUN_REALTIME_TESTS') != '1',
                              reason='set RUN_REALTIME_TESTS=1 with requirements-spark.txt')


@pytest.fixture(scope='module')
def lake():
    from pyspark.sql import SparkSession
    from spark.common.iceberg import ICEBERG_VERSION
    from spark.streaming.realtime_aggregates import ensure_tables
    with tempfile.TemporaryDirectory(prefix='hospital-realtime-') as path:
        spark = (SparkSession.builder.master('local[2]').appName('realtime-state-test')
                 .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
                 .config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
                 .config('spark.sql.catalog.iceberg.type', 'hadoop')
                 .config('spark.sql.catalog.iceberg.warehouse', path)
                 .config('spark.jars.packages', f'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}')
                 .getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        ensure_tables(spark)
        try:
            yield spark
        finally:
            spark.stop()


def test_lifecycle_identity_and_replay(lake):
    from spark.streaming.realtime_aggregates import update_state
    now = datetime(2026, 11, 16, tzinfo=timezone.utc)
    schema = 'event_id string,event_time timestamp,event_type string,patient_id string,encounter_id string,department string,is_abnormal boolean'
    def event(i, kind, seconds, patient='P1', encounter='E1', dept='ICU'):
        return (i, now+timedelta(seconds=seconds), kind, patient, encounter, dept, False)
    def send(*rows):
        update_state(lake, lake.createDataFrame(rows, schema))
    def state(encounter='E1'):
        return lake.table('iceberg.silver.realtime_encounters').where(f"encounter_id='{encounter}'").first()
    # Transfer arriving before admission is held until its admission arrives.
    send(event('t1', 'TRANSFER', 2, dept='CARDIOLOGY'))
    assert not state().seen_admission
    send(event('a1', 'ADMISSION', 1))
    assert state().seen_admission and state().department == 'CARDIOLOGY'
    send(event('wrong', 'TRANSFER', 3, patient='P2'))
    assert state().patient_id == 'P1' and state().department == 'CARDIOLOGY'
    # A single encounter cannot reopen after a discharge, even with a newer ID/time.
    send(event('d1', 'DISCHARGE', 4))
    send(event('t2', 'TRANSFER', 5), event('a2', 'ADMISSION', 6))
    assert state().event_type == 'DISCHARGE'
    assert {r.error_reason for r in lake.table('iceberg.silver.realtime_rejected').collect()} == {
        'ENCOUNTER_PATIENT_CONFLICT', 'EVENT_AFTER_DISCHARGE'}
    # One-batch and cross-batch order give the same terminal result.
    rows = (event('a3', 'ADMISSION', 1, encounter='E2'),
            event('d3', 'DISCHARGE', 2, encounter='E2'),
            event('t3', 'TRANSFER', 3, encounter='E2'))
    send(*rows)
    before = lake.table('iceberg.silver.realtime_events').count()
    send(*rows)
    assert state('E2').event_type == 'DISCHARGE' and state('E2').seen_admission
    assert lake.table('iceberg.silver.realtime_events').count() == before
    assert lake.table('iceberg.silver.realtime_rejected').where("event_id='t3'").count() == 1
    # Registrations have no encounter: different patients must not be grouped as
    # one encounter or rejected against whichever registration sorts first.
    send(event('r1', 'PATIENT_REGISTERED', 1, patient='P3', encounter=None),
         event('r2', 'PATIENT_REGISTERED', 2, patient='P4', encounter=None))
    assert lake.table('iceberg.silver.realtime_events').where("event_id IN ('r1','r2')").count() == 2
    # The validation cutoff and persisted tombstone use the same earliest close.
    send(event('a4', 'ADMISSION', 0, encounter='E4'), event('d4a', 'DISCHARGE', 2, encounter='E4'),
         event('d4b', 'DISCHARGE', 4, encounter='E4'))
    send(event('a4bad', 'ADMISSION', 3, encounter='E4'))
    assert lake.table('iceberg.silver.realtime_events').where("event_id='a4bad'").count() == 0
    send(event('d4early', 'DISCHARGE', 1, encounter='E4'))
    send(event('t4bad', 'TRANSFER', 1.5, encounter='E4'))
    assert lake.table('iceberg.silver.realtime_events').where("event_id='t4bad'").count() == 0
