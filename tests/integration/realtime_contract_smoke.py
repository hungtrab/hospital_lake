"""Real Spark/Iceberg → ClickHouse fixture. Use ONLY an isolated empty project."""
from __future__ import annotations
import os
import time
from datetime import datetime, timedelta, timezone

from spark.common.iceberg import create_spark_session
from spark.common.clickhouse import ClickHouse
from spark.streaming.realtime_aggregates import ensure_tables, update_state, publish


def main():
    if os.environ.get("LIVE_SMOKE_ISOLATED") != "1":
        raise SystemExit("Set LIVE_SMOKE_ISOLATED=1 only for an isolated test stack")
    spark = create_spark_session("RealtimeContractSmoke")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    client = ClickHouse()
    now = datetime.now(timezone.utc)
    ensure_tables(spark)
    if spark.table("iceberg.silver.realtime_encounters").limit(1).count():
        raise RuntimeError("fixture requires empty realtime state; use a new isolated project")
    schema = 'event_id string,event_time timestamp,event_type string,patient_id string,encounter_id string,department string,is_abnormal boolean'
    def send(rows):
        frame = spark.createDataFrame(rows, schema)
        update_state(spark, frame)
        publish(spark, client, as_of=now)
        return client.rows('SELECT * FROM hospital.realtime_hospital_state')[0]
    try:
        close = ('d1', now-timedelta(seconds=6), 'DISCHARGE', 'P1', 'E1', 'ICU', False)
        admission = ('a1', now-timedelta(seconds=10), 'ADMISSION', 'P1', 'E1', 'ICU', False)
        assert int(send([close])['current_patients']) == 0
        assert int(send([admission])['current_patients']) == 0, 'late admission must not reopen discharged encounter'
        opened = ('a2', now-timedelta(seconds=5), 'ADMISSION', 'P2', 'E2', 'ICU', False)
        state = send([opened])
        assert int(state['current_patients']) == 1 and int(state['icu_occupancy']) == 1
        assert int(send([opened])['current_patients']) == 1, 'retry must be idempotent'
        transferred = ('t2', now-timedelta(seconds=3), 'TRANSFER', 'P2', 'E2', 'CARDIOLOGY', False)
        state = send([transferred, ('l2', now-timedelta(seconds=2), 'LAB_RESULT', 'P2', 'E2', None, True)])
        assert int(state['current_patients']) == 1 and int(state['icu_occupancy']) == 0
        assert int(state['admissions_5m']) == 2 and int(state['discharges_5m']) == 1
        assert int(client.rows('SELECT sum(abnormal_labs) AS n FROM hospital.realtime_lab_metrics')[0]['n']) == 1
        # A torn unpublished snapshot cannot displace the last complete one.
        client.insert('hospital_snapshots', [dict(snapshot_version=time.time_ns(), event_time=now,
            current_patients=999, icu_occupancy=999, total_icu_beds=20, processed_events=1,
            invalid_events=0, batch_duration_seconds=0)])
        assert int(client.rows('SELECT current_patients FROM hospital.realtime_hospital_state')[0]['current_patients']) == 1
        # Read windows use event time and expire even without a running Spark job.
        old_version = time.time_ns()
        client.insert('department_snapshots', [dict(snapshot_version=old_version, window_start=now-timedelta(minutes=6),
            department='ICU', admissions_5m=999, discharges_5m=0)])
        client.insert('lab_snapshots', [dict(snapshot_version=old_version, window_start=now-timedelta(minutes=16), abnormal_labs=999)])
        client.insert('published_snapshots', [dict(snapshot_version=old_version, published_at=now)])
        assert int(client.rows('SELECT sum(admissions_5m) AS n FROM hospital.realtime_department_metrics')[0]['n']) == 0
        assert int(client.rows('SELECT sum(abnormal_labs) AS n FROM hospital.realtime_lab_metrics')[0]['n']) == 0
        publish(spark, client, as_of=now)
        print('PASS realtime contract: out-of-order, retry, transfer, lab, publication, idle expiry')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
