"""Known-event Kafka → Spark → ClickHouse → Grafana gate on a quiet demo stack."""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import os
import shlex
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spark.common.clickhouse import ClickHouse
from scripts.grafana_smoke import env_file, request, wait_for_plugins, panel_rows, EXPECTED
from scripts.trino_smoke import query as trino_query


def wait_for(fn, predicate, label, timeout):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = fn()
        if predicate(last):
            print(f"PASS {label}: {last}", flush=True)
            return last
        time.sleep(2)
    raise RuntimeError(f"{label}: timed out; last={last}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, default=Path('.env'))
    parser.add_argument('--timeout', type=int, default=240)
    parser.add_argument('--restart', action='store_true', help='restart the Compose stream service mid-journey')
    parser.add_argument('--output', type=Path, default=Path('results/realtime-gate.json'))
    args = parser.parse_args(argv)
    settings = {**env_file(args.env_file), **os.environ}
    if args.restart and not settings.get('COMPOSE'):
        parser.error('--restart requires COMPOSE identifying the target project and env file')
    compose = shlex.split(settings.get('COMPOSE', 'docker compose'))
    trino = [*compose, 'exec', '-T', 'trino', 'trino', '--output-format', 'CSV']
    def sql_query(sql, timeout=60):
        if settings.get('TRINO_URL'):
            from spark.common.trino import query
            return [[str(value) for value in row] for row in query(settings['TRINO_URL'], sql, timeout)]
        return trino_query(trino, sql, timeout)
    client = ClickHouse(settings.get('CLICKHOUSE_URL'), settings.get('CLICKHOUSE_USER'), settings.get('CLICKHOUSE_PASSWORD'))
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=settings.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'), acks='all')
    base = settings.get('GRAFANA_URL') or f"http://localhost:{settings.get('GRAFANA_PORT', '3000')}"
    auth = base64.b64encode(f"{settings.get('GRAFANA_ADMIN_USER', 'admin')}:{settings.get('GRAFANA_ADMIN_PASSWORD', 'admin')}".encode()).decode()
    prefix = 'gate-' + uuid.uuid4().hex[:12]
    patient, encounter = prefix+'-P', prefix+'-E'
    sent = []
    def emit(kind, payload=None, when=None, event_id=None, raw=None):
        event = dict(event_id=event_id or str(uuid.uuid4()), event_type=kind,
            event_time=(when or datetime.now(timezone.utc)).isoformat(), source='realtime-gate', version=1,
            payload=payload or dict(patient_id=patient, encounter_id=encounter, department='ICU', encounter_type='icu'))
        value = raw if raw is not None else json.dumps(event).encode()
        metadata = producer.send(settings.get('KAFKA_TOPICS', 'hospital.events'), key=patient.encode(), value=value).get(timeout=30)
        try:
            actual_id = json.loads(value).get('event_id')
        except (ValueError, AttributeError):
            actual_id = None
        sent.append(dict(topic=metadata.topic, partition=metadata.partition, offset=metadata.offset,
                         event_id=actual_id, sha256=hashlib.sha256(value).hexdigest()))
        return event, value
    def current():
        rows = client.rows('SELECT current_patients FROM hospital.realtime_hospital_state')
        return int(rows[0]['current_patients']) if rows else 0
    def marker():
        return int(client.rows('SELECT max(snapshot_version) AS v FROM hospital.published_snapshots')[0]['v'])
    def barrier():
        before = marker()
        emit('PATIENT_REGISTERED', dict(patient_id=patient))
        wait_for(marker, lambda v: v > before, 'new published batch', args.timeout)
    baseline = current()
    start = time.monotonic()
    try:
        event, value = emit('ADMISSION')
        wait_for(current, lambda n: n == baseline+1, 'ADMISSION increments current patients', args.timeout)
        # Spark propagates an event-time watermark across micro-batches. Advance
        # another completed batch before asserting rejection beyond that watermark.
        barrier()
        emit('ADMISSION', raw=value)
        emit('ADMISSION', dict(patient_id=patient+'-late', encounter_id=encounter+'-late', department='ICU', encounter_type='icu'),
             when=datetime.now(timezone.utc)-timedelta(hours=1))
        emit('VITAL_MEASUREMENT', raw=b'{malformed')
        malformed = sent[-1]
        emit('LAB_RESULT', dict(patient_id=patient, encounter_id=encounter, test_code='GLU', test_name='Glucose',
             result_value=200, reference_low=70, reference_high=99, unit='mg/dL', is_abnormal=False))
        barrier()
        assert current() == baseline+1, 'duplicate/late admission changed state'
        wait_for_plugins(base, auth, args.timeout)
        query = dict(refId='A', rawSql='SELECT current_patients FROM hospital.realtime_hospital_state',
                     format=1, editorType='sql', queryType='table', datasource=dict(type='grafana-clickhouse-datasource', uid='hospital-clickhouse'))
        response = request(base, auth, '/api/ds/query', dict(queries=[query], **{'from':'now-1h','to':'now'}))
        result = response['results']['A']
        assert not result.get('error'), result
        values = result['frames'][0]['data']['values'][0]
        assert int(values[0]) == baseline+1, values
        print('PASS Grafana reads expected current-patient value', flush=True)
        for uid in EXPECTED:
            panels = request(base, auth, f'/api/dashboards/uid/{uid}')['dashboard']['panels']
            for panel in panels:
                if (panel.get('datasource') or {}).get('uid') == 'hospital-clickhouse':
                    rows = panel_rows(base, auth, panel)
                    if not rows:
                        raise AssertionError(f"empty live panel: {uid}/{panel['title']}")
                    print(f"PASS live panel {uid}/{panel['title']}: {rows} rows", flush=True)
        if args.restart:
            subprocess.run([*compose, '--profile', 'kafka', '--profile', 'realtime', 'restart', 'stream'], check=True)
            barrier()
            assert current() == baseline+1, 'restart changed current-patient count'
        emit('DISCHARGE')
        wait_for(current, lambda n: n == baseline, 'DISCHARGE returns to baseline', args.timeout)
        # Raw bytes, including duplicates and malformed JSON, must survive. Check
        # Kafka coordinates and content hash, not just aggregate row counts.
        coordinates = ','.join(f"('{r['topic'].replace(chr(39), chr(39)*2)}',{r['partition']},{r['offset']},'{r['sha256']}')" for r in sent)
        sql = f"""WITH expected(topic, part, off, hash) AS (VALUES {coordinates}), actual AS (
          SELECT _kafka_topic topic, _kafka_partition part, _kafka_offset off,
                 lower(to_hex(sha256(raw_payload))) hash FROM iceberg.bronze.stream_events_raw
          UNION ALL SELECT _kafka_topic, _kafka_partition, _kafka_offset,
                 lower(to_hex(sha256(raw_payload))) FROM iceberg.bronze.stream_events_quarantine)
          SELECT count(*) FROM (SELECT e.topic,e.part,e.off FROM expected e LEFT JOIN actual a
            ON e.topic=a.topic AND e.part=a.part AND e.off=a.off
            GROUP BY e.topic,e.part,e.off,e.hash HAVING count(a.off) <> 1 OR min(a.hash) <> e.hash)"""
        wait_for(lambda: int(sql_query(sql)[0][0]), lambda n: n == 0,
                 'all sent bytes occur once in Bronze/quarantine', args.timeout)
        check = sql_query(f"""SELECT count_if(event_type='ADMISSION'),
          count_if(event_type='LAB_RESULT' AND is_abnormal), count_if(event_type='DISCHARGE')
          FROM iceberg.silver.realtime_events WHERE patient_id LIKE '{prefix}%'""")
        assert check == [['1', '1', '1']], f'dedup/late/lab/discharge ledger: {check}'
        rejected = sql_query(f"""SELECT error_reason FROM iceberg.bronze.stream_events_quarantine
          WHERE _kafka_partition={malformed['partition']} AND _kafka_offset={malformed['offset']}
          AND _kafka_topic='{malformed['topic']}'""")
        assert len(rejected) == 1 and 'INVALID_JSON' in rejected[0][0], rejected
        print('PASS Silver excludes duplicates/late events and recomputes abnormal labs', flush=True)
        report = dict(status='PASS', baseline=baseline, restart=args.restart, sent=sent,
                      elapsed_seconds=round(time.monotonic()-start, 2), run_id=prefix)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2)+'\n')
        print(f'PASS realtime gate; evidence={args.output}', flush=True)
    finally:
        producer.close(timeout=10)


if __name__ == '__main__':
    main()
