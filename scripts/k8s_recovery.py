"""Measure two executor counts and verify recovery on the isolated kind demo."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

from scripts.k8s import NAMESPACE, kubectl
from scripts.stream_gate import coverage_sql, check_coverage


def workload(*args):
    subprocess.run([sys.executable, '-m', 'scripts.k8s', *args], check=True)


def executors():
    pods = json.loads(kubectl('get', 'pods', '-l', 'app=hospital-stream,spark-role=executor',
                              '-o', 'json', capture=True))
    return {p['metadata']['name'] for p in pods['items']
            if p['status']['phase'] == 'Running' and not p['metadata'].get('deletionTimestamp')}


def offsets():
    output = kubectl('exec', 'hospital-combined-0', '-c', 'kafka', '--',
                     '/opt/kafka/bin/kafka-get-offsets.sh', '--bootstrap-server',
                     'localhost:9092', '--topic', 'hospital.events', capture=True)
    return {int(part): int(offset) for _, part, offset in
            (line.split(':') for line in output.splitlines() if line.startswith('hospital.events:'))}


def serving_progress():
    path = f'/api/v1/namespaces/{NAMESPACE}/services/stream:p8090/proxy/progress'
    return json.loads(kubectl('get', '--raw='+path, capture=True)).get('hospital_serving', {})


def verify_serving_counts():
    sql = """SELECT count(DISTINCT patient_id),
        count(DISTINCT CASE WHEN department='ICU' THEN patient_id END)
        FROM iceberg.silver.realtime_encounters
        WHERE seen_admission AND event_type <> 'DISCHARGE'"""
    historical = kubectl('exec','deployment/trino','--','trino','--output-format','TSV',
                         '--execute',sql,capture=True)
    live = kubectl('exec','clickhouse-0','--','sh','-c',
                   'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
                   '--query "SELECT current_patients,icu_occupancy FROM hospital.realtime_hospital_state"',
                   capture=True)
    if historical.split() != live.split() or len(live.split()) != 2:
        raise RuntimeError(f'Silver and ClickHouse census disagree: {historical!r}, {live!r}')
    return dict(zip(('current_patients','icu_occupancy'),map(int,live.split())))


def samples(count, *, after_batch=-1):
    rows = []
    deadline = time.monotonic() + 240
    seen = set()
    while time.monotonic() < deadline:
        try:
            batch = serving_progress()
        except subprocess.CalledProcessError:
            # Operator reports RUNNING before the new driver starts its HTTP server.
            time.sleep(3)
            continue
        if batch.get('numInputRows') and batch['batchId'] > after_batch and batch['batchId'] not in seen:
            seen.add(batch['batchId'])
            row = {k: batch[k] for k in ('batchId','timestamp','numInputRows','processedRowsPerSecond')}
            committed = batch['sources'][0]['endOffset']
            if isinstance(committed,str):
                committed = json.loads(committed)
            latest = offsets()
            row['lag_at_observation'] = sum(max(0, high - committed['hospital.events'].get(str(p),0))
                                            for p, high in latest.items())
            rows.append(row)
        if len(rows) >= count:
            return rows
        time.sleep(3)
    raise TimeoutError(f'expected {count} nonempty serving batches, observed {rows}')


def active_workload():
    kubectl('rollout','status','deployment/stream-generator','--timeout=90s')
    pods = json.loads(kubectl('get','pods','-l','app=stream-generator','-o','json',capture=True))
    ready = [p for p in pods['items'] if not p['metadata'].get('deletionTimestamp')
             and any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[]))]
    if len(ready) != 1:
        raise RuntimeError('expected one ready workload generator')
    start = offsets()
    deadline = time.monotonic()+45
    while time.monotonic()<deadline:
        time.sleep(1)
        end = offsets()
        if sum(end.values())>sum(start.values()):
            return dict(pod=ready[0]['metadata']['name'], before=start, after=end,
                        restarts=ready[0]['status']['containerStatuses'][0]['restartCount'])
    raise TimeoutError('generator is ready but Kafka offsets did not advance')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rate',type=int,default=30)
    parser.add_argument('--output',type=Path,default=Path('results/k8s/recovery.json'))
    args = parser.parse_args()
    if not 1 <= args.rate <= 10000:
        parser.error('rate must be between 1 and 10000 events/s')
    replicas = kubectl('get','deployment/stream-generator','-o','jsonpath={.spec.replicas}',capture=True)
    if int(replicas) != 0:
        raise RuntimeError('stop the generator before running the controlled experiment')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps({'status':'RUNNING'})+'\n')
    workload('stream','--executors','2')
    start = offsets()
    if not start:
        raise RuntimeError('no Kafka partitions')
    report = {'start':start, 'rate':args.rate}
    try:
        workload('produce','--rate',str(max(1,args.rate//10)))
        report['low_load_workload'] = active_workload()
        report['low_load'] = samples(3,after_batch=serving_progress().get('batchId',-1))
        workload('produce','--rate',str(args.rate))
        report['high_load_workload'] = active_workload()
        report['two_executors'] = samples(3,after_batch=serving_progress().get('batchId',-1))
        scale_start = time.monotonic()
        workload('stream','--executors','4')
        report['resubmit_seconds'] = round(time.monotonic() - scale_start, 2)
        report['four_executors'] = samples(3, after_batch=report['two_executors'][-1]['batchId'])
        before = executors()
        if len(before) != 4:
            raise RuntimeError(f'expected four active executors, got {before}')
        report['before_failure_workload'] = active_workload()
        victim = sorted(before)[0]
        kubectl('delete','pod',victim,'--wait=false')
        deadline = time.monotonic()+180
        while time.monotonic()<deadline:
            after = executors()
            if len(after)==4 and victim not in after and after-before:
                break
            time.sleep(2)
        else:
            raise TimeoutError('no replacement executor')
        report['after_failure_workload'] = active_workload()
        last = serving_progress().get('batchId', report['four_executors'][-1]['batchId'])
        report.update(removed=victim,replacement=sorted(after-before),after_recovery=samples(3,after_batch=last))
    finally:
        workload('stop-producer')
        kubectl('rollout','status','deployment/stream-generator','--timeout=90s')
        kubectl('wait','--for=delete','pod','-l','app=stream-generator','--timeout=90s')
    end = offsets()
    if sum(end.values()) <= sum(start.values()):
        raise RuntimeError('no workload observed')
    sql = coverage_sql('iceberg','hospital.events',start)
    deadline = time.monotonic()+480
    while time.monotonic()<deadline:
        out = kubectl('exec','deployment/trino','--','trino','--output-format','TSV',
                      '--execute',sql,capture=True)
        coverage = {int(row[0]):tuple(map(int,row[1:])) for row in
                    (line.split('\t') for line in out.splitlines() if line)}
        errors = check_coverage(start,end,coverage)
        progress = serving_progress()
        sources = progress.get('sources', [])
        committed = sources[0].get('endOffset') if sources else {}
        if isinstance(committed, str):
            committed = json.loads(committed)
        served = (committed or {}).get('hospital.events', {})
        if not errors and all(served.get(str(p), -1) >= high for p, high in end.items()):
            break
        time.sleep(5)
    else:
        raise RuntimeError(f"stream did not catch up: bronze={errors}, serving={served}, target={end}")
    report.update(end=end,coverage=coverage,serving=served,census=verify_serving_counts(),status='PASS')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print('PASS scale and recovery, evidence:',args.output)


if __name__ == '__main__':
    main()
