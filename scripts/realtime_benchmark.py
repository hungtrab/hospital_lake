"""Measured local workload/recovery run; requires an already running quiet stack.

Records per-batch Spark throughput, Kafka lag, Kafka-arrival-to-publication
latency, Docker resources and exact Bronze offset coverage. These measurements
describe this machine/profile; they do not establish production throughput.
"""
from __future__ import annotations
import argparse
import json
import math
import os
import shlex
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from scripts.grafana_smoke import env_file
from spark.common.clickhouse import ClickHouse


def sample_problem(sample, *, restart_elapsed=None, restart_grace=120):
    """Only a bounded, planned restart gap may lack observations."""
    problem=None
    if sample.get('error'):
        problem='sampling failed outside planned restart window: '+sample['error']
    elif (not all((sample.get('progress') or {}).get(name) for name in ('hospital_bronze', 'hospital_serving'))
          or not sample.get('serving') or not sample.get('resources')):
        problem='sample missing Spark progress, serving rows or Docker resource measurements'
    if problem and restart_elapsed is not None and 0 <= sample['elapsed_seconds']-restart_elapsed <= restart_grace:
        return None
    return problem


def measured_sample(sample):
    """Restart grace never substitutes for actual observations."""
    if sample_problem(sample):
        return False
    rows=sample['serving']
    try:
        valid_latency=all(row.get('latency_p95_seconds') is not None
                          and math.isfinite(float(row['latency_p95_seconds']))
                          and float(row['latency_p95_seconds']) >= 0 for row in rows)
    except (TypeError, ValueError):
        return False
    return valid_latency and sample['progress']['hospital_serving'].get('numInputRows', 0) > 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, default=Path('.env'))
    parser.add_argument('--rate', type=float, default=100)
    parser.add_argument('--duration', type=int, default=1800)
    parser.add_argument('--restart-at', type=int, default=0, help='restart stream after this many workload seconds')
    parser.add_argument('--output', type=Path, default=Path('results/realtime-benchmark'))
    args = parser.parse_args()
    if args.rate <= 0 or args.duration < 10 or not 0 <= args.restart_at < args.duration:
        parser.error('rate must be positive, duration >=10, restart-at inside workload')
    settings = {**env_file(args.env_file), **os.environ}
    if not settings.get('COMPOSE'):
        parser.error('set COMPOSE to identify the target project and env file')
    compose = shlex.split(settings['COMPOSE'])
    client = ClickHouse(settings.get('CLICKHOUSE_URL'), settings.get('CLICKHOUSE_USER'), settings.get('CLICKHOUSE_PASSWORD'))
    url = f"http://localhost:{settings.get('STREAM_METRICS_PORT', '8090')}"
    args.output.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, '-m', 'scripts.stream_gate', '--compose', '--rate', str(args.rate),
               '--duration', str(args.duration), '--interval', '30', '--catch-up', '600',
               '--bootstrap-servers', settings.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
               '--trino-url', settings.get('TRINO_URL') or f"http://localhost:{settings.get('TRINO_PORT', '8080')}",
               '--report', str(args.output/'bronze-gate.json')]
    def progress():
        with urllib.request.urlopen(url+'/progress', timeout=10) as response:
            return json.load(response)
    def service_state():
        container = subprocess.check_output([*compose, 'ps', '-q', 'stream'], text=True).strip()
        if not container:
            raise RuntimeError('stream container is not running')
        return json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
    progress()  # Fail before launching a workload if monitoring is unavailable.
    initial = service_state()
    started = time.monotonic()
    restarted = False
    restart_elapsed = None
    failures = []
    samples = 0
    measured_batches = 0
    final = None
    with (args.output/'gate.log').open('w') as log, (args.output/'samples.jsonl').open('w') as output:
        gate = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                env={**os.environ, 'COMPOSE': settings['COMPOSE']}, start_new_session=True)
        try:
            while gate.poll() is None:
                elapsed = time.monotonic()-started
                if args.restart_at and not restarted and elapsed >= args.restart_at:
                    restart_elapsed = elapsed
                    subprocess.run([*compose, 'restart', 'stream'], check=True)
                    restarted = True
                sample = dict(elapsed_seconds=round(elapsed, 2))
                try:
                    sample['progress'] = progress()
                    sample['serving'] = client.rows('SELECT * FROM hospital.realtime_hospital_state')
                    state = service_state()
                    sample['restart_count'] = state['RestartCount']
                    sample['oom_killed'] = state['State']['OOMKilled']
                    if state['RestartCount'] != initial['RestartCount'] or sample['oom_killed']:
                        failures.append('unexpected stream restart/OOM')
                    sample['resources'] = json.loads(subprocess.check_output(
                        ['docker', 'stats', '--no-stream', '--format', '{{json .}}', state['Id']], text=True))
                except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
                    sample['error'] = str(error)
                problem = sample_problem(sample, restart_elapsed=restart_elapsed)
                if problem:
                    failures.append(problem)
                if measured_sample(sample):
                    measured_batches += 1
                output.write(json.dumps(sample)+'\n')
                output.flush()
                samples += 1
                print(json.dumps({k:v for k,v in sample.items() if k in ('elapsed_seconds','restart_count','error')}), flush=True)
                time.sleep(10)
            if gate.returncode:
                failures.append(f'Bronze gate exited {gate.returncode}; see gate.log')
            report_path = args.output/'bronze-gate.json'
            if report_path.exists() and 'end_offsets' in json.loads(report_path.read_text()):
                bronze = json.loads(report_path.read_text())
                expected = {str(k): int(v) for k,v in bronze['end_offsets'].items()}
                deadline = time.monotonic()+600
                while time.monotonic() < deadline:
                    try:
                        final = progress()
                    except OSError:
                        time.sleep(5)
                        continue
                    serving = final.get('hospital_serving', {})
                    offsets = (serving.get('sources') or [{}])[0].get('endOffset') or {}
                    actual = offsets.get(settings.get('KAFKA_TOPICS','hospital.events'), {})
                    if all(int(actual.get(k,-1)) >= v for k,v in expected.items()):
                        break
                    time.sleep(5)
                else:
                    failures.append('serving did not catch up to workload end offsets')
            else:
                failures.append('missing exact Bronze coverage report')
        except KeyboardInterrupt:
            failures.append('benchmark interrupted before completion')
        finally:
            if gate.poll() is None:
                os.killpg(gate.pid, signal.SIGTERM)
                gate.wait(timeout=60)
    if not measured_batches:
        failures.append('no nonempty serving batch with measured latency/resources')
    if args.restart_at and not restarted:
        failures.append('planned stream restart did not occur')
    report = dict(status='FAIL' if failures else 'PASS', rate=args.rate, duration=args.duration,
                  restart_at=args.restart_at, restart_elapsed=restart_elapsed, restarted=restarted,
                  samples=samples, measured_batches=measured_batches,
                  failures=sorted(set(failures)), final_progress=final,
                  elapsed_seconds=round(time.monotonic()-started, 2))
    (args.output/'summary.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report), flush=True)
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())
