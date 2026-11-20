"""Validate saved benchmark evidence and summarize measured quantities.

Keeps the runner's original summary intact and never upgrades a failed run.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from scripts.realtime_benchmark import sample_problem, measured_sample


def unobserved_seconds(start, end, restart):
    """Remove only the overlap with the explicitly planned restart grace."""
    overlap=0 if restart is None else max(0, min(end,restart+120)-max(start,restart))
    return end-start-overlap


def analyze(summary, bronze, samples):
    errors=[]
    duration=float(summary['duration'])
    if summary.get('status') != 'PASS':errors.append('runner did not pass')
    restart=summary.get('restart_elapsed')
    if restart is None and summary.get('restarted'):
        restart=float(summary['restart_at'])
    if bronze.get('status')!='PASS':errors.append('Bronze coverage/duration gate did not pass')
    if float(summary.get('elapsed_seconds',0))<duration:errors.append('run shorter than target duration')
    if summary.get('restart_at') and not summary.get('restarted'):errors.append('planned restart did not occur')
    if not samples:errors.append('no samples')
    starts={str(k):int(v) for k,v in bronze.get('start_offsets',{}).items()}
    topic=bronze.get('topic','hospital.events')
    if not starts:errors.append('missing workload start offsets')
    initial_restart=next((s['restart_count'] for s in samples if 'restart_count' in s),None)
    batches=set(); latency=[]; memory=[]; cpu=[]; throughput={}
    previous=0
    for sample in samples:
        elapsed=float(sample['elapsed_seconds'])
        if elapsed < previous:errors.append('sample timestamps are not monotonic')
        if unobserved_seconds(previous,elapsed,restart)>60:errors.append('unobserved interval exceeds 60 seconds')
        previous=elapsed
        problem=sample_problem(sample,restart_elapsed=restart)
        if problem:errors.append(problem)
        if sample.get('oom_killed') or ('restart_count' in sample and sample['restart_count']!=initial_restart):
            errors.append('unexpected restart/OOM')
        serving=(sample.get('progress') or {}).get('hospital_serving',{})
        rows=sample.get('serving') or []
        # An idle snapshot can retain latency from a previous run. Only use
        # nonempty completed serving batches that overlap this workload.
        new_work=serving.get('numInputRows',0)>0 and any(
            int(value)>starts.get(str(part),int(value))
            for source in serving.get('sources',[])
            for part,value in (source.get('endOffset') or {}).get(topic,{}).items())
        for row in rows if new_work else []:
            value=row.get('latency_p95_seconds')
            if value is not None:
                value=float(value)
                if not math.isfinite(value) or value<0:errors.append('invalid latency measurement')
                else:latency.append(value)
        if new_work and measured_sample(sample):
            batches.add((serving.get('runId'),serving.get('batchId')))
        resources=sample.get('resources') or {}
        match=re.match(r'([\d.]+)([KMGT]?i?B)',resources.get('MemUsage',''))
        if match:
            unit=match[2];power='BKMGT'.index(unit[0]) if unit[0]!='B' else 0
            memory.append(float(match[1])*(1024 if 'i' in unit else 1000)**power)
        if resources.get('CPUPerc'):cpu.append(float(resources['CPUPerc'].rstrip('%')))
        # foreachBatch may scan its input several times. Offset differences
        # measure consumed Kafka records without inflating for repeated scans.
        for name,progress in (sample.get('progress') or {}).items():
            records=0; complete=True
            for source in progress.get('sources',[]):
                start,end=source.get('startOffset'),source.get('endOffset')
                if not isinstance(start,dict) or not isinstance(end,dict):
                    complete=False;break
                for topic,parts in end.items():
                    if topic not in start:complete=False;break
                    records+=sum(int(value)-int(start[topic].get(part,value)) for part,value in parts.items())
            seconds=progress.get('durationMs',{}).get('triggerExecution',0)/1000
            if complete and records>0 and seconds>0:
                throughput.setdefault(name,[]).append(records/seconds)
    if not batches or not latency or not memory or not cpu:errors.append('missing measured work/latency/resources')
    if samples and unobserved_seconds(samples[-1]['elapsed_seconds'],float(summary.get('elapsed_seconds',0)),restart)>60:
        errors.append('unobserved interval at end exceeds 60 seconds')
    expected={str(k):int(v) for k,v in bronze.get('end_offsets',{}).items()}
    if not expected:errors.append('missing workload end offsets')
    for name in ('hospital_bronze','hospital_serving'):
        progress=(summary.get('final_progress') or {}).get(name,{})
        actual={}
        for source in progress.get('sources',[]):
            actual.update((source.get('endOffset') or {}).get(bronze.get('topic','hospital.events'),{}))
        if not all(int(actual.get(k,-1))>=v for k,v in expected.items()):errors.append(name+' did not drain')
    return dict(status='FAIL' if errors else 'PASS',problems=sorted(set(errors)),
        duration_seconds=duration,produced_records=bronze.get('kafka_messages'),bronze_rows=bronze.get('bronze_rows'),
        sampled_nonempty_serving_batches=len(batches),max_batch_p95_latency_seconds=max(latency,default=None),
        max_sampled_memory_bytes=max(memory,default=None),max_sampled_cpu_percent=max(cpu,default=None),
        max_sampled_kafka_records_per_second={k:round(max(v),2) for k,v in throughput.items()},
        planned_restart=restart,runner_status=summary.get('status'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args()
    paths=[args.directory/name for name in ('summary.json','bronze-gate.json','samples.jsonl')]
    summary,bronze=[json.loads(path.read_text()) for path in paths[:2]]
    samples=[json.loads(line) for line in paths[2].read_text().splitlines()]
    report=analyze(summary,bronze,samples)
    report['evidence_sha256']={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    (args.directory/'validated-summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)
    return report['status']!='PASS'


if __name__=='__main__':
    raise SystemExit(main())
