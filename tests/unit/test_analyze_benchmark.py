from copy import deepcopy

from scripts.analyze_benchmark import analyze, unobserved_seconds


def evidence():
    progress = {
        name: {
            'runId': 'run-1', 'batchId': 1, 'numInputRows': 100,
            'durationMs': {'triggerExecution': 1000},
            'sources': [{'startOffset': {'custom.events': {'0': 0}},
                         'endOffset': {'custom.events': {'0': 100}}}],
        }
        for name in ('hospital_bronze', 'hospital_serving')
    }
    summary = dict(status='PASS', duration=1800, elapsed_seconds=1800,
                   restart_at=900, restart_elapsed=900, restarted=True,
                   final_progress=progress)
    bronze = dict(status='PASS', topic='custom.events', start_offsets={'0':0}, end_offsets={'0': 100},
                  kafka_messages=100, bronze_rows=100)
    sample = dict(progress=progress, restart_count=0, oom_killed=False,
                  serving=[{'latency_p95_seconds': 2.5}],
                  resources={'MemUsage': '1GiB / 3GiB', 'CPUPerc': '125.0%'})
    samples = [dict(deepcopy(sample), elapsed_seconds=t) for t in range(0, 1801, 30)]
    return summary, bronze, samples


def test_valid_observations_use_kafka_offsets_and_real_units():
    result = analyze(*evidence())
    assert result['status'] == 'PASS', result
    assert result['max_sampled_memory_bytes'] == 1024**3
    assert result['max_sampled_kafka_records_per_second']['hospital_serving'] == 100


def test_restart_only_exempts_its_own_missing_interval():
    assert unobserved_seconds(890, 1020, 900) == 10
    assert unobserved_seconds(0, 910, 900) == 900
    summary, bronze, samples = evidence()
    samples = [samples[0], dict(samples[0], elapsed_seconds=910),
               *[s for s in samples if s['elapsed_seconds'] >= 960]]
    result = analyze(summary, bronze, samples)
    assert result['status'] == 'FAIL'
    assert 'unobserved interval exceeds 60 seconds' in result['problems']


def test_failed_runner_or_missing_pipeline_evidence_never_upgraded():
    summary, bronze, samples = evidence()
    summary['status'] = 'FAIL'
    assert analyze(summary, bronze, samples)['status'] == 'FAIL'
    summary['status'] = 'PASS'
    summary['final_progress'] = {}
    assert analyze(summary, bronze, samples)['status'] == 'FAIL'


def test_latency_must_belong_to_observed_nonempty_work():
    summary, bronze, samples = evidence()
    for sample in samples:
        sample['serving'] = [{'latency_p95_seconds': None}]
    samples[0]['serving'] = [{'latency_p95_seconds': 1}]
    samples[0]['progress']['hospital_serving']['numInputRows'] = 0
    result = analyze(summary, bronze, samples)
    assert result['status'] == 'FAIL'
    assert result['sampled_nonempty_serving_batches'] == 0
    assert result['max_batch_p95_latency_seconds'] is None


def test_idle_snapshot_and_previous_workload_do_not_pollute_latency():
    summary,bronze,samples=evidence()
    samples[0]['progress']['hospital_serving']['numInputRows']=0
    samples[0]['serving']=[{'latency_p95_seconds':9999}]
    samples[1]['progress']['hospital_serving']['sources'][0]['endOffset']={'custom.events':{'0':0}}
    samples[1]['serving']=[{'latency_p95_seconds':9999}]
    result=analyze(summary,bronze,samples)
    assert result['status']=='PASS',result
    assert result['max_batch_p95_latency_seconds']==2.5


def test_oom_and_early_completion_are_failures():
    summary, bronze, samples = evidence()
    samples[20]['oom_killed'] = True
    assert analyze(summary, bronze, samples)['status'] == 'FAIL'
    samples[20]['oom_killed'] = False
    summary['elapsed_seconds'] = 1700
    assert analyze(summary, bronze, samples)['status'] == 'FAIL'
