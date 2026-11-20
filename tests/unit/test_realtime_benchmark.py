from scripts.realtime_benchmark import sample_problem, measured_sample


def test_failed_samples_cannot_be_success_outside_bounded_restart():
    failed = dict(elapsed_seconds=20, error='serving unavailable')
    assert sample_problem(failed)
    assert sample_problem(failed, restart_elapsed=10) is None
    assert sample_problem(dict(elapsed_seconds=200, error='still unavailable'), restart_elapsed=10)


def test_missing_measurements_are_failure():
    assert sample_problem(dict(elapsed_seconds=20, progress={}, serving=[], resources={}))
    complete = dict(elapsed_seconds=20, progress={'hospital_bronze':{'batchId':1},'hospital_serving':{'batchId':1}}, serving=[{'current_patients':0}], resources={'MemUsage':'1MiB'})
    assert sample_problem(complete) is None


def test_empty_progress_only_allowed_during_planned_warmup():
    starting=dict(elapsed_seconds=930,progress={},serving=[{'current_patients':1}],resources={'MemUsage':'1MiB'})
    assert sample_problem(starting,restart_elapsed=900) is None
    assert sample_problem(starting,restart_elapsed=700)
    assert sample_problem(starting)


def test_restart_grace_does_not_count_empty_or_invalid_latency_as_measurement():
    sample=dict(elapsed_seconds=930,progress={'hospital_serving':{'numInputRows':1},
                'hospital_bronze':{'batchId':1}},serving=[],resources={'MemUsage':'1MiB'})
    assert sample_problem(sample,restart_elapsed=900) is None
    assert not measured_sample(sample)
    for value in (None, float('nan'), float('inf'), -1, 'invalid'):
        sample['serving']=[{'latency_p95_seconds':value}]
        assert not measured_sample(sample)
    sample['serving']=[{'latency_p95_seconds':1.5}]
    assert measured_sample(sample)
