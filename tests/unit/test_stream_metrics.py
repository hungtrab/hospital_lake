from spark.common.stream_metrics import render


def test_lag_uses_completed_offsets_and_real_state_counters():
    progress = {'serving': dict(batchId=7, timestamp='2026-11-16T00:00:00.000Z',
        durationMs={'triggerExecution': 2000}, numInputRows=100, inputRowsPerSecond=10,
        processedRowsPerSecond=50, stateOperators=[dict(numRowsDroppedByWatermark=3,
        numRowsTotal=12, memoryUsedBytes=4096, customMetrics={'numDroppedDuplicateRows': 2})],
        sources=[dict(endOffset={'hospital.events': {'0': 100, '1': 50}})])}
    text = render(progress, {('hospital.events', 0): 120, ('hospital.events', 1): 50}, kafka_ok=True, active=True)
    assert 'hospital_kafka_lag{query="serving",topic="hospital.events",partition="0"} 20.0' in text
    assert 'hospital_stream_batch_late_events{query="serving"} 3.0' in text
    assert 'hospital_stream_batch_duplicate_events{query="serving"} 2.0' in text
    # A failed Kafka probe must not present a stale/zero backlog as a success.
    failed = render(progress, {}, kafka_ok=False, active=True)
    assert 'hospital_kafka_probe_success 0' in failed and 'hospital_kafka_lag{' not in failed


def test_no_completed_progress_does_not_fabricate_throughput():
    text = render({}, {}, kafka_ok=True, active=False)
    assert 'hospital_stream_active 0' in text
    assert 'processed_rows' not in text
