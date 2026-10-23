"""Live keyed transport checks on host and Compose Kafka listeners."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import subprocess
import sys
from time import monotonic
import uuid

from scripts.lakehouse_smoke import compose_command, run, ROOT


def receive(consumer, expected, timeout=30):
    """Ignore unrelated traffic, but require every exact keyed probe once."""
    remaining = Counter(expected)
    found = []
    deadline = monotonic() + timeout
    while remaining and monotonic() < deadline:
        for records in consumer.poll(timeout_ms=500).values():
            for record in records:
                pair = (record.key, record.value)
                if remaining[pair]:
                    found.append(record)
                    remaining[pair] -= 1
                    if not remaining[pair]:
                        del remaining[pair]
    if remaining:
        raise ValueError(f"Missing {sum(remaining.values())} keyed Kafka probe records")
    return found


def check_patient_keys(records, expected=None):
    partitions = {}
    actual_order = {}
    for record in records:
        patient_id = json.loads(record.value)["payload"]["patient_id"]
        if record.key != patient_id.encode():
            raise ValueError("Kafka key does not match payload.patient_id")
        actual_order.setdefault(record.key, []).append(record.value)
        previous = partitions.setdefault(record.key, record.partition)
        if previous != record.partition:
            raise ValueError("One patient was routed to multiple partitions")

    if expected is not None:
        expected_order = {}
        for key, value in expected:
            expected_order.setdefault(key, []).append(value)
        if actual_order != expected_order:
            raise ValueError('Per-patient Kafka event order changed')


def main(argv=None):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from generators.stream_generator.event_models import HospitalSimulator, SimulatorConfig

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap-servers', help='Default: published Compose Kafka port')
    parser.add_argument('--restart', action='store_true', help='Recreate broker and reread acknowledged probes at their original offsets')
    args = parser.parse_args(argv)
    compose = compose_command()
    producer = consumer = None
    try:
        broker = args.bootstrap_servers or run([*compose, 'port', 'kafka', '9092'], capture=True).strip()
        if not broker:
            raise ValueError('Kafka port is not published; run make kafka-up')
        producer = KafkaProducer(bootstrap_servers=broker, acks='all', retries=3,
                                 request_timeout_ms=15000, max_block_ms=15000)
        consumer = KafkaConsumer(bootstrap_servers=broker, group_id=None, enable_auto_commit=False,
                                 request_timeout_ms=15000)
        partitions = []
        for topic in ('hospital.events', 'hospital.dlq'):
            available = consumer.partitions_for_topic(topic)
            if not available:
                raise ValueError(f'Topic unavailable: {topic}; run make kafka-topics')
            partitions.extend(TopicPartition(topic, part) for part in sorted(available))
        consumer.assign(partitions)
        consumer.seek_to_end(*partitions)
        # Force positions before sending; no old offsets/groups are reset.
        for partition in partitions:
            consumer.position(partition)
        simulator = HospitalSimulator(SimulatorConfig(seed=42, run_id=uuid.uuid4().hex[:24],
                                                        late_event_probability=0, duplicate_probability=0, invalid_event_probability=0))
        messages = [simulator.next_message() for _ in range(24)]
        expected = [(msg.key.encode(), msg.value) for msg in messages]
        for key, value in expected:
            producer.send('hospital.events', key=key, value=value).get(timeout=20)
        records = receive(consumer, expected)
        check_patient_keys(records, expected)
        sample = next(record for record in records if (record.key, record.value) == expected[0])
        # Internal CLI must be able to read a record written through the host listener.
        result = run([*compose, 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh',
                      '--bootstrap-server', 'kafka:29092', '--topic', 'hospital.events',
                      '--partition', str(sample.partition), '--offset', str(sample.offset),
                      '--max-messages', '1', '--timeout-ms', '15000',
                      '--property', 'print.key=true', '--property', 'key.separator=\t'], capture=True, timeout=45)
        if result.rstrip('\n') != sample.key.decode() + '\t' + sample.value.decode():
            raise ValueError('Internal consumer did not read the exact host probe')
        # Internal producer -> host consumer, keyed to the same patient.
        key, value = expected[0]
        internal_value = json.loads(value)
        internal_value['event_id'] = str(uuid.uuid4())
        internal_bytes = json.dumps(internal_value, separators=(',', ':')).encode()
        sent = subprocess.run([*compose, 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh',
                               '--bootstrap-server', 'kafka:29092', '--topic', 'hospital.events',
                               '--property', 'parse.key=true', '--property', 'key.separator=\t'],
                              input=key.decode()+'\t'+internal_bytes.decode()+'\n', text=True,
                              cwd=ROOT, capture_output=True, timeout=45)
        if sent.returncode:
            raise RuntimeError(f'Internal producer failed: {sent.stderr}')
        internal = receive(consumer, [(key, internal_bytes)])
        check_patient_keys([sample, *internal])
        # Only transport availability is checked for DLQ, not Spark routing semantics.
        dlq = json.dumps({'smoke_id': uuid.uuid4().hex, 'purpose': 'dlq-transport-probe'}).encode()
        producer.send('hospital.dlq', key=key, value=dlq).get(timeout=20)
        received = receive(consumer, [(key, dlq)])
        if received[0].topic != 'hospital.dlq':
            raise ValueError('DLQ record came from the wrong topic')
        if args.restart:
            persisted = [sample, received[0]]
            consumer.close(autocommit=False)
            consumer = None
            producer.close(timeout=10)
            producer = None
            run([*compose, 'up', '-d', '--no-deps', '--no-build', '--pull', 'never',
                 '--force-recreate', '--wait', '--wait-timeout', '120', 'kafka'])
            run([*compose, 'run', '--rm', '--no-deps', 'kafka-init'])
            consumer = KafkaConsumer(bootstrap_servers=broker, group_id=None, enable_auto_commit=False,
                                     request_timeout_ms=15000)
            assigned = [TopicPartition(record.topic, record.partition) for record in persisted]
            consumer.assign(assigned)
            for partition, record in zip(assigned, persisted):
                consumer.seek(partition, record.offset)
            restored = receive(consumer, [(record.key, record.value) for record in persisted])
            if {(r.topic, r.partition, r.offset) for r in restored} != {(r.topic, r.partition, r.offset) for r in persisted}:
                raise ValueError('Probe offsets changed after broker recreation')
            print('PASS: acknowledged event and DLQ bytes/keys/offsets survive broker recreation', flush=True)
        print('PASS: 24 simulator events, patient keys/partitions, host <-> internal listeners, DLQ transport', flush=True)
        return 0
    except Exception as error:
        # Kafka errors vary by client version; all must remain visible and fail the CLI.
        print(f'FAIL Kafka smoke: {type(error).__name__}: {error}', file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
        if producer is not None:
            producer.close(timeout=10)


if __name__ == '__main__':
    raise SystemExit(main())
