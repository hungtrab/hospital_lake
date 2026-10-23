"""Validate keyed-probe assertions and optional local Kafka boundaries."""
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml
from scripts.kafka_smoke import check_patient_keys, receive

ROOT = Path(__file__).resolve().parents[2]


def record(patient='P1', partition=1, key=None):
    return SimpleNamespace(key=patient.encode() if key is None else key, partition=partition,
                           value=json.dumps({'payload': {'patient_id': patient}}).encode())


class KafkaSmokeTests(unittest.TestCase):
    def test_patient_key_and_partition_contract(self):
        check_patient_keys([record(), record(), record('P2', 2)])
        with self.assertRaisesRegex(ValueError, 'payload.patient_id'):
            check_patient_keys([record(key=b'wrong')])
        with self.assertRaisesRegex(ValueError, 'multiple partitions'):
            check_patient_keys([record(partition=1), record(partition=2)])

    def test_per_patient_order_is_checked(self):
        first, second = record(), record()
        second.value = json.dumps({'payload': {'patient_id': 'P1'}, 'event_id': 'second'}).encode()
        expected = [(r.key, r.value) for r in (first, second)]
        check_patient_keys([first, second], expected)
        with self.assertRaisesRegex(ValueError, 'event order'):
            check_patient_keys([second, first], expected)

    def test_receive_handles_unrelated_traffic_and_duplicate_probes(self):
        wanted, noise = record(), record('other')
        consumer = SimpleNamespace(poll=lambda **kwargs: {0: [noise, wanted, wanted]})
        self.assertEqual(len(receive(consumer, [(wanted.key, wanted.value)] * 2)), 2)

    def test_missing_probe_fails_instead_of_passing_empty_output(self):
        consumer = SimpleNamespace(poll=lambda **kwargs: {})
        with patch('scripts.kafka_smoke.monotonic', side_effect=[0, 0, 31]):
            with self.assertRaisesRegex(ValueError, 'Missing 1'):
                receive(consumer, [(b'key', b'value')], timeout=30)

    def test_invalid_partition_count_fails_before_external_calls(self):
        for value in ('0', '-1', 'oops', '3; touch /tmp/not-run'):
            with self.subTest(value=value):
                result = subprocess.run(['sh', str(ROOT / 'infra/docker/kafka/bootstrap-topics.sh')],
                                        env={**os.environ, 'KAFKA_TOPIC_PARTITIONS': value},
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn('KAFKA_TOPIC_PARTITIONS', result.stderr)

    def test_kafka_is_optional_and_persistent_with_healthy_topic_dependency(self):
        config = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
        broker = config['services']['kafka']
        init = config['services']['kafka-init']
        self.assertIn('kafka', broker['profiles'])
        self.assertIn('kafka', init['profiles'])
        self.assertIn('kafka-data:/var/lib/kafka/data', broker['volumes'])
        self.assertEqual(init['depends_on']['kafka']['condition'], 'service_healthy')
        self.assertEqual(broker['environment']['KAFKA_AUTO_CREATE_TOPICS_ENABLE'], 'false')
        self.assertEqual(broker['environment']['KAFKA_PROCESS_ROLES'], 'broker,controller')
