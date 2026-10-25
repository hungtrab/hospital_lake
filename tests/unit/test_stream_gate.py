"""Unit tests for the Week 4 stream gate (scripts/stream_gate.py) without Kafka or Trino."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import stream_gate


class StreamGateTests(unittest.TestCase):
    def test_kafka_end_offsets_parses_cli_output(self):
        output = "hospital.events:0:2811\nhospital.events:1:2824\nhospital.events:2:2965\n"
        with mock.patch.object(stream_gate.subprocess, "run",
                               return_value=subprocess.CompletedProcess([], 0, output, "")):
            self.assertEqual(stream_gate.kafka_end_offsets(["docker", "compose"], "hospital.events"),
                             {0: 2811, 1: 2824, 2: 2965})
        with mock.patch.object(stream_gate.subprocess, "run",
                               return_value=subprocess.CompletedProcess([], 0, "", "")):
            with self.assertRaises(ValueError):
                stream_gate.kafka_end_offsets(["docker", "compose"], "hospital.events")

    def test_coverage_sql_filters_new_offsets_and_rejects_unsafe_names(self):
        sql = stream_gate.coverage_sql("iceberg", "hospital.events", {0: 10, 1: 20})
        self.assertIn("(_kafka_partition = 0 AND _kafka_offset >= 10)", sql)
        self.assertIn("stream_events_quarantine", sql)
        with self.assertRaises(ValueError):
            stream_gate.coverage_sql("iceberg", "x' OR 1=1 --", {0: 0})
        with self.assertRaises(ValueError):
            stream_gate.coverage_sql("bad-catalog", "hospital.events", {0: 0})

    def test_check_coverage(self):
        start, end = {0: 10, 1: 0}, {0: 15, 1: 3}
        self.assertEqual(stream_gate.check_coverage(start, end, {0: (5, 5, 10, 14), 1: (3, 3, 0, 2)}), [])
        missing = stream_gate.check_coverage(start, end, {0: (4, 4, 10, 13), 1: (3, 3, 0, 2)})
        self.assertEqual(len(missing), 1)
        self.assertIn("4/5", missing[0])
        duplicate = stream_gate.check_coverage(start, end, {0: (6, 5, 10, 14), 1: (3, 3, 0, 2)})
        self.assertIn("duplicate", duplicate[0])
        absent = stream_gate.check_coverage(start, end, {0: (5, 5, 10, 14)})
        self.assertIn("0/3", absent[0])

    def test_evaluate_requires_quota_catch_up_and_clean_producer(self):
        start, end = {0: 0}, {0: 100}
        full = {0: (100, 100, 0, 99)}
        ok = dict(rate=10, duration=10, producer_returncode=0, catch_up_seconds=5, catch_up_limit=60)
        self.assertEqual(stream_gate.evaluate(start, end, full, **ok), [])
        short = stream_gate.evaluate(start, {0: 70}, {0: (70, 70, 0, 69)}, **ok)
        self.assertTrue(any("70/100" in p for p in short), short)
        self.assertTrue(stream_gate.evaluate(start, end, full, **dict(ok, producer_returncode=1)))
        self.assertTrue(stream_gate.evaluate(start, end, full, **dict(ok, catch_up_seconds=None)))
        self.assertTrue(stream_gate.evaluate(start, end, full, **dict(ok, catch_up_seconds=61)))

    def run_main(self, ends, coverages, producer_rc=0):
        class FakeProducer:
            returncode = None

            def __init__(self, *args, **kwargs):
                kwargs["stderr"].write('{"events_sent": 100, "events_per_second": 10.0}\n')

            def poll(self):
                self.returncode = producer_rc
                return producer_rc

            def terminate(self):
                raise AssertionError("finished producer must not be terminated")

        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "gate.json"
            with mock.patch.object(stream_gate, "kafka_end_offsets", side_effect=ends), \
                    mock.patch.object(stream_gate, "read_coverage", side_effect=coverages), \
                    mock.patch.object(stream_gate.subprocess, "Popen", FakeProducer), \
                    mock.patch.object(stream_gate.time, "sleep"), \
                    mock.patch("builtins.print"):
                code = stream_gate.main(["--rate", "10", "--duration", "10", "--interval", "1",
                                         "--server", "http://t:8080", "--report", str(report)])
            return code, json.loads(report.read_text())

    def test_main_passes_when_all_offsets_arrive(self):
        full = {0: (100, 100, 0, 99)}
        code, report = self.run_main([{0: 0}, {0: 100}, {0: 100}], [full, full])
        self.assertEqual((code, report["status"], report["kafka_messages"]), (0, "PASS", 100))
        self.assertEqual(report["producer"]["events_sent"], 100)
        self.assertIn("interval_seconds", report)

    def test_main_fails_on_producer_error(self):
        full = {0: (100, 100, 0, 99)}
        code, report = self.run_main([{0: 0}, {0: 100}, {0: 100}], [full, full], producer_rc=2)
        self.assertEqual((code, report["status"]), (1, "FAIL"))


if __name__ == "__main__":
    unittest.main()
