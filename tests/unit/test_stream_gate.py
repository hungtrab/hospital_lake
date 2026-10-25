"""Unit tests for the Week 4 stream gate (scripts/stream_gate.py) without Kafka or Trino."""

import subprocess
import unittest
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


if __name__ == "__main__":
    unittest.main()
