"""Five-minute Bronze progress probe logic without requiring a live cluster."""

import unittest
from unittest.mock import patch

from scripts.stream_bronze_probe import monitor, read_counts


class FakeTime:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class StreamBronzeProbeTests(unittest.TestCase):
    def test_trino_query_counts_both_tables_and_rejects_bad_result(self):
        with patch("scripts.stream_bronze_probe.query", return_value=[["12", "2", "0"]]) as query:
            self.assertEqual(read_counts(["trino"], "iceberg", 5), (12, 2, 0))
            sql = query.call_args.args[1]
            self.assertIn('"iceberg"."bronze"."stream_events_raw"', sql)
            self.assertIn('"iceberg"."bronze"."stream_events_quarantine"', sql)
            self.assertIn("_kafka_offset", sql)
        with patch("scripts.stream_bronze_probe.query", return_value=[["12", "oops", "0"]]):
            with self.assertRaisesRegex(ValueError, "malformed"):
                read_counts(["trino"], "iceberg", 5)

    def test_five_minute_growth_and_quarantine(self):
        clock = FakeTime()

        def stats():
            return (100 + int(clock.now), int(clock.now >= 60), 0)

        result = monitor(stats, duration=300, interval=30, min_events=300,
                         require_quarantine=True, monotonic=clock.monotonic,
                         sleep=clock.sleep)
        self.assertEqual(result, (300, 1))
        self.assertEqual(clock.now, 300)

    def test_duplicate_kafka_coordinates_fail_immediately(self):
        clock = FakeTime()
        reads = iter([(100, 0, 0), (110, 0, 1)])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            monitor(lambda: next(reads), duration=300, interval=30,
                    min_events=1, require_quarantine=False,
                    monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(clock.now, 30)

    def test_no_growth_fails_after_duration(self):
        clock = FakeTime()
        with self.assertRaisesRegex(ValueError, "only 0"):
            monitor(lambda: (100, 0, 0), duration=300, interval=100,
                    min_events=1, require_quarantine=False,
                    monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(clock.now, 300)

    def test_growth_then_long_stall_fails_before_full_window(self):
        clock = FakeTime()

        def stats():
            return (100 + int(clock.now >= 30), 0, 0)

        with self.assertRaisesRegex(ValueError, "idle"):
            monitor(stats, duration=300, interval=30, max_idle=60,
                    min_events=1, require_quarantine=False,
                    monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(clock.now, 90)


if __name__ == "__main__":
    unittest.main()
