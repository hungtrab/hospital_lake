"""Known event-time values, payload rejection and ICU ordering (real local Spark)."""
from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import unittest

from spark.streaming.realtime_aggregates import (
    department_metrics, icu_encounter_state, icu_snapshot, lab_metrics, prepare_metric_events,
)
from spark.streaming.kafka_reader import deduplicate_events, parse_kafka_events


def message(key, kind, minute, **payload):
    return {"event_id": key, "event_type": kind, "event_time": "2026-11-17T10:" + minute + "Z",
            "source": "fixture", "version": 1,
            "payload": dict(patient_id="p1", encounter_id="e1", **payload)}


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class RealtimeAggregateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("tv2-realtime-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def parsed(self, messages):
        now = datetime(2026, 11, 17, 11, tzinfo=timezone.utc)
        rows = [("fixture", 0, i, now, json.dumps(msg).encode()) for i, msg in enumerate(messages)]
        records = self.spark.createDataFrame(rows, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, value BINARY")
        return prepare_metric_events(parse_kafka_events(records))

    def clean(self, messages):
        return deduplicate_events(self.parsed(messages))

    def test_department_windows_are_half_open_and_deduplicated(self):
        first = message("a", "ADMISSION", "00:00", department=" intensive care ")
        frame = self.clean([first, first, message("b", "ADMISSION", "04:59", department="ICU"),
                            message("c", "DISCHARGE", "05:00", department="ICU")])
        rows = {r.start: (r.admissions_5m, r.discharges_5m) for r in
                department_metrics(frame).selectExpr("date_format(window_start, 'HH:mm') AS start", "*").collect()}
        self.assertEqual(rows["10:00"], (2, 0))
        self.assertEqual(rows["10:01"], (1, 1))
        self.assertEqual(rows["10:05"], (0, 1))

    def test_lab_recomputes_flags_and_15m_window_does_not_double_count(self):
        def lab(key, minute, value):
            return message(key, "LAB_RESULT", minute, result_value=value, reference_low=70,
                           reference_high=100, is_abnormal=False)
        first = lab("a", "00:00", 101)
        frame = self.clean([first, first, lab("b", "14:59", 100), lab("c", "15:00", 69)])
        bucket_rows = lab_metrics(frame).selectExpr("date_format(window_start, 'HH:mm') AS start", "abnormal_labs").collect()
        self.assertEqual({r.start: r.abnormal_labs for r in bucket_rows}, {"10:00": 1, "10:14": 0, "10:15": 1})
        rolling = lab_metrics(frame, rolling=True).selectExpr("date_format(window_start, 'HH:mm') AS start", "abnormal_labs_last_15m").collect()
        self.assertEqual({r.start: r.abnormal_labs_last_15m for r in rolling}["10:00"], 1)
        self.assertEqual({r.start: r.abnormal_labs_last_15m for r in rolling}["10:01"], 1)

    def test_bad_payloads_keep_raw_bytes_and_reasons(self):
        invalid = [message("a", "ADMISSION", "00:00", department="unknown"),
                   message("b", "LAB_RESULT", "00:00", result_value="NaN", reference_low=70, reference_high=100),
                   message("c", "LAB_RESULT", "00:00", result_value=80, reference_low=100, reference_high=70)]
        invalid[0]["payload"]["encounter_id"] = ""
        rows = {r.event_id: r for r in self.parsed(invalid).collect()}
        self.assertIn("UNKNOWN_DEPARTMENT", rows["a"].validation_error)
        self.assertIn("MISSING_ENCOUNTER_ID", rows["a"].validation_error)
        self.assertIn("INVALID_RESULT_VALUE", rows["b"].validation_error)
        self.assertIn("INVALID_REFERENCE_RANGE", rows["c"].validation_error)
        self.assertEqual(json.loads(bytes(rows["a"].value)), invalid[0])

    def test_icu_latest_event_wins_despite_duplicates_and_out_of_order_arrival(self):
        admission = message("a", "ADMISSION", "00:00", department="ICU")
        discharge = message("z", "DISCHARGE", "05:00", department="ICU")
        states = icu_encounter_state(self.parsed([discharge, admission, discharge]))
        self.assertEqual(icu_snapshot(states).first().icu_occupancy, 0)
        # A long stay must survive the windowed metrics' ten-minute watermark.
        other = message("b", "ADMISSION", "30:00", department="ICU")
        other["payload"]["encounter_id"] = "e2"
        self.assertEqual(icu_snapshot(icu_encounter_state(self.parsed([other, admission]))).first().icu_occupancy, 2)
        tied = message("zz", "DISCHARGE", "00:00", department="ICU")
        self.assertEqual(icu_snapshot(icu_encounter_state(self.parsed([admission, tied]))).first().icu_occupancy, 0)

    def test_version_and_missing_nonfinite_lab_values_are_rejected(self):
        rows = [message("version", "ADMISSION", "00:00", department="ICU"),
                message("missing", "LAB_RESULT", "00:00", reference_low=0, reference_high=1),
                message("infinite", "LAB_RESULT", "00:00", result_value="Infinity", reference_low=0, reference_high=1)]
        rows[0]["version"] = 2
        rejected = {r.event_id: r.validation_error for r in self.parsed(rows).collect()}
        self.assertIn("UNSUPPORTED_METRIC_VERSION", rejected["version"])
        self.assertIn("INVALID_RESULT_VALUE", rejected["missing"])
        self.assertIn("INVALID_RESULT_VALUE", rejected["infinite"])
