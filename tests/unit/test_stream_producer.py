"""Unit tests for the realtime hospital event simulator and producer (TV1, Week 4)."""

import importlib.util
import io
import json
import unittest
from collections import Counter, defaultdict
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

from generators.stream_generator.event_models import EVENT_TYPES, HospitalSimulator, SimulatorConfig
from generators.stream_generator.producer import main, run

NOW = datetime(2026, 10, 22, 10, 0, tzinfo=timezone.utc)
CLEAN = SimulatorConfig(seed=7, late_event_probability=0, duplicate_probability=0, invalid_event_probability=0)


def simulator(config=CLEAN):
    return HospitalSimulator(config, clock=lambda: NOW)


def decode(message):
    return json.loads(message.value)


class EnvelopeTests(unittest.TestCase):
    def test_envelope_matches_stream_contract(self):
        for message in simulator().messages(2000):
            event = decode(message)
            self.assertEqual(set(event), {"event_id", "event_type", "event_time", "source", "version", "payload"})
            self.assertIn(event["event_type"], EVENT_TYPES)
            self.assertEqual((event["source"], event["version"]), ("hospital-simulator", 1))
            self.assertEqual(event["event_time"], "2026-10-22T10:00:00.000Z")
            self.assertIsInstance(event["payload"], dict)
            self.assertEqual(message.key, event["payload"]["patient_id"])

    def test_same_seed_is_deterministic_and_event_ids_unique(self):
        first = [m.value for m in simulator().messages(3000)]
        self.assertEqual(first, [m.value for m in simulator().messages(3000)])
        ids = [json.loads(v)["event_id"] for v in first]
        self.assertEqual(len(ids), len(set(ids)))

    def test_every_event_type_is_produced(self):
        produced = {decode(m)["event_type"] for m in simulator().messages(5000)}
        self.assertEqual(produced, set(EVENT_TYPES))

    def test_encounter_event_order_is_plausible(self):
        registered, timeline = set(), defaultdict(list)
        for message in simulator().messages(20000):
            event = decode(message)
            payload = event["payload"]
            if event["event_type"] == "PATIENT_REGISTERED":
                registered.add(payload["patient_id"])
                continue
            if event["event_type"] == "ADMISSION":
                self.assertIn(payload["patient_id"], registered)
            timeline[payload["encounter_id"]].append(event["event_type"])
        for encounter_id, events in timeline.items():
            self.assertEqual(events[0], "ADMISSION", encounter_id)
            if "DISCHARGE" in events:
                tail = events[events.index("DISCHARGE"):]
                self.assertIn(tail, (["DISCHARGE"], ["DISCHARGE", "BED_RELEASED"]), encounter_id)
            if "BED_ASSIGNED" in events:
                self.assertEqual(events[1], "BED_ASSIGNED", encounter_id)
            else:
                self.assertNotIn("TRANSFER", events, encounter_id)

    def test_patient_never_has_two_open_encounters(self):
        open_encounters, admitted_before = defaultdict(set), set()
        readmissions = 0
        for message in simulator().messages(30000):
            event = decode(message)
            payload = event["payload"]
            if event["event_type"] == "ADMISSION":
                patient_id = payload["patient_id"]
                self.assertFalse(open_encounters[patient_id], payload)
                readmissions += patient_id in admitted_before
                admitted_before.add(patient_id)
                open_encounters[patient_id].add(payload["encounter_id"])
            elif event["event_type"] == "DISCHARGE":
                open_encounters[payload["patient_id"]].discard(payload["encounter_id"])
        self.assertGreater(readmissions, 0)

    def test_census_is_bounded(self):
        sim = simulator(SimulatorConfig(seed=3, max_active_encounters=50, late_event_probability=0,
                                        duplicate_probability=0, invalid_event_probability=0))
        sim.messages(10000)
        self.assertLessEqual(len(sim._active), 50)

    def test_defect_rates_and_shapes(self):
        config = SimulatorConfig(seed=11, late_event_probability=0.05, duplicate_probability=0.03,
                                 invalid_event_probability=0.02)
        messages = simulator(config).messages(40000)
        kinds = Counter(m.kind for m in messages)
        total = len(messages)
        self.assertAlmostEqual(kinds["late"] / total, 0.05, delta=0.01)
        self.assertAlmostEqual(kinds["duplicate"] / total, 0.03, delta=0.01)
        self.assertAlmostEqual(kinds["invalid"] / total, 0.02, delta=0.01)
        for index, message in enumerate(messages):
            if message.kind == "duplicate":
                self.assertEqual(message.value, messages[index - 1].value)
            if message.kind == "late":
                delay = NOW - datetime.fromisoformat(decode(message)["event_time"].replace("Z", "+00:00"))
                self.assertTrue(timedelta(minutes=11) <= delay <= timedelta(minutes=30))

    def test_defects_do_not_change_clean_content(self):
        noisy = SimulatorConfig(seed=7, late_event_probability=0.1, duplicate_probability=0.1,
                                invalid_event_probability=0.1)
        clean_ids = [decode(m)["event_id"] for m in simulator().messages(3000)]
        noisy_ids = [decode(m)["event_id"] for m in simulator(noisy).messages(4000)
                     if m.kind in ("valid", "late")]
        self.assertEqual(noisy_ids[:2000], clean_ids[:2000])


class PacingTests(unittest.TestCase):
    def test_rate_limits_and_counts(self):
        clock = {"now": 0.0}
        sent = []
        stats = run(simulator(), lambda key, value: sent.append(key), rate=500, duration=2.0,
                    monotonic=lambda: clock["now"],
                    sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
        self.assertEqual(len(sent), 1000)
        self.assertEqual(sum(stats.values()), len(sent))

    def test_max_events_stops_exactly(self):
        clock = {"now": 0.0}
        sent = []
        run(simulator(), lambda key, value: sent.append(value), rate=1000, max_events=123,
            monotonic=lambda: clock["now"],
            sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
        self.assertEqual(len(sent), 123)

    def test_dry_run_cli_prints_json_lines(self):
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["--dry-run", "--max-events", "25", "--rate", "10000"]), 0)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 25)
        self.assertEqual(json.loads(lines[0])["event_type"], "PATIENT_REGISTERED")


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class KafkaReaderContractTests(unittest.TestCase):
    """Messages must be accepted/rejected exactly as TV3's stream parser expects."""

    def test_parse_and_dedup_with_stream_reader(self):
        from pyspark.sql import SparkSession

        from spark.streaming.kafka_reader import deduplicate_events, parse_kafka_events

        spark = (SparkSession.builder.master("local[2]").appName("producer-contract")
                 .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        config = SimulatorConfig(seed=5, late_event_probability=0.05, duplicate_probability=0.05,
                                 invalid_event_probability=0.05)
        messages = HospitalSimulator(config).messages(3000)
        rows = [("hospital.events", 0, offset, NOW, bytearray(m.value), m.kind)
                for offset, m in enumerate(messages)]
        records = spark.createDataFrame(
            rows, "topic string, partition int, offset long, timestamp timestamp, value binary, kind string")
        parsed = parse_kafka_events(records.drop("kind")).join(
            records.select("offset", "kind"), "offset")
        rejected = Counter(row.kind for row in parsed.where("validation_error IS NOT NULL").collect())
        self.assertEqual(set(rejected), {"invalid"})
        self.assertEqual(rejected["invalid"], Counter(m.kind for m in messages)["invalid"])
        unique = deduplicate_events(parse_kafka_events(records.drop("kind"))).count()
        expected = sum(1 for m in messages if m.kind in ("valid", "late"))
        self.assertEqual(unique, expected)


if __name__ == "__main__":
    unittest.main()
