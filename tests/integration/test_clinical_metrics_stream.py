"""Opt-in real Structured Streaming state/restart test; no service mocks."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest


@unittest.skipUnless(os.environ.get("RUN_CLINICAL_STREAM_TESTS") == "1", "set RUN_CLINICAL_STREAM_TESTS=1 for live local streaming")
class ClinicalMetricsStreamTests(unittest.TestCase):
    def test_windows_late_duplicate_quarantine_and_restart(self):
        from pyspark.sql import SparkSession, functions as F
        from spark.streaming.clinical_metrics import runtime_identity, start_queries
        from spark.streaming.kafka_reader import parse_kafka_events

        spark = (SparkSession.builder.master("local[2]").appName("clinical-stream-integration")
                 .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        queries = []
        with tempfile.TemporaryDirectory(prefix="clinical-stream-") as temp:
            root = Path(temp).resolve()
            incoming = root / "incoming"
            incoming.mkdir()
            state = root / "state"
            state.mkdir()
            identity = runtime_identity(state, {"source": "file-fixture"})

            def event(key, kind, minute, encounter="e1", **payload):
                return dict(event_id=key, event_type=kind, event_time="2026-11-17T10:" + minute + "Z",
                            source="fixture", version=1, payload=dict(patient_id="p1", encounter_id=encounter, **payload))

            def send(number, events):
                rows = [{"value": json.dumps(item), "offset": number * 100 + i} for i, item in enumerate(events)]
                (incoming / (str(number) + ".json")).write_text("\n".join(json.dumps(row) for row in rows) + "\n")

            def start():
                raw = (spark.readStream.schema("value STRING, offset LONG").option("maxFilesPerTrigger", 1)
                       .json(str(incoming)).withColumn("value", F.col("value").cast("binary"))
                       .withColumn("topic", F.lit("file-fixture")).withColumn("partition", F.lit(0))
                       .withColumn("timestamp", F.current_timestamp()))
                return start_queries(parse_kafka_events(raw), state_dir=state,
                                     revision_base=identity["revision_base"])

            def drain():
                for query in queries:
                    query.processAllAvailable()

            def rows(metric):
                paths = sorted((state / "output" / metric).glob("batch-*"))
                return spark.read.json([str(path) for path in paths])

            def latest_icu():
                return rows("icu").orderBy(F.desc("revision")).first().icu_occupancy

            admission = event("admit", "ADMISSION", "00:00", department="ICU")
            abnormal = event("lab", "LAB_RESULT", "01:00", result_value=101, reference_low=70, reference_high=100)
            try:
                send(0, [admission, abnormal, event("bad", "ADMISSION", "01:00", department="UNKNOWN")])
                queries = start()
                drain()
                self.assertEqual(latest_icu(), 1)
                self.assertEqual(rows("lab").orderBy(F.desc("revision")).first().abnormal_labs, 1)
                self.assertTrue((state / "output" / "quarantine" / "_spark_metadata").exists())
                quarantined = spark.read.json(str(state / "output" / "quarantine")).first()
                self.assertIn("UNKNOWN_DEPARTMENT", quarantined.validation_error)
                send(1, [admission, abnormal])
                drain()
                self.assertEqual(latest_icu(), 1)
                # Kafka/file replay produces no second lab in the dedup buffer.
                accepted_labs = spark.read.parquet(str(state / "buffers" / "dedup")).where("event_type = 'LAB_RESULT'")
                self.assertEqual(accepted_labs.count(), 1)
                old_revision = rows("lab").agg(F.max("revision")).first()[0]
                for query in reversed(queries):
                    query.stop()
                queries = []
                self.assertEqual(runtime_identity(state, {"source": "file-fixture"}), identity)
                send(2, [event("discharge", "DISCHARGE", "30:00", department="ICU"),
                         event("advance", "LAB_RESULT", "30:00", result_value=200, reference_low=70, reference_high=100)])
                queries = start()
                drain()
                self.assertEqual(latest_icu(), 0)
                self.assertGreater(rows("lab").agg(F.max("revision")).first()[0], old_revision)
                send(3, [event("too-late", "LAB_RESULT", "02:00", result_value=200, reference_low=70, reference_high=100),
                         admission])
                drain()
                self.assertEqual(latest_icu(), 0, "old admission resurrected discharged ICU encounter")
                self.assertEqual(spark.read.parquet(str(state / "buffers" / "dedup"))
                                 .where("event_id = 'too-late'").count(), 0)
                # Still retained for traceability/replay ahead of the window watermark.
                self.assertEqual(spark.read.parquet(str(state / "buffers" / "accepted"))
                                 .where("event_id = 'too-late'").count(), 1)
                self.assertTrue(any(any(op.numRowsDroppedByWatermark > 0 for op in p.stateOperators)
                                    for p in queries[2].recentProgress))
                for index in (3, 4):
                    self.assertTrue(any(any(op.numRowsRemoved > 0 for op in p.stateOperators)
                                        for p in queries[index].recentProgress), "window state did not evict")
            finally:
                for query in reversed(queries):
                    query.stop()
                spark.stop()
