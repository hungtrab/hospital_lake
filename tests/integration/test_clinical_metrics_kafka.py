"""Opt-in live Kafka -> Spark -> TV2 clinical snapshot check."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid


@unittest.skipUnless(os.environ.get("RUN_CLINICAL_KAFKA_TESTS") == "1", "start Kafka and set RUN_CLINICAL_KAFKA_TESTS=1")
class ClinicalKafkaTests(unittest.TestCase):
    def test_live_keyed_events_reach_clinical_outputs(self):
        import pyspark
        from pyspark.sql import SparkSession, functions as F
        from kafka import KafkaAdminClient, KafkaProducer
        from kafka.admin import NewTopic
        from spark.streaming.kafka_reader import kafka_connector_package, parse_kafka_events, read_kafka_events
        from spark.streaming.clinical_metrics import start_queries

        broker = os.environ.get("CLINICAL_TEST_KAFKA", "localhost:9092")
        topic = "hospital.tv2-test-" + uuid.uuid4().hex
        admin = KafkaAdminClient(bootstrap_servers=broker)
        spark = producer = None
        queries = []
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
            producer = KafkaProducer(bootstrap_servers=broker, acks="all")
            base = dict(source="fixture", version=1, event_time="2026-11-17T10:01:00Z")
            admission = dict(base, event_id="a", event_type="ADMISSION", payload=dict(patient_id="p1", encounter_id="e1", department="ICU"))
            lab = dict(base, event_id="l", event_type="LAB_RESULT", payload=dict(patient_id="p1", encounter_id="e1", result_value=150, reference_low=70, reference_high=100))
            for row in (admission, admission, lab, lab):
                producer.send(topic, key=b"p1", value=json.dumps(row).encode()).get(timeout=20)
            producer.send(topic, key=b"p1", value=b"{").get(timeout=20)
            producer.flush()
            spark = (SparkSession.builder.master("local[2]").appName("tv2-live-kafka-test")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.jars.packages", kafka_connector_package(pyspark.__version__)).getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            with tempfile.TemporaryDirectory(prefix="clinical-kafka-") as temp:
                root = Path(temp).resolve()
                raw = read_kafka_events(spark, bootstrap_servers=broker, topics=topic, starting_offsets="earliest")
                queries = start_queries(parse_kafka_events(raw), state_dir=root, revision_base=1_000_000)
                for query in queries:
                    query.processAllAvailable()
                def output(name):
                    return spark.read.json([str(path) for path in (root / "output" / name).glob("batch-*")])
                self.assertEqual(output("icu").orderBy(F.desc("revision")).first().icu_occupancy, 1)
                self.assertEqual(output("lab").orderBy(F.desc("revision")).first().abnormal_labs, 1)
                self.assertEqual(output("department").agg(F.max("admissions_5m")).first()[0], 1)
                self.assertEqual(spark.read.parquet(str(root / "buffers" / "dedup")).count(), 2)
                rejected = spark.read.json(str(root / "output" / "quarantine"))
                self.assertEqual(rejected.count(), 1)
                self.assertIn("INVALID_JSON", rejected.first().validation_error)
                for query in reversed(queries):
                    query.stop()
                queries = []
        finally:
            for query in reversed(queries):
                query.stop()
            if spark is not None:
                spark.stop()
            if producer is not None:
                producer.close(timeout=10)
            admin.delete_topics([topic])
            admin.close()
