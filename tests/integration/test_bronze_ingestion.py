"""Real Parquet → Spark → Iceberg tests. No Docker or MinIO required.

Install requirements-spark.txt, then run make test-bronze. Maven access is needed
on first run. This local Hadoop catalog exercises writes, not the REST/S3 stack.
"""

import json
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.readwriter import DataFrameWriterV2
    from pyspark.sql.types import StringType, StructField, StructType
except ImportError:
    SparkSession = None

from generators.batch_generator.generate import FIELDS
from spark.batch.ingest_bronze import ENTITIES, METADATA_TYPES, ingest_bronze
from spark.common.iceberg import ICEBERG_VERSION

RAW_FIELDS = {
    **FIELDS,
    "medications": ("medication_id", "patient_id", "encounter_id", "drug_name", "dose",
                    "dose_unit", "route", "start_time", "end_time"),
    "vitals": ("measurement_id", "patient_id", "encounter_id", "heart_rate", "spo2",
               "systolic_bp", "diastolic_bp", "temperature", "measured_at"),
    "bed_events": ("event_id", "patient_id", "encounter_id", "department", "bed_id",
                   "event_type", "event_time"),
}


def row_values(frame):
    return Counter(json.dumps(row.asDict(), sort_keys=True) for row in frame.collect())


@unittest.skipUnless(os.environ.get("RUN_BRONZE_TESTS") == "1" and SparkSession,
                     "run make test-bronze with requirements-spark.txt installed")
class BronzeIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.storage = tempfile.TemporaryDirectory(prefix="hospital-bronze-")
        cls.spark = (SparkSession.builder.master("local[2]").appName("bronze-integration")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}")
                     .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.iceberg.type", "hadoop")
                     .config("spark.sql.catalog.iceberg.warehouse", cls.storage.name + "/warehouse")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE iceberg.bronze")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.storage.cleanup()

    def setUp(self):
        self.input = tempfile.TemporaryDirectory(prefix="hospital-parquet-")
        self.root = Path(self.input.name)
        self.addCleanup(self.input.cleanup)
        for entity in ENTITIES:
            self.spark.sql(f"DROP TABLE IF EXISTS iceberg.bronze.{entity}_raw PURGE")

    def fixture(self, entities):
        originals = {}
        for entity in entities:
            names = RAW_FIELDS[entity]
            schema = StructType([StructField(name, StringType()) for name in names])
            row = tuple("patient-1" if name == "patient_id" else "encounter-1"
                        if name == "encounter_id" else "raw-" + name for name in names)
            bad = (None,) + row[1:]
            raw = self.spark.createDataFrame([row, row, bad], schema)
            raw.write.parquet(str(self.root / f"{entity}.parquet"))
            columns = ", ".join(f"{name} STRING" for name in names)
            metadata = ", ".join(f"{name} {kind}" for name, kind in METADATA_TYPES.items())
            self.spark.sql(f"CREATE TABLE iceberg.bronze.{entity}_raw ({columns}, {metadata}) USING iceberg")
            originals[entity] = raw
        return originals

    def test_all_entities_preserve_raw_rows_metadata_and_batch_retry(self):
        originals = self.fixture(ENTITIES)
        with self.assertLogs("spark.batch.ingest_bronze", level="INFO") as logs:
            reports = ingest_bronze(self.spark, self.root, "batch-'001")
        self.assertEqual(len(logs.output), 6)
        self.assertTrue(all(report["input_rows"] == report["output_rows"] == 3 for report in reports))
        for entity in ENTITIES:
            data = self.spark.table(f"iceberg.bronze.{entity}_raw")
            self.assertEqual(row_values(data.select(*RAW_FIELDS[entity])), row_values(originals[entity]))
            for row in data.collect():
                self.assertTrue(row._source.startswith("file:"))
                self.assertIn(entity + ".parquet", row._source)
                self.assertIsNotNone(row._ingested_at)
                self.assertEqual(row._batch_id, "batch-'001")
                self.assertEqual(row._schema_version, 1)
                self.assertEqual(row._event_id, row.event_id if entity == "bed_events" else None)
        # Fresh SQL session; retry must use persisted Iceberg metadata/data, not Python state.
        retry = ingest_bronze(self.spark.newSession(), self.root, "batch-'001")
        self.assertTrue(all(report["status"] == "skipped" and report["output_rows"] == 0 for report in retry))
        ingest_bronze(self.spark, self.root, "batch-002")
        self.assertTrue(all(self.spark.table(f"iceberg.bronze.{entity}_raw").count() == 6 for entity in ENTITIES))

    def test_retry_recovers_after_one_table_commits(self):
        entities = ("patients", "encounters")
        self.fixture(entities)
        original_append = DataFrameWriterV2.append
        calls = 0

        def interrupt_second_write(writer):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected write failure")
            return original_append(writer)

        with patch.object(DataFrameWriterV2, "append", interrupt_second_write):
            with self.assertRaisesRegex(RuntimeError, "injected write failure"):
                ingest_bronze(self.spark, self.root, "recover-001", entities)
        reports = ingest_bronze(self.spark, self.root, "recover-001", entities)
        self.assertEqual([report["status"] for report in reports], ["skipped", "written"])
        for entity in entities:
            self.assertEqual(self.spark.table(f"iceberg.bronze.{entity}_raw").count(), 3)

    def test_generator_parquet_matches_shared_ddl_and_verifier_ids(self):
        from generators.batch_generator.generate import GeneratorConfig, generate_tables, write_dataset
        from scripts.verify_bronze import ENTITY_IDS, checksum, source_ids
        from spark.common.bronze import load_contracts

        config = GeneratorConfig.from_yaml(Path("generators/batch_generator/config.yaml"), patients=20)
        tables, defects = generate_tables(config)
        write_dataset(config, tables, defects, self.root)
        for contract in load_contracts():
            self.spark.sql(contract.ddl)
        reports = ingest_bronze(self.spark, self.root, "shared-contract")
        self.assertEqual({r["table"].split(".")[-1][:-4]: r["output_rows"] for r in reports},
                         {entity: table.num_rows for entity, table in tables.items()})
        for entity, id_column in ENTITY_IDS.items():
            actual = self.spark.table(f"iceberg.bronze.{entity}_raw")
            raw = self.spark.read.parquet(str(self.root / f"{entity}.parquet"))
            self.assertFalse(actual.select(*raw.columns).exceptAll(raw).limit(1).count())
            self.assertFalse(raw.exceptAll(actual.select(*raw.columns)).limit(1).count())
            expected_ids = source_ids(self.root, entity, id_column)
            self.assertEqual(checksum(expected_ids), checksum(row[0] for row in actual.select(id_column).collect()))

    def test_retry_rejects_changed_values_or_duplicate_counts_without_writing(self):
        originals = self.fixture(("patients",))
        original = originals["patients"]
        ingest_bronze(self.spark, self.root, "immutable-001", ("patients",))
        before = row_values(self.spark.table("iceberg.bronze.patients_raw").drop("_ingested_at"))
        from pyspark.sql import functions as F

        changed_inputs = (
            original.withColumn("city", F.lit("changed-city")),  # Same IDs and row count.
            original.unionByName(original.limit(1)),  # Added duplicate.
            original.limit(2),  # Removed raw row.
            self.spark.createDataFrame([], original.schema),
        )
        for changed in changed_inputs:
            changed.write.mode("overwrite").parquet(str(self.root / "patients.parquet"))
            with self.assertRaisesRegex(ValueError, "different raw rows"):
                ingest_bronze(self.spark, self.root, "immutable-001", ("patients",))
            after = row_values(self.spark.table("iceberg.bronze.patients_raw").drop("_ingested_at"))
            self.assertEqual(after, before)
        original.orderBy(F.col("patient_id").desc_nulls_first()).write.mode("overwrite").parquet(
            str(self.root / "patients.parquet"))
        retry = ingest_bronze(self.spark.newSession(), self.root, "immutable-001", ("patients",))
        self.assertEqual(retry[0]["status"], "skipped")

    def test_empty_source_is_a_safe_repeatable_noop(self):
        original = self.fixture(("patients",))["patients"]
        self.spark.createDataFrame([], original.schema).write.mode("overwrite").parquet(
            str(self.root / "patients.parquet"))
        for _ in range(2):
            report = ingest_bronze(self.spark, self.root, "empty-001", ("patients",))[0]
            self.assertEqual((report["status"], report["input_rows"], report["output_rows"]),
                             ("empty", 0, 0))
        self.assertEqual(self.spark.table("iceberg.bronze.patients_raw").count(), 0)

    def test_preflight_missing_input_or_schema_drift_does_not_write(self):
        self.fixture(("patients",))
        with self.assertRaisesRegex(ValueError, "encounters"):
            ingest_bronze(self.spark, self.root, "missing-input", ("patients", "encounters"))
        self.spark.sql("ALTER TABLE iceberg.bronze.patients_raw ADD COLUMN extra_field STRING")
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            ingest_bronze(self.spark, self.root, "drift", ("patients",))
        self.assertEqual(self.spark.table("iceberg.bronze.patients_raw").count(), 0)

    def test_missing_table_and_corrupt_parquet_fail_visibly(self):
        self.fixture(("patients",))
        self.spark.sql("DROP TABLE iceberg.bronze.patients_raw PURGE")
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            ingest_bronze(self.spark, self.root, "missing-table", ("patients",))
        (self.root / "encounters.parquet").write_text("corrupt parquet")
        self.spark.sql("CREATE TABLE iceberg.bronze.encounters_raw (encounter_id STRING) USING iceberg")
        with self.assertRaises(Exception) as error:
            ingest_bronze(self.spark, self.root, "corrupt", ("encounters",))
        self.assertIn("parquet", str(error.exception).lower())
        self.assertEqual(self.spark.table("iceberg.bronze.encounters_raw").count(), 0)


if __name__ == "__main__":
    unittest.main()
