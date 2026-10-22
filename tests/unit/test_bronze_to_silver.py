"""Local Spark tests for Silver patients/encounters (no Iceberg service needed)."""

import importlib.util
import re
import unittest
from datetime import datetime, timezone

from spark.common.normalization import CANONICAL_DEPARTMENTS, DEPARTMENT_ALIASES, department_key

PROCESSED_AT = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
METADATA_DDL = ("_source STRING, _ingested_at TIMESTAMP, _batch_id STRING, "
                "_event_id STRING, _schema_version INT")
PATIENTS_DDL = ("patient_id STRING, gender STRING, birth_year INT, city STRING, "
                "created_at TIMESTAMP, " + METADATA_DDL)
ENCOUNTERS_DDL = ("encounter_id STRING, patient_id STRING, encounter_type STRING, department STRING, "
                  "admission_time TIMESTAMP, discharge_time TIMESTAMP, status STRING, " + METADATA_DDL)


def ts(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc) if value else None


def meta(batch="b1"):
    return ("file.parquet", ts("2026-10-22T00:00:00"), batch, None, 1)


class NormalizationTests(unittest.TestCase):
    def test_generator_aliases_map_to_their_canonical_department(self):
        from generators.batch_generator.schemas import DEPARTMENT_ALIASES as GENERATED

        self.assertEqual(set(GENERATED), set(CANONICAL_DEPARTMENTS))
        for canonical, aliases in GENERATED.items():
            for spelling in (canonical, *aliases):
                self.assertEqual(DEPARTMENT_ALIASES.get(department_key(spelling)), canonical, spelling)


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class BronzeToSilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("bronze-to-silver-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.ansi.enabled", "true")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def entity(self, name):
        from spark.batch.bronze_to_silver import ENTITIES

        return next(entity for entity in ENTITIES if entity.name == name)

    def patients_bronze(self):
        rows = [
            ("P1", "m", 1980, " Hanoi ", ts("2025-01-01T00:00:00"), *meta("b1")),
            ("P1", "m", 1980, " Hanoi ", ts("2025-01-01T00:00:00"), *meta("b2")),  # re-ingested
            ("P2", "F", 1990, "", ts("2025-02-01T00:00:00"), *meta()),
            ("P3", "X", 1990, "Hue", ts("2025-02-01T00:00:00"), *meta()),          # bad gender
            ("P4", "F", 2030, "Hue", ts("2025-02-01T00:00:00"), *meta()),          # born after registration
            ("P5", "F", 1970, "Hue", ts("2025-02-01T00:00:00"), *meta()),
            ("P5", "M", 1970, "Hue", ts("2025-02-01T00:00:00"), *meta()),          # conflicting duplicate
        ]
        return self.spark.createDataFrame(rows, PATIENTS_DDL)

    def encounters_bronze(self):
        rows = [
            ("E1", "P1", "ICU", " intensive care ", ts("2026-01-01T10:00:00"), ts("2026-01-02T12:30:00"), "Discharged", *meta()),
            ("E1", "P1", "ICU", " intensive care ", ts("2026-01-01T10:00:00"), ts("2026-01-02T12:30:00"), "Discharged", *meta("b0")),
            ("E2", "P2", "emergency", "ER", ts("2026-01-03T08:00:00"), None, "admitted", *meta()),
            ("E3", "P1", "inpatient", "Cardio", ts("2026-01-05T08:00:00"), ts("2026-01-04T08:00:00"), "discharged", *meta()),
            ("E4", "P9", "inpatient", "SURGERY", ts("2026-01-05T08:00:00"), ts("2026-01-06T08:00:00"), "discharged", *meta()),
            ("E5", "P2", "inpatient", "Oncology", ts("2026-01-05T08:00:00"), ts("2026-01-06T08:00:00"), "discharged", *meta()),
            ("E6", "P2", "inpatient", "SURGERY", ts("2026-01-05T08:00:00"), None, "discharged", *meta()),
            ("E7", "P2", "outpatient", "IM", ts("2026-01-07T08:00:00"), ts("2026-01-07T09:00:00"), "discharged", *meta()),
            ("E7", "P2", "outpatient", "IM", ts("2026-01-07T08:00:00"), ts("2026-01-07T10:00:00"), "discharged", *meta()),
        ]
        return self.spark.createDataFrame(rows, ENCOUNTERS_DDL)

    def run_patients(self):
        from spark.batch.bronze_to_silver import transform_entity

        return transform_entity(self.entity("patients"), self.patients_bronze(), {}, PROCESSED_AT)

    def run_encounters(self):
        from spark.batch.bronze_to_silver import transform_entity

        patients = self.run_patients().valid
        return transform_entity(self.entity("encounters"), self.encounters_bronze(),
                                {"patients": patients}, PROCESSED_AT)

    @staticmethod
    def reasons(result):
        return {row.record_key: sorted(row.reason_codes) for row in result.quarantine.collect()}

    def test_patients_normalized_validated_and_deduplicated(self):
        result = self.run_patients()
        valid = {row.patient_id: row for row in result.valid.collect()}
        self.assertEqual(set(valid), {"P1", "P2"})
        self.assertEqual((valid["P1"].gender, valid["P1"].city, valid["P1"]._bronze_batch_id), ("M", "Hanoi", "b1"))
        self.assertIsNone(valid["P2"].city)
        self.assertEqual(self.reasons(result), {
            "P3": ["INVALID_GENDER"], "P4": ["IMPLAUSIBLE_BIRTH_YEAR"],
            "P5": ["DUPLICATE_KEY"]})
        self.assertEqual(result.report["bronze_rows"], 7)
        self.assertEqual(result.report["exact_duplicate_rows"], 1)
        self.assertEqual(result.report["output_rows"], 2)
        self.assertEqual(result.report["quarantine_rows"], 4)

    def test_encounters_canonical_values_and_length_of_stay(self):
        result = self.run_encounters()
        valid = {row.encounter_id: row for row in result.valid.collect()}
        self.assertEqual(set(valid), {"E1", "E2"})
        self.assertEqual((valid["E1"].department, valid["E1"].encounter_type, valid["E1"].status),
                         ("ICU", "icu", "discharged"))
        self.assertAlmostEqual(valid["E1"].length_of_stay_hours, 26.5)
        self.assertEqual(valid["E1"]._bronze_batch_id, "b0")
        self.assertEqual(valid["E2"].department, "EMERGENCY")
        self.assertIsNone(valid["E2"].length_of_stay_hours)

    def test_encounters_quarantine_reasons(self):
        result = self.run_encounters()
        self.assertEqual(self.reasons(result), {
            "E3": ["DISCHARGE_BEFORE_ADMISSION"],
            "E4": ["UNKNOWN_PATIENT"],
            "E5": ["UNKNOWN_DEPARTMENT"],
            "E6": ["STATUS_DISCHARGE_MISMATCH"],
            "E7": ["DUPLICATE_KEY"],
        })
        self.assertEqual(result.report["quarantine_rows"], 6)
        raw = next(row.raw_record for row in result.quarantine.collect() if row.record_key == "E5")
        self.assertIn('"department":"Oncology"', raw)
        self.assertNotIn("_chk_", raw)

    def test_rerun_is_deterministic(self):
        first = self.run_encounters().valid.drop("_processed_at")
        second = self.run_encounters().valid.drop("_processed_at")
        self.assertEqual(sorted(first.collect()), sorted(second.collect()))

    def test_outputs_match_silver_ddl(self):
        from spark.batch.bronze_to_silver import ddl_statements

        columns = {}
        for statement in ddl_statements():
            match = re.match(r"CREATE TABLE IF NOT EXISTS iceberg\.silver\.(\w+) \((.*?)\) USING", statement, re.S)
            if match:
                columns[match.group(1)] = self.spark.createDataFrame(
                    [], match.group(2).replace("TIMESTAMP_LTZ", "TIMESTAMP")).schema
        signature = lambda schema: [(f.name, f.dataType.simpleString()) for f in schema]
        self.assertEqual(signature(self.run_patients().valid.schema), signature(columns["patients"]))
        encounters = self.run_encounters()
        self.assertEqual(signature(encounters.valid.schema), signature(columns["encounters"]))
        self.assertEqual(signature(encounters.quarantine.schema), signature(columns["quarantine"]))

    def test_silver_encounters_satisfy_gold_job(self):
        from spark.batch.silver_to_gold import build_gold

        encounters = self.run_encounters().valid
        labs = self.spark.createDataFrame([], "lab_result_id STRING, test_name STRING, is_abnormal BOOLEAN, measured_at TIMESTAMP")
        beds = self.spark.createDataFrame([], "event_id STRING, event_type STRING, event_time TIMESTAMP")
        gold = build_gold(self.spark, encounters, labs, beds, as_of="2026-01-04T00:00:00Z",
                          capacities={name: 10 for name in CANONICAL_DEPARTMENTS})
        self.assertGreater(gold["department_daily_metrics"].count(), 0)


if __name__ == "__main__":
    unittest.main()
