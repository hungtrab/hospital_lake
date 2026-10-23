"""Clinical Silver boundary, FK propagation, quarantine and Gold contracts."""
from __future__ import annotations

import importlib.util
import re
import unittest
from datetime import datetime, timezone

from spark.batch.bronze_to_silver import ENTITIES, ddl_statements, transform_entity
from spark.common.bronze import load_contracts

NOW = datetime(2026, 1, 2, tzinfo=timezone.utc)


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class SilverClinicalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-clinical-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schemas = {c.name[:-4]: c.columns for c in load_contracts()}
        cls.encounters = cls.spark.createDataFrame([("E1", "P1")], "encounter_id STRING, patient_id STRING")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def record(self, entity, key, **changes):
        values = {
            "patient_id": "P1", "encounter_id": "E1", "_source": "fixture", "_ingested_at": NOW,
            "_batch_id": "b1", "_event_id": None, "_schema_version": 1,
        }
        defaults = {
            "lab_results": dict(lab_result_id=key, test_code="GLU", test_name="Glucose", result_value=100.0,
                                unit="mg/dL", reference_low=70.0, reference_high=100.0,
                                is_abnormal=True, measured_at=NOW),
            "bed_events": dict(event_id=key, department=" ER ", bed_id="B1", event_type=" ASSIGNED ", event_time=NOW),
            "medications": dict(medication_id=key, drug_name=" Saline ", dose=250.0, dose_unit=" mL ",
                                route=" Intravenous ", start_time=NOW, end_time=None),
            "vitals": dict(measurement_id=key, heart_rate=70, spo2=98.0, systolic_bp=120, diastolic_bp=80,
                           temperature=37.0, measured_at=NOW),
        }
        values.update(defaults[entity])
        values.update(changes)
        return values

    def result(self, entity, rows):
        definition = next(e for e in ENTITIES if e.name == entity)
        bronze = self.spark.createDataFrame(rows, self.schemas[entity])
        return transform_entity(definition, bronze, {"encounters": self.encounters}, NOW)

    def reasons(self, result):
        return {r.record_key: set(r.reason_codes) for r in result.quarantine.collect()}

    def test_labs_recalculate_inclusive_ranges_and_preserve_raw_rejections(self):
        rows = [self.record("lab_results", "low", result_value=70.0), self.record("lab_results", "high"),
                self.record("lab_results", "outside", result_value=100.1, is_abnormal=False),
                self.record("lab_results", "range", reference_low=101.0),
                self.record("lab_results", "nan", result_value=float("nan")),
                self.record("lab_results", "inf", reference_high=float("inf")),
                self.record("lab_results", "null", result_value=None)]
        result = self.result("lab_results", rows)
        self.assertEqual({r.lab_result_id: r.is_abnormal for r in result.valid.collect()},
                         {"low": False, "high": False, "outside": True})
        self.assertEqual(self.reasons(result), {"range": {"INVALID_REFERENCE_RANGE"}, "nan": {"INVALID_LAB_VALUE"},
                                               "inf": {"INVALID_REFERENCE_RANGE"}, "null": {"INVALID_LAB_VALUE"}})
        rejected = next(r for r in result.quarantine.collect() if r.record_key == "range")
        self.assertIn('"reference_low":101.0', rejected.raw_record)
        self.assertNotIn("_chk_", rejected.raw_record)

    def test_each_entity_rejects_quarantined_parent_and_patient_mismatch(self):
        for entity in ("lab_results", "bed_events", "medications", "vitals"):
            with self.subTest(entity=entity):
                result = self.result(entity, [self.record(entity, "orphan", encounter_id="quarantined-E"),
                                              self.record(entity, "mismatch", patient_id="P2"),
                                              self.record(entity, "missing", patient_id=None)])
                reasons = self.reasons(result)
                self.assertEqual(reasons["orphan"], {"UNKNOWN_ENCOUNTER"})
                self.assertIn("ENCOUNTER_PATIENT_MISMATCH", reasons["mismatch"])
                self.assertIn("MISSING_PATIENT_ID", reasons["missing"])
                self.assertEqual(result.valid.count(), 0)

    def test_beds_normalize_and_reject_unknown_values(self):
        result = self.result("bed_events", [self.record("bed_events", "valid"),
                  self.record("bed_events", "transfer", event_type=" TRANSFERRED ", department="icu"),
                  self.record("bed_events", "release", event_type="released"),
                  self.record("bed_events", "bad", event_type="occupied"),
                  self.record("bed_events", "dept", department="Oncology")])
        self.assertEqual({r.event_id: (r.department, r.event_type) for r in result.valid.collect()},
                         {"valid": ("EMERGENCY", "assigned"), "transfer": ("ICU", "transferred"),
                          "release": ("EMERGENCY", "released")})
        self.assertEqual(self.reasons(result), {"bad": {"INVALID_BED_EVENT_TYPE"}, "dept": {"UNKNOWN_DEPARTMENT"}})

    def test_medications_validate_dose_times_and_keep_units(self):
        result = self.result("medications", [self.record("medications", "valid"),
                  self.record("medications", "zero", dose=0.0),
                  self.record("medications", "nan", dose=float("nan")),
                  self.record("medications", "time", end_time=datetime(2026, 1, 1, tzinfo=timezone.utc))])
        row = result.valid.first()
        self.assertEqual((row.medication_id, row.dose_unit, row.route, row.drug_name),
                         ("valid", "mL", "intravenous", "Saline"))
        self.assertIsNone(row.end_time)
        self.assertEqual(self.reasons(result), {"zero": {"INVALID_DOSE"}, "nan": {"INVALID_DOSE"},
                                               "time": {"END_BEFORE_START"}})

    def test_vitals_validate_boundaries_and_nonfinite_values(self):
        rows = [self.record("vitals", "valid"), self.record("vitals", "hr", heart_rate=0),
                self.record("vitals", "spo2", spo2=100.1), self.record("vitals", "nan", spo2=float("nan")),
                self.record("vitals", "temp", temperature=46.0), self.record("vitals", "bp", systolic_bp=70),
                self.record("vitals", "null", temperature=None),
                self.record("vitals", "lower", spo2=0.0, temperature=25.0),
                self.record("vitals", "upper", spo2=100.0, temperature=45.0)]
        result = self.result("vitals", rows)
        self.assertEqual({r.measurement_id for r in result.valid.collect()}, {"valid", "lower", "upper"})
        self.assertEqual(self.reasons(result), {"hr": {"INVALID_HEART_RATE"}, "spo2": {"INVALID_SPO2"},
                         "nan": {"INVALID_SPO2"}, "temp": {"INVALID_TEMPERATURE"},
                         "bp": {"INVALID_BLOOD_PRESSURE"}, "null": {"INVALID_TEMPERATURE"}})

    def test_duplicates_lineage_reruns_and_output_schemas_for_all_entities(self):
        schemas = {}
        for statement in ddl_statements():
            match = re.match(r"CREATE TABLE IF NOT EXISTS iceberg\.silver\.(\w+) \((.*?)\) USING", statement, re.S)
            if match:
                schemas[match[1]] = self.spark.createDataFrame([], match[2]).schema
        signature = lambda schema: [(f.name, f.dataType.simpleString()) for f in schema]
        for entity in ("lab_results", "bed_events", "medications", "vitals"):
            with self.subTest(entity=entity):
                row = self.record(entity, "valid")
                rows = [row, dict(row, _batch_id="b0"), self.record(entity, "conflict"),
                        self.record(entity, "conflict", patient_id="P1", _batch_id="b2", **{
                            {"lab_results": "result_value", "bed_events": "bed_id", "medications": "dose", "vitals": "heart_rate"}[entity]:
                            {"lab_results": 99.0, "bed_events": "B2", "medications": 100.0, "vitals": 80}[entity]})]
                first = self.result(entity, rows)
                self.assertEqual(first.report["exact_duplicate_rows"], 1)
                self.assertEqual(first.report["duplicate_key_rows"], 2)
                self.assertEqual(first.valid.first()._bronze_batch_id, "b0")
                self.assertEqual(self.reasons(first), {"conflict": {"DUPLICATE_KEY"}})
                self.assertEqual(signature(first.valid.schema), signature(schemas[entity]))
                self.assertEqual(signature(first.quarantine.schema), signature(schemas["quarantine"]))
                self.assertEqual(first.valid.collect(), self.result(entity, rows).valid.collect())

    def test_invalid_conflicting_variant_never_selects_a_valid_winner(self):
        changes = {"lab_results": {"result_value": float("nan")},
                   "bed_events": {"event_type": "invalid"},
                   "medications": {"dose": -1.0}, "vitals": {"spo2": 101.0}}
        for entity, invalid in changes.items():
            with self.subTest(entity=entity):
                result = self.result(entity, [self.record(entity, "same"), self.record(entity, "same", **invalid)])
                self.assertEqual(result.valid.count(), 0)
                rejected = result.quarantine.collect()
                self.assertEqual(len(rejected), 2)
                self.assertTrue(all("DUPLICATE_KEY" in row.reason_codes for row in rejected))
                self.assertEqual((result.report["invalid_rows"], result.report["duplicate_key_rows"],
                                  result.report["quarantine_rows"]), (1, 1, 2))
