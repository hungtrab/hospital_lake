"""Known-value tests for the historical Gold metrics (no Iceberg service needed)."""

import importlib.util
import unittest
from datetime import datetime, timezone


def utc_hour(value):
    """PySpark collects timestamps as naive local-time datetimes; key them in UTC."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H")


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class GoldMetricTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("gold-metric-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self):
        from pyspark.sql.types import (BooleanType, StringType, StructField,
                                       StructType, TimestampType)

        def ts(value):
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)

        encounters = self.spark.createDataFrame([
            ("e1", "p1", "ICU", ts("2026-10-01T10:15:00"), ts("2026-10-01T12:30:00")),
            ("e2", "p2", "CARDIOLOGY", ts("2026-10-01T11:00:00"), ts("2026-10-02T01:00:00")),
            ("e3", "p1", "ICU", ts("2026-10-02T02:00:00"), None),
        ], StructType([
            StructField("encounter_id", StringType()),
            StructField("patient_id", StringType()),
            StructField("department", StringType()),
            StructField("admission_time", TimestampType()),
            StructField("discharge_time", TimestampType()),
        ]))
        labs = self.spark.createDataFrame([
            ("l1", "Glucose", True, ts("2026-10-01T10:30:00")),
            ("l2", "Glucose", False, ts("2026-10-01T11:10:00")),
            ("l3", "Glucose", True, ts("2026-10-02T02:05:00")),
        ], ["lab_result_id", "test_name", "is_abnormal", "measured_at"])
        beds = self.spark.createDataFrame([
            ("b1", "transferred", ts("2026-10-01T11:20:00")),
        ], ["event_id", "event_type", "event_time"])
        return encounters, labs, beds

    def test_hourly_and_icu_metrics(self):
        from spark.batch.silver_to_gold import build_gold

        result = build_gold(self.spark, *self.fixture(),
                            as_of="2026-10-02T04:00:00Z",
                            capacities={"ICU": 2, "CARDIOLOGY": 2})
        rows = {utc_hour(row.hour): row
                for row in result["hospital_hourly_metrics"].collect()}
        self.assertEqual((rows["2026-10-01 10"].current_patients,
                          rows["2026-10-01 10"].admissions,
                          rows["2026-10-01 10"].active_icu_patients,
                          rows["2026-10-01 10"].abnormal_lab_results), (1, 1, 1, 1))
        self.assertEqual((rows["2026-10-01 11"].current_patients,
                          rows["2026-10-01 11"].transfers), (2, 1))
        self.assertEqual((rows["2026-10-01 12"].current_patients,
                          rows["2026-10-01 12"].discharges), (1, 1))
        self.assertEqual(rows["2026-10-02 04"].current_patients, 1)
        icu = {utc_hour(row.timestamp): row
               for row in result["icu_occupancy"].collect()}
        self.assertEqual((icu["2026-10-01 10"].occupied_beds,
                          icu["2026-10-01 10"].total_beds,
                          icu["2026-10-01 10"].available_beds,
                          icu["2026-10-01 10"].occupancy_rate), (1, 2, 1, 0.5))
        self.assertEqual(icu["2026-10-01 12"].occupied_beds, 0)

    def test_daily_metrics(self):
        from spark.batch.silver_to_gold import build_gold

        result = build_gold(self.spark, *self.fixture(),
                            as_of="2026-10-02T04:00:00Z",
                            capacities={"ICU": 2, "CARDIOLOGY": 2})
        dept = {(str(row.date), row.department): row
                for row in result["department_daily_metrics"].collect()}
        icu_day = dept["2026-10-01", "ICU"]
        self.assertEqual((icu_day.admissions, icu_day.discharges,
                          icu_day.unique_patients), (1, 1, 1))
        self.assertAlmostEqual(icu_day.avg_length_of_stay_hours, 2.25)
        self.assertAlmostEqual(icu_day.bed_utilization, 2.25 / 48)
        self.assertAlmostEqual(dept["2026-10-02", "CARDIOLOGY"].bed_utilization, 1 / 48)
        labs = {(str(row.date), row.test_name): row
                for row in result["lab_daily_metrics"].collect()}
        self.assertEqual((labs["2026-10-01", "Glucose"].total_tests,
                          labs["2026-10-01", "Glucose"].abnormal_tests,
                          labs["2026-10-01", "Glucose"].abnormal_rate), (2, 1, 0.5))

    def test_invalid_capacity_and_duplicate_encounter_fail(self):
        from spark.batch.silver_to_gold import build_gold

        encounters, labs, beds = self.fixture()
        with self.assertRaisesRegex(ValueError, "capacity"):
            build_gold(self.spark, encounters, labs, beds,
                       as_of="2026-10-02T04:00:00Z", capacities={"ICU": 2})
        duplicate = encounters.unionByName(encounters.limit(1))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build_gold(self.spark, duplicate, labs, beds,
                       as_of="2026-10-02T04:00:00Z",
                       capacities={"ICU": 2, "CARDIOLOGY": 2})


if __name__ == "__main__":
    unittest.main()
