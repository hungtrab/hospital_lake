"""Local Spark coverage for the shared batch data-quality framework."""

import logging
import unittest
import importlib.util


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark is not installed")
class DataQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("hospital-lake-data-quality-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_validation_quarantines_reasons_and_duplicate_groups(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from spark.common.data_quality import REASON_COLUMN, assess_dataframe

        schema = StructType([
            StructField("encounter_id", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("spo2", IntegerType(), True),
        ])
        frame = self.spark.createDataFrame([
            ("E1", "P1", 98),
            ("E2", None, 97),
            ("E3", "P3", 120),
            ("E4", "P4", 95),
            ("E4", "P4", 96),
        ], schema)
        logger = logging.getLogger("test.quality")

        with self.assertLogs(logger, level="INFO") as records:
            result = assess_dataframe(
                frame,
                required_columns=("encounter_id", "patient_id", "spo2"),
                rules={"SPO2_OUT_OF_RANGE": F.col("spo2").between(0, 100)},
                duplicate_keys=("encounter_id",),
                job_name="encounters_silver",
                logger=logger,
            )

        self.assertEqual(result.metrics.input_rows, 5)
        self.assertEqual(result.metrics.valid_rows, 1)
        self.assertEqual(result.metrics.invalid_rows, 2)
        self.assertEqual(result.metrics.duplicate_rows, 2)
        self.assertEqual([row.encounter_id for row in result.valid_rows.collect()], ["E1"])
        quarantined = [(row.encounter_id, row[REASON_COLUMN])
                       for row in result.quarantine_rows.collect()]
        self.assertEqual(len(quarantined), 4)
        self.assertIn(("E2", ["MISSING_PATIENT_ID"]), quarantined)
        self.assertIn(("E3", ["SPO2_OUT_OF_RANGE"]), quarantined)
        self.assertEqual([reason for entity, reason in quarantined if entity == "E4"],
                         [["DUPLICATE_KEY"], ["DUPLICATE_KEY"]])
        self.assertIn("input_rows=5 valid_rows=1 invalid_rows=2 duplicate_rows=2", records.output[0])

    def test_null_rule_result_is_invalid_and_unique_rows_pass(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from spark.common.data_quality import assess_dataframe

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ])
        frame = self.spark.createDataFrame([("one", None), ("two", 4)], schema)
        result = assess_dataframe(
            frame,
            required_columns=("id",),
            rules={"VALUE_POSITIVE": F.col("value") > 0},
            duplicate_keys=("id",),
            job_name="test_null_predicate",
        )

        self.assertEqual(result.metrics.input_rows, 2)
        self.assertEqual(result.metrics.valid_rows, 1)
        self.assertEqual(result.metrics.invalid_rows, 1)
        self.assertEqual(result.metrics.duplicate_rows, 0)
        self.assertEqual(result.quarantine_rows.first()._quality_reason_codes, ["VALUE_POSITIVE"])

    def test_rejects_duplicate_key_not_declared_required(self):
        from pyspark.sql.types import StringType, StructField, StructType

        from spark.common.data_quality import assess_dataframe

        frame = self.spark.createDataFrame([], StructType([
            StructField("id", StringType(), True),
        ]))
        with self.assertRaisesRegex(ValueError, "duplicate_keys"):
            assess_dataframe(
                frame,
                required_columns=(),
                rules={},
                duplicate_keys=("id",),
                job_name="invalid_contract",
            )

    def test_invalid_variant_does_not_publish_a_valid_duplicate_winner(self):
        from pyspark.sql import functions as F
        from spark.common.data_quality import assess_dataframe

        frame = self.spark.createDataFrame([("same", 4), ("same", -1)], "id STRING, value INT")
        result = assess_dataframe(frame, required_columns=("id",),
                                  rules={"VALUE_POSITIVE": F.col("value") > 0},
                                  duplicate_keys=("id",), job_name="mixed-conflict")
        self.assertEqual(result.valid_rows.count(), 0)
        self.assertEqual((result.metrics.input_rows, result.metrics.invalid_rows,
                          result.metrics.duplicate_rows, result.metrics.valid_rows), (2, 1, 1, 0))
        reasons = {row.value: set(row._quality_reason_codes) for row in result.quarantine_rows.collect()}
        self.assertEqual(reasons, {4: {"DUPLICATE_KEY"}, -1: {"VALUE_POSITIVE", "DUPLICATE_KEY"}})


if __name__ == "__main__":
    unittest.main()
