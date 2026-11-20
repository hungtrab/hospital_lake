"""Local Parquet -> actual Silver -> actual Gold acceptance scenarios."""

import importlib.util
import json

import pytest


@pytest.mark.skipif(importlib.util.find_spec("pyspark") is None, reason="pyspark is not installed")
def test_dirty_scenarios_preserve_raw_account_for_rows_and_feed_gold(tmp_path):
    from pyspark.sql import SparkSession
    from scripts.data_quality_demo import run_demo

    spark = (SparkSession.builder.master("local[2]").appName("quality-demo-test")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.ansi.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        output, summary = run_demo(spark, tmp_path)
        saved = json.loads((output / "report.json").read_text())
        assert saved == summary
        assert summary["scenario_count"] == 36
        assert all(case["passed"] for case in summary["scenarios"])
        assert all(report["rerun_identical"] for report in summary["entities"])
        assert sum(report["bronze_rows"] for report in summary["entities"]) == 39
        assert sum(report["output_rows"] for report in summary["entities"]) == 11
        assert sum(report["quarantine_rows"] for report in summary["entities"]) == 27
        assert (output / "report.md").is_file()
        assert len(list((output / "bronze").iterdir())) == 6
        assert len(list((output / "gold").iterdir())) == 4
    finally:
        spark.stop()
