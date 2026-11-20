"""Run synthetic dirty-data scenarios through the real batch Silver/Gold code.

Local Parquet fixtures only: never write to the running Iceberg warehouse.
Each invocation creates a new output directory with observed before/after data.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import sys

from spark.batch.bronze_to_silver import ENTITIES, ddl_statements, transform_entity
from spark.batch.silver_to_gold import build_gold
from spark.common.bronze import load_contracts
from spark.common.normalization import CANONICAL_DEPARTMENTS

FIXTURE = Path(__file__).resolve().parents[1] / "data/samples/dirty_scenarios.json"
PROCESSED_AT = datetime(2026, 1, 3, tzinfo=timezone.utc)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fixture_rows(document, entity, schema):
    """Build raw rows without ever reading expected outcomes into the source."""
    from pyspark.sql.types import DoubleType, TimestampType

    rows = []
    for case in document["cases"]:
        if case["entity"] != entity.name:
            continue
        for changes in case["rows"]:
            row = dict(document["defaults"][entity.name], **{
                entity.key: case["id"], "_source": "synthetic-dirty-scenarios",
                "_ingested_at": PROCESSED_AT, "_batch_id": "quality-demo-b1",
                "_event_id": None, "_schema_version": 1,
            })
            row.update(changes)
            require(set(row) == set(schema.names), f"{case['id']}: fixture schema mismatch")
            for field in schema:
                value = row[field.name]
                if isinstance(field.dataType, TimestampType) and isinstance(value, str):
                    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    require(instant.tzinfo is not None, f"{case['id']}: timestamp needs an offset")
                    row[field.name] = instant
                elif isinstance(field.dataType, DoubleType) and value is not None:
                    row[field.name] = float(value)
            rows.append(row)
    return rows


def records(frame):
    # Spark renders timestamps using the UTC session rather than host TZ, and
    # serializes non-finite numbers as strings, keeping the report valid JSON.
    return sorted((json.loads(row) for row in frame.toJSON().collect()),
                  key=lambda row: json.dumps(row, sort_keys=True))


def signature(frame):
    return [(f.name, f.dataType.simpleString()) for f in frame.schema]


def run_demo(spark, output_dir, fixture=FIXTURE):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", "false")
    document = json.loads(Path(fixture).read_text(encoding="utf-8"))
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="run-", dir=base.resolve()))
    contracts = {c.name[:-4]: c.columns for c in load_contracts()}
    silver_schemas = {}
    for statement in ddl_statements():
        match = re.match(r"CREATE TABLE IF NOT EXISTS iceberg\.silver\.(\w+) \((.*?)\) USING", statement, re.S)
        if match:
            silver_schemas[match[1]] = signature(spark.createDataFrame([], match[2]))

    silver, reports, observations = {}, [], []
    for entity in ENTITIES:
        schema = spark.createDataFrame([], contracts[entity.name]).schema
        source_rows = fixture_rows(document, entity, schema)
        source = spark.createDataFrame(source_rows, schema)
        bronze_path = output / "bronze" / entity.name
        source.coalesce(1).write.parquet(str(bronze_path))
        bronze = spark.read.parquet(str(bronze_path))
        before = records(bronze)
        result = transform_entity(entity, bronze, silver, PROCESSED_AT)
        require(signature(result.valid) == silver_schemas[entity.name], f"{entity.name}: Silver schema drift")
        require(signature(result.quarantine) == silver_schemas["quarantine"], "quarantine schema drift")
        valid_path = output / "silver" / entity.name
        quarantine_path = output / "quarantine" / entity.name
        result.valid.coalesce(1).write.parquet(str(valid_path))
        result.quarantine.coalesce(1).write.parquet(str(quarantine_path))
        silver[entity.name] = spark.read.parquet(str(valid_path))
        valid = records(silver[entity.name])
        rejected = records(spark.read.parquet(str(quarantine_path)))
        report = result.report
        require(report["bronze_rows"] == len(valid) + len(rejected) + report["exact_duplicate_rows"],
                f"{entity.name}: rows disappeared")
        reason_counts = Counter(code for row in rejected for code in row["reason_codes"])
        report["reason_counts"] = dict(sorted(reason_counts.items()))

        # Retry with different row/partition ordering; do not compare runtime.
        rerun = transform_entity(entity, bronze.repartition(2), silver, PROCESSED_AT)
        require(records(rerun.valid) == valid, f"{entity.name}: retry changed Silver")
        require(records(rerun.quarantine) == rejected, f"{entity.name}: retry changed quarantine")
        require(records(spark.read.parquet(str(bronze_path))) == before, f"{entity.name}: Bronze changed")
        report["rerun_identical"] = True

        cases = [case for case in document["cases"] if case["entity"] == entity.name]
        expected_duplicates = sum(case.get("exact_duplicates", 0) for case in cases)
        require(report["exact_duplicate_rows"] == expected_duplicates, f"{entity.name}: wrong duplicate count")
        covered_valid, covered_rejected = 0, 0
        for case in cases:
            keys = {row.get(entity.key, case["id"]) for row in case["rows"]}
            raw = [row for row in before if row[entity.key] in keys]
            accepted = [row for row in valid if row[entity.key] in keys]
            quarantine = [row for row in rejected if row["record_key"] in keys]
            expected_valid = case.get("valid", [])
            require(len(accepted) == len(expected_valid), f"{case['id']}: wrong accepted count")
            for expected in expected_valid:
                require(any(all(row[field] == value for field, value in expected.items()) for row in accepted),
                        f"{case['id']}: normalization differs from expectation")
            actual_reasons = sorted(sorted(row["reason_codes"]) for row in quarantine)
            expected_reasons = sorted(sorted(reasons) for reasons in case.get("quarantine", []))
            require(actual_reasons == expected_reasons, f"{case['id']}: reasons {actual_reasons} != {expected_reasons}")
            # Quarantine must preserve each rejected source field, including nulls.
            for row in quarantine:
                original = json.loads(row["raw_record"])
                require(any(all(source[field] == value for field, value in original.items()) for source in raw),
                        f"{case['id']}: quarantine changed the original record")
                require(set(original) == set(document["defaults"][entity.name]) | {entity.key},
                        f"{case['id']}: raw_record lost fields")
            covered_valid += len(accepted)
            covered_rejected += len(quarantine)
            observations.append({"id": case["id"], "entity": entity.name,
                                 "description": case["description"], "before": raw,
                                 "after": accepted, "quarantine": quarantine, "passed": True})
        require((covered_valid, covered_rejected) == (len(valid), len(rejected)),
                f"{entity.name}: unaccounted outcomes")
        reports.append(report)
        print(f"PASS {entity.name}: {len(before)} raw -> {len(valid)} Silver, "
              f"{len(rejected)} quarantine, {report['exact_duplicate_rows']} exact copies", flush=True)

    gold = build_gold(spark, silver["encounters"], silver["lab_results"], silver["bed_events"],
                      as_of="2026-01-02T12:00:00Z", capacities={name: 10 for name in CANONICAL_DEPARTMENTS})
    gold_observed = {}
    for name, frame in gold.items():
        path = output / "gold" / name
        frame.coalesce(1).write.parquet(str(path))
        gold_observed[name] = records(spark.read.parquet(str(path)))
    labs = gold_observed["lab_daily_metrics"]
    require(len(labs) == 1 and labs[0]["test_name"] == "Glucose", "Gold split equivalent lab names")
    require((labs[0]["total_tests"], labs[0]["abnormal_tests"], labs[0]["abnormal_rate"]) == (4, 1, 0.25),
            "Gold lab metrics are incorrect")
    hourly = gold_observed["hospital_hourly_metrics"]
    require(sum(row["admissions"] for row in hourly) == 2, "Gold counted duplicate/quarantined admissions")
    require(max(hourly, key=lambda row: row["hour"])["current_patients"] == 1,
            "Gold lost the ongoing encounter")

    summary = {"passed": True, "scope": "local Spark + Parquet; actual batch transforms; no live Iceberg writes",
               "spark_version": spark.version, "scenario_count": len(observations),
               "entities": reports, "scenarios": observations, "gold": gold_observed}
    (output / "report.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    lines = ["# Kết quả demo dữ liệu bẩn HospitalLake", "", "Dữ liệu giả lập; chạy thật bằng Spark và hàm Silver/Gold của dự án.",
             "Không ghi vào Iceberg/MinIO đang dùng. Timestamp trong báo cáo là UTC.", "",
             f"**{len(observations)}/{len(observations)} kịch bản PASS.** Spark {spark.version}. Chạy lại cho kết quả giống nhau.", "",
             "| Bảng | Đầu vào | Silver | Cách ly | Bản sao gộp |", "|---|---:|---:|---:|---:|"]
    for report in reports:
        lines.append(f"| {report['entity']} | {report['bronze_rows']} | {report['output_rows']} | {report['quarantine_rows']} | {report['exact_duplicate_rows']} |")
    lines += ["", "Gold: 2 lượt nhập viện; 1 bệnh nhân còn nằm viện tại thời điểm chốt. "
              "Glucose: 4 kết quả hợp lệ, 1 bất thường (25%).", "",
              "Bảng lỗi giữ bản ghi gốc và mã lý do; chi tiết đầy đủ trong `report.json`.", ""]
    for case in observations:
        lines += [f"## {case['id']}: {case['description']}", "", "Trước:", "```json",
                  json.dumps([{k: v for k, v in row.items() if not k.startswith('_')} for row in case["before"]], ensure_ascii=False, indent=2),
                  "```", "", "Sau:", "```json",
                  json.dumps(case["after"] or [{"record_key": row["record_key"], "reason_codes": row["reason_codes"]}
                                               for row in case["quarantine"]], ensure_ascii=False, indent=2), "```", ""]
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"PASS {len(observations)} scenarios + Gold + reruns. Report: {output / 'report.md'}", flush=True)
    return output, summary


def main(argv=None):
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("results/data-quality"))
    args = parser.parse_args(argv)
    # This runner is deliberately local; use the active virtualenv for workers.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = (SparkSession.builder.master("local[2]").appName("hospital-data-quality-demo")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.ansi.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run_demo(spark, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
