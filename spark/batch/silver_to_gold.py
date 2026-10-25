"""Full historical recomputation of four Gold tables from validated Silver data."""

import argparse
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path


LOG = logging.getLogger(__name__)
TABLES = (
    "hospital_hourly_metrics",
    "department_daily_metrics",
    "lab_daily_metrics",
    "icu_occupancy",
)
DDL = Path(__file__).resolve().parents[2] / "sql" / "iceberg" / "create_gold_tables.sql"


def parse_as_of(value):
    """Return a UTC timestamp for a reproducible historical cutoff."""
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("--as-of must include a UTC offset")
    return instant.astimezone(timezone.utc).replace(tzinfo=None)


def parse_capacities(value):
    """Parse department=bed-count pairs; no capacity is inferred from events."""
    result = {}
    for item in value.split(","):
        if "=" not in item:
            raise ValueError("capacity must be DEPARTMENT=positive_integer")
        department, raw = (part.strip() for part in item.split("=", 1))
        if not re.fullmatch(r"[A-Z_]+", department) or not raw.isdecimal() or int(raw) < 1:
            raise ValueError("capacity must be DEPARTMENT=positive_integer")
        if department in result:
            raise ValueError(f"duplicate capacity for {department}")
        result[department] = int(raw)
    if "ICU" not in result:
        raise ValueError("ICU capacity is required")
    return result


def _required(frame, name, columns):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"silver.{name} missing columns: {', '.join(missing)}")


def build_gold(spark, encounters, labs, bed_events, *, as_of, capacities):
    """Return Gold DataFrames; all hourly states are at the end of each UTC hour.

    ``as_of`` bounds open encounters and guarantees the same input yields the
    same output on a retry. Capacity is supplied independently of event counts.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cutoff = parse_as_of(as_of)
    if "ICU" not in capacities or any(value < 1 for value in capacities.values()):
        raise ValueError("positive ICU and department capacity values are required")
    _required(encounters, "encounters", ("encounter_id", "patient_id", "department",
                                        "admission_time", "discharge_time"))
    _required(labs, "lab_results", ("lab_result_id", "test_name", "is_abnormal", "measured_at"))
    _required(bed_events, "bed_events", ("event_id", "event_type", "event_time"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    end = F.to_timestamp(F.lit(cutoff.strftime("%Y-%m-%d %H:%M:%S")))
    if encounters.where(F.col("encounter_id").isNull() | F.col("patient_id").isNull()
                        | F.col("department").isNull() | F.col("admission_time").isNull()
                        | (F.col("discharge_time") < F.col("admission_time"))).limit(1).count():
        raise ValueError("invalid Silver encounter")
    if (encounters.groupBy("encounter_id").count().where("count > 1").limit(1).count()):
        raise ValueError("duplicate Silver encounter_id")
    observed_departments = {row.department for row in encounters.select("department").distinct().collect()}
    if observed_departments - capacities.keys():
        raise ValueError("missing capacity for " + ", ".join(sorted(observed_departments - capacities.keys())))
    encounters = encounters.where(F.col("admission_time") <= end)
    labs = labs.where(F.col("measured_at") <= end)
    bed_events = bed_events.where(F.col("event_time") <= end)

    admissions = (encounters.withColumn("hour", F.date_trunc("hour", "admission_time"))
                  .groupBy("hour").agg(F.count("*").alias("admissions"),
                                       F.sum(F.when(F.col("department") == "ICU", 1).otherwise(0))
                                       .alias("icu_admissions")))
    discharges = (encounters.where(F.col("discharge_time") <= end)
                  .withColumn("hour", F.date_trunc("hour", "discharge_time"))
                  .groupBy("hour").agg(F.count("*").alias("discharges"),
                                       F.sum(F.when(F.col("department") == "ICU", 1).otherwise(0))
                                       .alias("icu_discharges")))
    transfers = (bed_events.where(F.lower("event_type") == "transferred")
                 .withColumn("hour", F.date_trunc("hour", "event_time"))
                 .groupBy("hour").agg(F.count("*").alias("transfers")))
    abnormal = (labs.where(F.col("is_abnormal") == True)
                .withColumn("hour", F.date_trunc("hour", "measured_at"))
                .groupBy("hour").agg(F.count("*").alias("abnormal_lab_results")))

    first_hour = (encounters.select(F.min("admission_time").alias("first"))
                  .first()["first"])
    if first_hour is None:
        raise ValueError("no Silver encounters at or before --as-of")
    hours = spark.range(1).select(F.explode(F.sequence(
        F.date_trunc("hour", F.lit(first_hour)), F.date_trunc("hour", end),
        F.expr("INTERVAL 1 HOUR"))).alias("hour"))
    hourly = hours
    for frame in (admissions, discharges, transfers, abnormal):
        hourly = hourly.join(frame, "hour", "left")
    hourly = hourly.fillna(0, subset=("admissions", "icu_admissions", "discharges",
                                      "icu_discharges", "transfers", "abnormal_lab_results"))
    window = Window.orderBy("hour").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    hourly = (hourly.withColumn("current_patients",
                                F.sum(F.col("admissions") - F.col("discharges")).over(window))
              .withColumn("active_icu_patients",
                          F.sum(F.col("icu_admissions") - F.col("icu_discharges")).over(window))
              .select("hour", "current_patients", "admissions", "discharges", "transfers",
                      "active_icu_patients", "abnormal_lab_results"))
    icu_total = capacities["ICU"]
    icu = hourly.select(F.col("hour").alias("timestamp"),
                        F.col("active_icu_patients").alias("occupied_beds"),
                        F.lit(icu_total).cast("long").alias("total_beds"),
                        (F.lit(icu_total) - F.col("active_icu_patients")).cast("long")
                        .alias("available_beds"),
                        (F.col("active_icu_patients") / F.lit(float(icu_total)))
                        .alias("occupancy_rate"))

    active = (encounters.withColumn("effective_end",
                                    F.least(F.coalesce("discharge_time", end), end))
              .where(F.col("effective_end") > F.col("admission_time"))
              .withColumn("date", F.explode(F.sequence(F.to_date("admission_time"),
                                                       F.to_date("effective_end"))))
              .withColumn("occupied_seconds", F.greatest(F.lit(0),
                          F.least(F.unix_timestamp("effective_end"),
                                  F.unix_timestamp(F.date_add("date", 1)))
                          - F.greatest(F.unix_timestamp("admission_time"),
                                       F.unix_timestamp("date"))))
              .where(F.col("occupied_seconds") > 0))
    occupied = (active.groupBy("date", "department")
                .agg(F.countDistinct("patient_id").alias("unique_patients"),
                     (F.sum("occupied_seconds") / 3600.0).alias("occupied_hours")))
    daily_admit = (encounters.withColumn("date", F.to_date("admission_time"))
                   .groupBy("date", "department").agg(F.count("*").alias("admissions")))
    daily_discharge = (encounters.where(F.col("discharge_time") <= end)
                       .withColumn("date", F.to_date("discharge_time"))
                       .groupBy("date", "department")
                       .agg(F.count("*").alias("discharges"),
                            F.avg((F.unix_timestamp("discharge_time")
                                   - F.unix_timestamp("admission_time")) / 3600.0)
                            .alias("avg_length_of_stay_hours")))
    keys = (occupied.select("date", "department")
            .unionByName(daily_admit.select("date", "department"))
            .unionByName(daily_discharge.select("date", "department")).distinct())
    capacity_frame = spark.createDataFrame(
        [(department, count) for department, count in capacities.items()],
        "department string, capacity long")
    daily = (keys.join(occupied, ["date", "department"], "left")
             .join(daily_admit, ["date", "department"], "left")
             .join(daily_discharge, ["date", "department"], "left")
             .join(capacity_frame, "department")
             .select("date", "department", F.coalesce("admissions", F.lit(0)).alias("admissions"),
                     F.coalesce("discharges", F.lit(0)).alias("discharges"),
                     F.coalesce("unique_patients", F.lit(0)).alias("unique_patients"),
                     "avg_length_of_stay_hours",
                     (F.coalesce("occupied_hours", F.lit(0.0)) / (F.col("capacity") * 24.0))
                     .alias("bed_utilization")))
    lab_daily = (labs.withColumn("date", F.to_date("measured_at"))
                 .groupBy("date", "test_name")
                 .agg(F.count("*").alias("total_tests"),
                      F.sum(F.when(F.col("is_abnormal") == True, 1).otherwise(0))
                      .alias("abnormal_tests"))
                 .withColumn("abnormal_rate", F.col("abnormal_tests") / F.col("total_tests"))
                 .select("date", "test_name", "total_tests", "abnormal_tests", "abnormal_rate"))
    return {
        "hospital_hourly_metrics": hourly,
        "department_daily_metrics": daily,
        "lab_daily_metrics": lab_daily,
        "icu_occupancy": icu,
    }


def main(argv=None):
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from spark.common.config import SparkConfig
    from spark.common.iceberg import create_spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--as-of", default=os.environ.get("GOLD_AS_OF"), required=False)
    parser.add_argument("--bed-capacities", default=os.environ.get("GOLD_BED_CAPACITIES"))
    args = parser.parse_args(argv)
    if not args.as_of or not args.bed_capacities:
        parser.error("GOLD_AS_OF and GOLD_BED_CAPACITIES are required")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog):
        parser.error("--catalog must be a simple SQL identifier")
    capacities = parse_capacities(args.bed_capacities)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    started = time.monotonic()
    if SparkConf().contains(f"spark.sql.catalog.{args.catalog}"):
        # scripts/run_gold.py (`make gold`) passes the catalog as --conf flags.
        spark = SparkSession.builder.appName("hospital-lake-gold").getOrCreate()
    else:
        # Compose `gold` service: same env-driven session as the Bronze/Silver jobs.
        spark = create_spark_session("hospital-lake-gold", SparkConfig.from_env())
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        silver = {name: spark.table(f"{args.catalog}.silver.{name}")
                  for name in ("encounters", "lab_results", "bed_events")}
        inputs = {name: frame.count() for name, frame in silver.items()}
        gold = build_gold(spark, silver["encounters"], silver["lab_results"],
                          silver["bed_events"], as_of=args.as_of, capacities=capacities)
        ddl = DDL.read_text(encoding="utf-8").replace("iceberg.gold.", f"{args.catalog}.gold.")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.gold")
        for statement in ddl.split(";"):
            if statement.strip():
                spark.sql(statement.strip())
        for name in TABLES:
            rows = gold[name].count()
            gold[name].writeTo(f"{args.catalog}.gold.{name}").overwrite(F.lit(True))
            LOG.info("job=silver_to_gold table=%s output_rows=%d as_of=%s", name, rows, args.as_of)
        LOG.info("job=silver_to_gold input_rows=%s invalid_rows=0 duplicate_rows=0 duration_seconds=%.2f",
                 inputs, time.monotonic() - started)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
