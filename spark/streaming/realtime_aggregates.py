"""Event-time hospital metrics, replayable from Iceberg; one serving writer.

The raw query preserves every Kafka coordinate. A second query uses Spark's
watermark/event-ID state before updating narrow Silver state and publishing
aggregate snapshots. ClickHouse never receives the event ledger.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spark.common.normalization import DEPARTMENT_ALIASES, department_key, ENCOUNTER_TYPES

LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]


def payload_values(payload, event_type, version):
    """Pure payload contract, also used by distributed Spark workers.

    Returns IDs, normalized department, computed abnormal flag and rejection
    reason. No clinical inference or imputation is performed.
    """
    empty = (None, None, None, False)
    try:
        p = json.loads(payload)
    except (TypeError, ValueError):
        return (*empty, "INVALID_PAYLOAD")
    if not isinstance(p, dict) or version != 1:
        return (*empty, "UNSUPPORTED_VERSION" if version != 1 else "INVALID_PAYLOAD")
    def identifier(key):
        value = p.get(key)
        return value.strip() if isinstance(value, str) and value.strip() else None
    patient, encounter = identifier("patient_id"), identifier("encounter_id")
    if not patient or (event_type != "PATIENT_REGISTERED" and not encounter):
        return (*empty, "MISSING_PAYLOAD_ID")
    dept = DEPARTMENT_ALIASES.get(department_key(str(p.get("department", ""))))
    abnormal = False
    def number(key, low=-math.inf, high=math.inf):
        value = p.get(key)
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and low <= value <= high)
    reason = None
    if event_type in ("ADMISSION", "DISCHARGE", "TRANSFER", "BED_ASSIGNED", "BED_RELEASED") and dept is None:
        reason = "UNKNOWN_DEPARTMENT"
    elif event_type == "ADMISSION" and p.get("encounter_type") not in ENCOUNTER_TYPES:
        reason = "INVALID_ENCOUNTER_TYPE"
    elif event_type == "TRANSFER" and (not identifier("from_bed_id") or not identifier("to_bed_id")):
        reason = "MISSING_BED_ID"
    elif event_type in ("BED_ASSIGNED", "BED_RELEASED") and not identifier("bed_id"):
        reason = "MISSING_BED_ID"
    elif event_type == "LAB_RESULT":
        if not all(number(k) for k in ("result_value", "reference_low", "reference_high")):
            reason = "INVALID_LAB_VALUE"
        elif p["reference_low"] > p["reference_high"]:
            reason = "INVALID_REFERENCE_RANGE"
        elif not all(identifier(k) for k in ("test_code", "test_name", "unit")):
            reason = "MISSING_LAB_FIELD"
        else:
            abnormal = not p["reference_low"] <= p["result_value"] <= p["reference_high"]
    elif event_type == "VITAL_MEASUREMENT":
        bounds = {"heart_rate": (20, 250), "spo2": (0, 100), "systolic_bp": (40, 300),
                  "diastolic_bp": (20, 200), "temperature": (25, 45)}
        if not all(number(k, *limits) for k, limits in bounds.items()):
            reason = "INVALID_VITAL"
        elif p["diastolic_bp"] >= p["systolic_bp"]:
            reason = "INVALID_BLOOD_PRESSURE"
    elif event_type == "MEDICATION_ORDER" and (not number("dose", 0) or p["dose"] == 0
              or not all(identifier(k) for k in ("drug_name", "dose_unit", "route"))):
        reason = "INVALID_MEDICATION"
    return patient, encounter, dept, abnormal, reason


def validate_payload(events):
    from pyspark.sql import functions as F
    schema = "patient_id string, encounter_id string, department string, is_abnormal boolean, reason string"
    parse = F.udf(payload_values, schema)
    return (events.withColumn("_payload", parse("payload", "event_type", "version"))
            .withColumn("validation_error", F.coalesce("validation_error", "_payload.reason",
                F.when(F.col("event_time") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"),
                       F.lit("FUTURE_EVENT_TIME"))))
            .select("*", "_payload.patient_id", "_payload.encounter_id", "_payload.department", "_payload.is_abnormal")
            .drop("_payload"))


def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    for statement in (ROOT / "sql/iceberg/create_realtime_tables.sql").read_text().split(";"):
        if statement.strip():
            spark.sql(statement)


def update_state(spark, events):
    """Replay-safe event ledger and latest encounter state; SQL stays distributed."""
    from pyspark.sql import functions as F, Window
    columns = ["event_id", "event_time", "event_type", "patient_id", "encounter_id", "department", "is_abnormal"]
    events = events.select(*columns)
    # An encounter ID belongs to one patient. A discharge is terminal; a later
    # admission must have a new encounter ID. Preserve conflicting events with
    # reasons instead of letting them change current-state or window metrics.
    existing = spark.table("iceberg.silver.realtime_encounters").select(
        "encounter_id", F.col("patient_id").alias("_owner"),
        F.when(F.col("event_type") == "DISCHARGE", F.col("event_time")).alias("_closed"))
    order = Window.partitionBy("encounter_id").orderBy("event_time", "event_id")
    classified = (events.join(existing, "encounter_id", "left")
        .withColumn("_owner", F.coalesce("_owner", F.first("patient_id").over(order)))
        .withColumn("_closed", F.least("_closed", F.min(F.when(
            (F.col("event_type") == "DISCHARGE") & (F.col("patient_id") == F.col("_owner")),
            F.col("event_time"))).over(Window.partitionBy("encounter_id"))))
        .withColumn("error_reason", F.when(F.col("encounter_id").isNotNull()
            & (F.col("patient_id") != F.col("_owner")), "ENCOUNTER_PATIENT_CONFLICT")
            .when((F.col("event_type").isin("ADMISSION", "TRANSFER"))
                  & (F.col("event_time") >= F.col("_closed")), "EVENT_AFTER_DISCHARGE")))
    classified.where("error_reason IS NOT NULL").select(*columns, "error_reason").createOrReplaceTempView("_realtime_rejected")
    spark.sql("""MERGE INTO iceberg.silver.realtime_rejected t USING _realtime_rejected s
        ON t.event_id=s.event_id AND t.event_time=s.event_time
        WHEN NOT MATCHED THEN INSERT *""")
    events = classified.where("error_reason IS NULL").select(*columns)
    # Collect strings formatted in Spark's UTC session. Collecting TIMESTAMP as
    # Python datetime would convert to the host timezone and corrupt pruning.
    extent = events.agg(F.min("event_time").cast("string"), F.max("event_time").cast("string")).first()
    if extent[0] is None:
        return
    low, high = extent
    events.createOrReplaceTempView("_realtime_batch")
    spark.sql(f"""MERGE INTO iceberg.silver.realtime_events t USING _realtime_batch s
        ON t.event_time BETWEEN TIMESTAMP '{low}' AND TIMESTAMP '{high}'
        AND t.event_id = s.event_id AND t.event_time = s.event_time
        WHEN NOT MATCHED THEN INSERT *""")
    lifecycle = (events.where("event_type IN ('ADMISSION','DISCHARGE','TRANSFER')")
        .withColumn("event_priority", F.when(F.col("event_type") == "DISCHARGE", 3)
                    .when(F.col("event_type") == "TRANSFER", 2).otherwise(1)))
    by_encounter = Window.partitionBy("encounter_id")
    latest = (lifecycle.withColumn("seen_admission", F.max(
                  (F.col("event_type") == "ADMISSION").cast("int")).over(by_encounter).cast("boolean"))
              .withColumn("_rank", F.row_number().over(by_encounter.orderBy(
                  (F.col("event_type") == "DISCHARGE").desc(),
                  F.when(F.col("event_type") == "DISCHARGE", F.col("event_time")).asc_nulls_last(),
                  F.col("event_time").desc(), F.col("event_priority").desc(), F.col("event_id").desc()))))
    latest.where("_rank = 1").drop("_rank", "is_abnormal").createOrReplaceTempView("_encounter_batch")
    newer = "struct(s.event_time,s.event_priority,s.event_id) > struct(t.event_time,t.event_priority,t.event_id)"
    spark.sql(f"""MERGE INTO iceberg.silver.realtime_encounters t USING _encounter_batch s
        ON t.encounter_id = s.encounter_id
        WHEN MATCHED AND t.event_type='DISCHARGE' AND s.event_type='DISCHARGE'
          AND s.event_time < t.event_time THEN UPDATE SET
          t.event_time=s.event_time, t.event_id=s.event_id, t.department=s.department,
          t.seen_admission=(t.seen_admission OR s.seen_admission)
        WHEN MATCHED AND (t.event_type <> 'DISCHARGE' AND (s.event_type='DISCHARGE' OR {newer})) THEN UPDATE SET
          t.patient_id=s.patient_id, t.department=s.department, t.event_time=s.event_time,
          t.event_id=s.event_id, t.event_type=s.event_type, t.event_priority=s.event_priority,
          t.seen_admission=(t.seen_admission OR s.seen_admission)
        WHEN MATCHED AND s.seen_admission AND NOT t.seen_admission
          THEN UPDATE SET t.seen_admission=true
        WHEN NOT MATCHED THEN INSERT *""")


def publish(spark, client, *, processed=0, duration=0.0, as_of=None, arrival_percentiles=None):
    """Publish only aggregates, then atomically expose their common version.

    Per-second buckets let ClickHouse expire 5/15-minute windows during idle
    periods. The as_of argument is for deterministic integration fixtures.
    """
    from pyspark.sql import functions as F
    now = as_of or datetime.now(timezone.utc)
    version = time.time_ns()
    active = spark.table("iceberg.silver.realtime_encounters").where("seen_admission AND event_type <> 'DISCHARGE'")
    state = active.agg(F.countDistinct("patient_id").alias("current_patients"),
                      F.countDistinct(F.when(F.col("department") == "ICU", F.col("patient_id"))).alias("icu_occupancy")).first().asDict()
    capacity = int(os.environ.get("REALTIME_ICU_BEDS", "20"))
    if capacity <= 0:
        raise ValueError("REALTIME_ICU_BEDS must be positive")
    state.update(snapshot_version=version, event_time=now, total_icu_beds=capacity,
                 processed_events=processed, invalid_events=0, batch_duration_seconds=duration,
                 latency_p50_seconds=None, latency_p95_seconds=None)
    if spark.catalog.tableExists('iceberg.bronze.stream_events_quarantine'):
        state['invalid_events'] = spark.table('iceberg.bronze.stream_events_quarantine').count()
    recent = spark.table("iceberg.silver.realtime_events").where(
        (F.col("event_time") >= F.lit(now - timedelta(minutes=15))) & (F.col("event_time") <= F.lit(now)))
    department = (recent.where("event_type IN ('ADMISSION','DISCHARGE')")
        .groupBy(F.date_trunc("second", "event_time").alias("window_start"), "department")
        .agg(F.sum((F.col("event_type") == "ADMISSION").cast("long")).alias("admissions_5m"),
             F.sum((F.col("event_type") == "DISCHARGE").cast("long")).alias("discharges_5m")))
    labs = (recent.where("event_type = 'LAB_RESULT' AND is_abnormal")
        .groupBy(F.date_trunc("second", "event_time").alias("window_start"))
        .agg(F.count("*").alias("abnormal_labs")))
    def rows(frame):
        frame = frame.withColumn('window_start', F.date_format('window_start', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
        for row in frame.toLocalIterator():
            yield dict(row.asDict(), snapshot_version=version)
    client.insert("department_snapshots", rows(department))
    client.insert("lab_snapshots", rows(labs))
    if arrival_percentiles:
        published = time.time()
        state['latency_p50_seconds'] = max(0, published-arrival_percentiles[1]/1000)
        state['latency_p95_seconds'] = max(0, published-arrival_percentiles[0]/1000)
    client.insert("hospital_snapshots", [state])
    client.insert("published_snapshots", [{"snapshot_version": version, "published_at": now}])
    return state


def write_microbatch(frame, batch_id, *, client):
    from pyspark.sql import functions as F
    start = time.monotonic()
    frame.persist()
    try:
        count = frame.count()
        if not count:
            return
        update_state(frame.sparkSession, frame)
        arrival = frame.agg(F.expr('percentile_approx(unix_millis(timestamp), array(0.05D, 0.5D), 10000)')).first()[0]
        result = publish(frame.sparkSession, client, processed=count, duration=time.monotonic()-start,
                         arrival_percentiles=arrival)
        LOG.info("job=realtime batch_id=%s rows=%s current_patients=%s duration_seconds=%.3f",
                 batch_id, count, result["current_patients"], time.monotonic()-start)
    finally:
        frame.unpersist()


def main():
    import pyspark
    from spark.common.iceberg import create_spark_session
    from spark.common.clickhouse import ClickHouse
    from spark.common.stream_metrics import StreamMetrics
    from spark.streaming.kafka_reader import read_kafka_events, parse_kafka_events, deduplicate_events, kafka_connector_package
    from spark.streaming.bronze_sink import start_stream
    checkpoint = os.environ.get("REALTIME_CHECKPOINT_LOCATION", "")
    if not checkpoint:
        raise ValueError("REALTIME_CHECKPOINT_LOCATION must persist across restarts")
    client = ClickHouse()
    spark = create_spark_session("HospitalRealtime", extra_packages=(kafka_connector_package(pyspark.__version__),))
    spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("STREAM_SHUFFLE_PARTITIONS", "4"))
    ensure_tables(spark)
    events = validate_payload(parse_kafka_events(read_kafka_events(spark,
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topics=os.environ.get("KAFKA_TOPICS", "hospital.events"),
        starting_offsets=os.environ.get("KAFKA_STARTING_OFFSETS", "earliest"))))
    metrics = StreamMetrics(spark, os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                            os.environ.get('KAFKA_TOPICS', 'hospital.events'),
                            int(os.environ.get('STREAM_METRICS_PORT', '8090')))
    try:
        start_stream(events, checkpoint_location=checkpoint + "/bronze")
        (deduplicate_events(events).writeStream
            .queryName('hospital_serving')
            .foreachBatch(lambda frame, batch_id: write_microbatch(frame, batch_id, client=client))
            .option("checkpointLocation", checkpoint + "/serving")
            .trigger(processingTime=os.environ.get("STREAM_TRIGGER", "10 seconds")).start())
        while not spark.streams.awaitAnyTermination(5):
            metrics.refresh(spark)
    finally:
        metrics.close()
        for query in spark.streams.active:
            query.stop()
        spark.stop()


from spark.common.normalization import canonical_department


def prepare_metric_events(events):
    """Validate metric payloads, preserving Kafka bytes and traceable reasons.

    Other supported event types remain valid (and advance the ingestion
    watermark) but do not contribute to these metrics. Lab flags are recomputed.
    """
    from pyspark.sql import functions as F

    required = {"event_id", "event_type", "event_time", "payload", "validation_error", "version"}
    if required - set(events.columns):
        raise ValueError("metric events missing: " + ", ".join(sorted(required - set(events.columns))))
    frame = events
    for name in ("patient_id", "encounter_id", "department", "result_value", "reference_low", "reference_high"):
        frame = frame.withColumn("_metric_" + name, F.get_json_object("payload", "$." + name))
    frame = frame.withColumn("department", canonical_department(F.col("_metric_department")))
    for name in ("result_value", "reference_low", "reference_high"):
        frame = frame.withColumn("_metric_" + name, F.expr("try_cast(_metric_" + name + " AS DOUBLE)"))

    journey = F.col("event_type").isin("ADMISSION", "DISCHARGE")
    lab = F.col("event_type") == "LAB_RESULT"
    relevant = journey | lab
    reasons = [F.col("validation_error"),
               F.when(F.col("version") != 1, "UNSUPPORTED_METRIC_VERSION")]
    for name in ("patient_id", "encounter_id"):
        value = F.col("_metric_" + name)
        reasons.append(F.when(relevant & (value.isNull() | (F.trim(value) == "")), "MISSING_" + name.upper()))
    reasons.append(F.when(journey & F.col("department").isNull(), "UNKNOWN_DEPARTMENT"))
    for name in ("result_value", "reference_low", "reference_high"):
        value = F.col("_metric_" + name)
        finite = value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))
        reasons.append(F.when(lab & ~finite, "INVALID_" + name.upper()))
    low, high, value = (F.col("_metric_" + name) for name in ("reference_low", "reference_high", "result_value"))
    reasons.append(F.when(lab & (low > high), "INVALID_REFERENCE_RANGE"))
    error = F.concat_ws("|", *reasons)
    return (frame.withColumn("validation_error", F.when(error != "", error))
            .withColumn("encounter_id", F.col("_metric_encounter_id"))
            .withColumn("is_abnormal", F.when(lab, (value < low) | (value > high)))
            .drop(*[name for name in frame.columns if name.startswith("_metric_")]))


def _watermark(events, delay):
    # Dedup runs in its own query. These functions contain one stateful operator.
    return events.withWatermark("event_time", delay) if events.isStreaming else events


def department_metrics(events, *, watermark_delay="10 minutes"):
    """Five-minute event-time windows sliding by one minute, [start, end).

window_start names the start, as in the TV3 serving contract. Updates are
provisional until the watermark passes window_end; never sum adjacent windows.
"""
    from pyspark.sql import functions as F

    frame = _watermark(events, watermark_delay).where("event_type IN ('ADMISSION', 'DISCHARGE')")
    return (frame.groupBy(F.window("event_time", "5 minutes", "1 minute"), "department")
            .agg(F.sum(F.when(F.col("event_type") == "ADMISSION", 1).otherwise(0)).alias("admissions_5m"),
                 F.sum(F.when(F.col("event_type") == "DISCHARGE", 1).otherwise(0)).alias("discharges_5m"))
            .select(F.col("window.start").alias("window_start"), "department", "admissions_5m", "discharges_5m"))


def lab_metrics(events, *, watermark_delay="10 minutes", rolling=False):
    """Non-overlapping minute buckets for ClickHouse, or explicit 15m windows.

Only minute buckets belong in realtime_lab_metrics; summing overlapping 15m
windows would inflate Grafana's last-15-minute panel.
"""
    from pyspark.sql import functions as F

    frame = _watermark(events, watermark_delay).where("event_type = 'LAB_RESULT'")
    duration = "15 minutes" if rolling else "1 minute"
    count = "abnormal_labs_last_15m" if rolling else "abnormal_labs"
    return (frame.groupBy(F.window("event_time", duration, "1 minute"))
            .agg(F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias(count))
            .select(F.col("window.start").alias("window_start"), F.col(count)))


def icu_encounter_state(events):
    """Latest ICU admission/discharge per encounter, ordered by event time.

The simulator's TRANSFER changes beds within the same department. Department
transfers are not modeled by its current payload contract. A discharge wins an
equal-time admission; older/out-of-order events never resurrect an encounter.
Complete mode retains one register per ICU encounter for the query lifetime.
"""
    from pyspark.sql import functions as F

    frame = events.where("validation_error IS NULL AND department = 'ICU' AND event_type IN ('ADMISSION', 'DISCHARGE')")
    active = F.col("event_type") == "ADMISSION"
    return (frame.groupBy("encounter_id")
            .agg(F.max(F.struct("event_time", F.when(active, 0).otherwise(1).alias("closed"),
                                "event_id")).alias("latest")))


def icu_snapshot(encounters):
    """Reduce a complete register snapshot to the ICU fragment for TV1.

Never write this fragment into realtime_hospital_state: TV1 supplies its other
columns and owns the single combined hospital-state writer.
"""
    from pyspark.sql import functions as F

    return encounters.agg(F.max("latest.event_time").alias("event_time"),
                          F.coalesce(F.sum(F.when(F.col("latest.closed") == 0, 1).otherwise(0)),
                                     F.lit(0)).alias("icu_occupancy"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
