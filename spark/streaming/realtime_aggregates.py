"""TV2 event-time department/lab metrics and current ICU census.

Window functions consume the durable, deduplicated event stream. ICU instead
uses all validated events: a latest-event register per encounter is idempotent
and must not lose an old admission simply because the window watermark moved.
"""
from __future__ import annotations

from spark.common.normalization import canonical_department


def prepare_metric_events(events):
    """Validate metric payloads, preserving Kafka bytes and traceable reasons.

Other supported event types remain valid (and advance the ingestion watermark)
but do not contribute to these three metrics. Lab flags are recomputed.
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
