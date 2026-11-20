CREATE DATABASE IF NOT EXISTS hospital;

-- All tables contain Spark aggregates, not source events. A publication marker
-- hides a partially written snapshot. FINAL handles retry duplicates immediately.
CREATE TABLE IF NOT EXISTS hospital.hospital_snapshots (
    snapshot_version UInt64, event_time DateTime64(6, 'UTC'),
    current_patients UInt64, icu_occupancy UInt64, total_icu_beds UInt32,
    processed_events UInt64, invalid_events UInt64, batch_duration_seconds Float64
) ENGINE = ReplacingMergeTree ORDER BY snapshot_version;

ALTER TABLE hospital.hospital_snapshots ADD COLUMN IF NOT EXISTS latency_p50_seconds Nullable(Float64);
ALTER TABLE hospital.hospital_snapshots ADD COLUMN IF NOT EXISTS latency_p95_seconds Nullable(Float64);

CREATE TABLE IF NOT EXISTS hospital.department_snapshots (
    snapshot_version UInt64, window_start DateTime64(6, 'UTC'), department LowCardinality(String),
    admissions_5m UInt64, discharges_5m UInt64
) ENGINE = ReplacingMergeTree ORDER BY (snapshot_version, window_start, department);

CREATE TABLE IF NOT EXISTS hospital.lab_snapshots (
    snapshot_version UInt64, window_start DateTime64(6, 'UTC'), abnormal_labs UInt64
) ENGINE = ReplacingMergeTree ORDER BY (snapshot_version, window_start);

CREATE TABLE IF NOT EXISTS hospital.published_snapshots (
    snapshot_version UInt64, published_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree ORDER BY snapshot_version;

CREATE OR REPLACE VIEW hospital.realtime_department_metrics AS
SELECT (SELECT argMax(published_at, snapshot_version) FROM hospital.published_snapshots) AS window_start, department,
       sum(admissions_5m) AS admissions_5m, sum(discharges_5m) AS discharges_5m
FROM hospital.department_snapshots AS d FINAL
WHERE d.snapshot_version = (SELECT max(snapshot_version) FROM hospital.published_snapshots)
  AND d.window_start >= now64(6, 'UTC') - INTERVAL 5 MINUTE AND d.window_start <= now64(6, 'UTC')
GROUP BY department;

CREATE OR REPLACE VIEW hospital.realtime_lab_metrics AS
SELECT window_start, abnormal_labs FROM hospital.lab_snapshots FINAL
WHERE snapshot_version = (SELECT max(snapshot_version) FROM hospital.published_snapshots)
  AND window_start >= now64(6, 'UTC') - INTERVAL 15 MINUTE AND window_start <= now64(6, 'UTC');

CREATE OR REPLACE VIEW hospital.realtime_hospital_state AS
SELECT event_time, current_patients, icu_occupancy, total_icu_beds,
       greatest(toInt64(total_icu_beds)-toInt64(icu_occupancy), 0) AS available_icu_beds,
       icu_occupancy / total_icu_beds AS icu_occupancy_rate,
       (SELECT sum(admissions_5m) FROM hospital.realtime_department_metrics) AS admissions_5m,
       (SELECT sum(discharges_5m) FROM hospital.realtime_department_metrics) AS discharges_5m,
       processed_events, invalid_events, batch_duration_seconds, latency_p50_seconds, latency_p95_seconds
FROM hospital.hospital_snapshots FINAL
WHERE snapshot_version = (SELECT max(snapshot_version) FROM hospital.published_snapshots);

-- Compatibility tables for the TV3 revision-based sink; the live deployment
-- keeps the common published-snapshot views above as its sole serving path.
CREATE DATABASE IF NOT EXISTS hospital;

CREATE TABLE IF NOT EXISTS hospital.hospital_state_versions (
    event_time DateTime64(6, 'UTC'),
    current_patients UInt64,
    icu_occupancy UInt64,
    admissions_5m UInt64,
    discharges_5m UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS hospital.department_metric_versions (
    window_start DateTime64(6, 'UTC'),
    department LowCardinality(String),
    admissions_5m UInt64,
    discharges_5m UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (window_start, department);

CREATE TABLE IF NOT EXISTS hospital.lab_metric_versions (
    window_start DateTime64(6, 'UTC'),
    abnormal_labs UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY window_start;
