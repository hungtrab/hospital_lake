CREATE DATABASE IF NOT EXISTS hospital;

CREATE TABLE IF NOT EXISTS hospital.hospital_state_versions (
    event_time DateTime64(3, 'UTC'),
    current_patients UInt64,
    icu_occupancy UInt64,
    admissions_5m UInt64,
    discharges_5m UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS hospital.department_metric_versions (
    window_start DateTime64(3, 'UTC'),
    department LowCardinality(String),
    admissions_5m UInt64,
    discharges_5m UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (window_start, department);

CREATE TABLE IF NOT EXISTS hospital.lab_metric_versions (
    window_start DateTime64(3, 'UTC'),
    abnormal_labs UInt64,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY window_start;
