CREATE TABLE IF NOT EXISTS iceberg.silver.realtime_events (
    event_id STRING, event_time TIMESTAMP, event_type STRING,
    patient_id STRING, encounter_id STRING, department STRING, is_abnormal BOOLEAN
) USING iceberg PARTITIONED BY (days(event_time))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

-- Latest event-time state, including discharged tombstones to prevent an older
-- admission from reopening a closed encounter. Single streaming writer.
CREATE TABLE IF NOT EXISTS iceberg.silver.realtime_encounters (
    encounter_id STRING, patient_id STRING, department STRING,
    event_time TIMESTAMP, event_id STRING, event_type STRING, event_priority INT,
    seen_admission BOOLEAN
) USING iceberg TBLPROPERTIES ('format-version'='2');

-- Valid envelopes that conflict with an encounter's identity/lifecycle remain
-- traceable by event ID (the original bytes are preserved in Bronze).
CREATE TABLE IF NOT EXISTS iceberg.silver.realtime_rejected (
    event_id STRING, event_time TIMESTAMP, event_type STRING,
    patient_id STRING, encounter_id STRING, department STRING,
    is_abnormal BOOLEAN, error_reason STRING
) USING iceberg PARTITIONED BY (days(event_time))
TBLPROPERTIES ('format-version'='2');
