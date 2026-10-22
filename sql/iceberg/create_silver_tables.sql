-- Spark SQL, Iceberg format v2. Silver is fully recomputed from Bronze by
-- spark/batch/bronze_to_silver.py; every table carries lineage columns
-- _bronze_batch_id (earliest Bronze batch containing the record) and
-- _processed_at (Silver run time).
CREATE NAMESPACE IF NOT EXISTS iceberg.silver;

CREATE TABLE IF NOT EXISTS iceberg.silver.patients (
    patient_id STRING,
    gender STRING,
    birth_year INT,
    city STRING,
    created_at TIMESTAMP_LTZ,
    _bronze_batch_id STRING,
    _processed_at TIMESTAMP_LTZ
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

-- Monthly partitions: ~20 months of demo history; daily partitions would
-- produce hundreds of tiny files at development scale (AGENTS.md §46).
CREATE TABLE IF NOT EXISTS iceberg.silver.encounters (
    encounter_id STRING,
    patient_id STRING,
    encounter_type STRING,
    department STRING,
    admission_time TIMESTAMP_LTZ,
    discharge_time TIMESTAMP_LTZ,
    status STRING,
    length_of_stay_hours DOUBLE,
    _bronze_batch_id STRING,
    _processed_at TIMESTAMP_LTZ
) USING iceberg
PARTITIONED BY (months(admission_time))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

-- Rejected Bronze records for every Silver entity, with traceable reasons.
CREATE TABLE IF NOT EXISTS iceberg.silver.quarantine (
    entity STRING,
    record_key STRING,
    reason_codes ARRAY<STRING>,
    raw_record STRING,
    _bronze_batch_id STRING,
    _processed_at TIMESTAMP_LTZ
) USING iceberg
PARTITIONED BY (entity)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');
