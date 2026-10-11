-- Spark SQL, Iceberg format v2. Source fields stay nullable in raw Bronze.
-- Required ingestion metadata is supplied by TV3's ingestion job.
CREATE NAMESPACE IF NOT EXISTS iceberg.bronze;

CREATE TABLE IF NOT EXISTS iceberg.bronze.patients_raw (
    patient_id STRING,
    gender STRING,
    birth_year INT,
    city STRING,
    created_at TIMESTAMP_LTZ,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');

CREATE TABLE IF NOT EXISTS iceberg.bronze.encounters_raw (
    encounter_id STRING,
    patient_id STRING,
    encounter_type STRING,
    department STRING,
    admission_time TIMESTAMP_LTZ,
    discharge_time TIMESTAMP_LTZ,
    status STRING,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
PARTITIONED BY (days(admission_time))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');

CREATE TABLE IF NOT EXISTS iceberg.bronze.lab_results_raw (
    lab_result_id STRING,
    patient_id STRING,
    encounter_id STRING,
    test_code STRING,
    test_name STRING,
    result_value DOUBLE,
    unit STRING,
    reference_low DOUBLE,
    reference_high DOUBLE,
    is_abnormal BOOLEAN,
    measured_at TIMESTAMP_LTZ,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
PARTITIONED BY (days(measured_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');

CREATE TABLE IF NOT EXISTS iceberg.bronze.medications_raw (
    medication_id STRING,
    patient_id STRING,
    encounter_id STRING,
    drug_name STRING,
    dose DOUBLE,
    dose_unit STRING,
    route STRING,
    start_time TIMESTAMP_LTZ,
    end_time TIMESTAMP_LTZ,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');

CREATE TABLE IF NOT EXISTS iceberg.bronze.vitals_raw (
    measurement_id STRING,
    patient_id STRING,
    encounter_id STRING,
    heart_rate INT,
    spo2 DOUBLE,
    systolic_bp INT,
    diastolic_bp INT,
    temperature DOUBLE,
    measured_at TIMESTAMP_LTZ,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
PARTITIONED BY (days(measured_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');

CREATE TABLE IF NOT EXISTS iceberg.bronze.bed_events_raw (
    event_id STRING,
    patient_id STRING,
    encounter_id STRING,
    department STRING,
    bed_id STRING,
    event_type STRING,
    event_time TIMESTAMP_LTZ,
    _source STRING NOT NULL,
    _ingested_at TIMESTAMP_LTZ NOT NULL,
    _batch_id STRING NOT NULL,
    _event_id STRING,
    _schema_version INT NOT NULL
) USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.target-file-size-bytes'='134217728');
