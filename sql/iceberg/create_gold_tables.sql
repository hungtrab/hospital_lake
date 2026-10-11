CREATE TABLE IF NOT EXISTS iceberg.gold.hospital_hourly_metrics (
    hour TIMESTAMP,
    current_patients BIGINT,
    admissions BIGINT,
    discharges BIGINT,
    transfers BIGINT,
    active_icu_patients BIGINT,
    abnormal_lab_results BIGINT
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.gold.department_daily_metrics (
    date DATE,
    department STRING,
    admissions BIGINT,
    discharges BIGINT,
    unique_patients BIGINT,
    avg_length_of_stay_hours DOUBLE,
    bed_utilization DOUBLE
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.gold.lab_daily_metrics (
    date DATE,
    test_name STRING,
    total_tests BIGINT,
    abnormal_tests BIGINT,
    abnormal_rate DOUBLE
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.gold.icu_occupancy (
    timestamp TIMESTAMP,
    occupied_beds BIGINT,
    total_beds BIGINT,
    available_beds BIGINT,
    occupancy_rate DOUBLE
) USING iceberg;
