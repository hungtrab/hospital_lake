CREATE VIEW IF NOT EXISTS hospital.realtime_hospital_state AS
SELECT event_time, current_patients, icu_occupancy, admissions_5m, discharges_5m
FROM hospital.hospital_state_versions FINAL;

CREATE VIEW IF NOT EXISTS hospital.realtime_department_metrics AS
SELECT window_start, department, admissions_5m, discharges_5m
FROM hospital.department_metric_versions FINAL;

CREATE VIEW IF NOT EXISTS hospital.realtime_lab_metrics AS
SELECT window_start, abnormal_labs
FROM hospital.lab_metric_versions FINAL;
