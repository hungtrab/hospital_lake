-- 1. Hospital census and activity by hour.
SELECT hour, current_patients, admissions, discharges, transfers,
       active_icu_patients, abnormal_lab_results
FROM iceberg.gold.hospital_hourly_metrics
ORDER BY hour DESC LIMIT 24;

-- 2. Daily department demand and utilization.
SELECT date, department, admissions, discharges, unique_patients,
       avg_length_of_stay_hours, bed_utilization
FROM iceberg.gold.department_daily_metrics
ORDER BY date DESC, department LIMIT 100;

-- 3. ICU capacity trend.
SELECT "timestamp", occupied_beds, total_beds, available_beds, occupancy_rate
FROM iceberg.gold.icu_occupancy
ORDER BY "timestamp" DESC LIMIT 24;

-- 4. Abnormal laboratory results by test and day.
SELECT date, test_name, total_tests, abnormal_tests, abnormal_rate
FROM iceberg.gold.lab_daily_metrics
ORDER BY date DESC, abnormal_rate DESC LIMIT 100;

-- 5. Daily hospital admissions across departments.
SELECT date, sum(admissions) AS admissions, sum(discharges) AS discharges,
       sum(unique_patients) AS department_patient_visits
FROM iceberg.gold.department_daily_metrics
GROUP BY date ORDER BY date DESC LIMIT 30;
