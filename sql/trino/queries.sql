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

-- 6. Admissions per day (Silver encounters, last 14 days of data).
SELECT date(admission_time) AS day, count(*) AS admissions,
       count_if(encounter_type = 'icu') AS icu_admissions
FROM iceberg.silver.encounters
GROUP BY date(admission_time) ORDER BY day DESC LIMIT 14;

-- 7. Average length of stay by department (discharged Silver encounters).
SELECT department, count(*) AS discharged_encounters,
       round(avg(length_of_stay_hours), 1) AS avg_length_of_stay_hours,
       round(approx_percentile(length_of_stay_hours, 0.5), 1) AS median_length_of_stay_hours
FROM iceberg.silver.encounters
WHERE status = 'discharged'
GROUP BY department ORDER BY avg_length_of_stay_hours DESC;

-- 8. Abnormal lab rate by test (all Silver lab results).
SELECT test_name, count(*) AS total_tests, count_if(is_abnormal) AS abnormal_tests,
       round(avg(IF(is_abnormal, 1.0, 0.0)), 3) AS abnormal_rate
FROM iceberg.silver.lab_results
GROUP BY test_name ORDER BY abnormal_rate DESC;

-- 9. ICU utilization by month (Gold icu_occupancy).
SELECT date_trunc('month', "timestamp") AS month,
       round(avg(occupancy_rate), 3) AS avg_occupancy_rate,
       max(occupied_beds) AS peak_occupied_beds, max(total_beds) AS total_beds
FROM iceberg.gold.icu_occupancy
GROUP BY date_trunc('month', "timestamp") ORDER BY month DESC LIMIT 12;

-- 10. Patient history: encounters and lab results of the most frequent patient.
WITH frequent AS (
    SELECT patient_id FROM iceberg.silver.encounters
    GROUP BY patient_id ORDER BY count(*) DESC, patient_id LIMIT 1
)
SELECT e.patient_id, e.encounter_id, e.encounter_type, e.department,
       e.admission_time, e.discharge_time,
       count(l.lab_result_id) AS lab_results, count_if(l.is_abnormal) AS abnormal_labs
FROM iceberg.silver.encounters e
JOIN frequent f ON f.patient_id = e.patient_id
LEFT JOIN iceberg.silver.lab_results l ON l.encounter_id = e.encounter_id
GROUP BY e.patient_id, e.encounter_id, e.encounter_type, e.department,
         e.admission_time, e.discharge_time
ORDER BY e.admission_time;

-- 11. Data quality: quarantined Silver rows by entity and reason code.
SELECT entity, reason, count(*) AS rows
FROM iceberg.silver.quarantine CROSS JOIN UNNEST(reason_codes) AS t(reason)
GROUP BY entity, reason ORDER BY entity, rows DESC;
