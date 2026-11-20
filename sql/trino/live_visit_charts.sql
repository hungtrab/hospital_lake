-- UTC bounds are supplied by the API for the current Hanoi day.
WITH accepted AS (
    SELECT event_id,event_time,event_type,patient_id,encounter_id
    FROM iceberg.silver.realtime_events
    WHERE event_time BETWEEN TIMESTAMP '{start_utc} UTC' AND TIMESTAMP '{end_utc} UTC'
      AND encounter_id IS NOT NULL
      AND event_type IN ('PATIENT_REGISTERED','CONSULTATION_STARTED','ADMISSION')
), raw AS (
    SELECT _event_id,event_time,try(json_parse(from_utf8(raw_payload))) AS body
    FROM iceberg.bronze.stream_events_raw
    WHERE event_time BETWEEN TIMESTAMP '{start_utc} UTC' AND TIMESTAMP '{end_utc} UTC'
), matched AS (
    SELECT e.event_id,e.event_time,e.event_type,
           json_extract_scalar(r.body,'$.payload.age_years') AS age_text,
           nullif(trim(json_extract_scalar(r.body,'$.payload.condition_group')),'') AS condition_name,
           json_extract_scalar(r.body,'$.payload.wait_seconds') AS wait_text
    FROM accepted e LEFT JOIN raw r ON e.event_type <> 'ADMISSION'
      AND e.event_id=r._event_id AND e.event_time=r.event_time
      AND json_extract_scalar(r.body,'$.event_type')=e.event_type
      AND trim(json_extract_scalar(r.body,'$.payload.patient_id'))=e.patient_id
      AND trim(json_extract_scalar(r.body,'$.payload.encounter_id'))=e.encounter_id
), dedup AS (
    SELECT event_id,event_time,event_type,
        IF(count(DISTINCT ROW(age_text))=1, try_cast(max(age_text) AS integer)) AS age,
        IF(count(DISTINCT ROW(condition_name))=1, max(condition_name)) AS condition_name,
        IF(count(DISTINCT ROW(wait_text))=1, try_cast(max(wait_text) AS bigint)) AS wait_seconds
    FROM matched GROUP BY 1,2,3
)
SELECT event_type, hour(at_timezone(event_time AT TIME ZONE 'UTC','Asia/Ho_Chi_Minh')) AS local_hour,
       CASE WHEN event_type='PATIENT_REGISTERED' THEN CASE
         WHEN age BETWEEN 0 AND 17 THEN 'child' WHEN age BETWEEN 18 AND 59 THEN 'adult'
         WHEN age BETWEEN 60 AND 110 THEN 'senior' ELSE 'unknown' END END AS age_band,
       IF(event_type='PATIENT_REGISTERED',coalesce(condition_name,'Chưa ghi nhận')) AS condition_group,
       CASE WHEN event_type='CONSULTATION_STARTED' THEN CASE
         WHEN wait_seconds IS NULL OR wait_seconds<0 THEN 'unknown'
         WHEN wait_seconds<900 THEN 'short' WHEN wait_seconds<1800 THEN 'medium'
         WHEN wait_seconds<3600 THEN 'long' ELSE 'hour_plus' END END AS wait_band,
       count(*) AS events
FROM dedup GROUP BY 1,2,3,4,5
