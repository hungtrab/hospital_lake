-- Only validated lifecycle/consultation events can establish a location.
-- Keep the last completed location transition per encounter before reading
-- raw resource identifiers. Lab results and vital signs do not vacate a bed.
WITH transitions AS (
    SELECT e.*, count_if(event_type IN ('DISCHARGE','OUTPATIENT_COMPLETED')) OVER
        (PARTITION BY encounter_id) AS closed, row_number() OVER (
        PARTITION BY encounter_id ORDER BY event_time DESC,
        CASE event_type WHEN 'DISCHARGE' THEN 8 WHEN 'OUTPATIENT_COMPLETED' THEN 7
            WHEN 'TRANSFER' THEN 6 WHEN 'BED_ASSIGNED' THEN 5 WHEN 'BED_RELEASED' THEN 4
            WHEN 'ADMISSION' THEN 3 ELSE 1 END DESC, event_id DESC
    ) AS seq
    FROM iceberg.silver.realtime_events e
    WHERE encounter_id IS NOT NULL AND event_time <= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)
      AND event_type IN ('CONSULTATION_STARTED','OUTPATIENT_COMPLETED','ADMISSION',
                         'BED_ASSIGNED','BED_RELEASED','TRANSFER','DISCHARGE')
), active AS (
    SELECT * FROM transitions WHERE seq=1 AND closed=0 AND
        (event_type IN ('BED_ASSIGNED','TRANSFER','ADMISSION') OR
         (event_type='CONSULTATION_STARTED' AND event_time >= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) - INTERVAL '1' HOUR))
), raw_events AS (
    SELECT _event_id,event_time,_kafka_partition,_kafka_offset,
           try(json_parse(from_utf8(raw_payload))) AS envelope
    FROM iceberg.bronze.stream_events_raw
), locations AS (
    SELECT a.encounter_id,a.patient_id,a.event_type,a.event_time,a.department,
           CASE WHEN count(DISTINCT ROW(json_extract_scalar(r.envelope,'$.payload.room_id'),
               json_extract_scalar(r.envelope,'$.payload.bed_id'),json_extract_scalar(r.envelope,'$.payload.to_bed_id'),
               json_extract_scalar(r.envelope,'$.payload.consultation_seconds'))) = 1
               THEN min_by(json_extract(r.envelope,'$.payload'), ROW(r._kafka_partition,r._kafka_offset)) END AS p
    FROM active a LEFT JOIN raw_events r
        ON r._event_id=a.event_id AND r.event_time=a.event_time
        AND json_extract_scalar(r.envelope,'$.event_type')=a.event_type
        AND trim(json_extract_scalar(r.envelope,'$.payload.encounter_id'))=a.encounter_id
        AND trim(json_extract_scalar(r.envelope,'$.payload.patient_id'))=a.patient_id
    GROUP BY 1,2,3,4,5
)
SELECT encounter_id,patient_id,event_type,to_iso8601(event_time AT TIME ZONE 'UTC'),department,
    json_extract_scalar(p,'$.room_id'),
    CASE WHEN event_type='TRANSFER' THEN json_extract_scalar(p,'$.to_bed_id') ELSE json_extract_scalar(p,'$.bed_id') END,
    json_extract_scalar(p,'$.specialty'),json_extract_scalar(p,'$.doctor_id')
FROM locations
WHERE event_type <> 'CONSULTATION_STARTED' OR
    (try_cast(json_extract_scalar(p,'$.consultation_seconds') AS integer) BETWEEN 1 AND 3600 AND
     date_diff('millisecond',event_time,CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) <
         BIGINT '1000' * try_cast(json_extract_scalar(p,'$.consultation_seconds') AS integer))
ORDER BY event_time,encounter_id
