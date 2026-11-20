"""Read-only Trino check using in-query fixtures and the deployed map query."""
import json
import os
from pathlib import Path
from spark.common.trino import query
from dashboard.hospital.room_map import snapshot
from generators.stream_generator.provincial import PROFILE

ROOT = Path(__file__).resolve().parents[2]


def main():
    cases = [
        ('consult','CONSULTATION_STARTED',-30, dict(room_id='ED-1', consultation_seconds=120)),
        ('expired','CONSULTATION_STARTED',-600, dict(room_id='ED-1', consultation_seconds=60)),
        ('home','CONSULTATION_STARTED',-60, dict(room_id='ED-2', consultation_seconds=600)),
        ('home','OUTPATIENT_COMPLETED',-30, {}),
        ('stay','BED_ASSIGNED',-600, dict(bed_id='ICU-B003')),
        ('stay','VITAL_MEASUREMENT',-30, {}),
        ('move','BED_ASSIGNED',-600, dict(bed_id='ICU-B001')),
        ('move','TRANSFER',-300, dict(to_bed_id='CARDIOLOGY-B010')),
        ('out','BED_ASSIGNED',-600, dict(bed_id='ICU-B004')),
        ('out','BED_RELEASED',-60, dict(bed_id='ICU-B004')),
        ('closed','DISCHARGE',-700, {}),
        ('closed','BED_ASSIGNED',-300, dict(bed_id='ICU-B004')),
        ('future','BED_ASSIGNED',60, dict(bed_id='ICU-B006')),
        ('conflict','BED_ASSIGNED',-120, dict(bed_id='ICU-B001')),
        ('unknown','ADMISSION',-350, {}),
    ]
    silver, bronze = [], []
    for i, (enc, kind, seconds, payload) in enumerate(cases):
        at = f"date_add('second',{seconds},CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp))"
        key = f'test-{i}'
        raw = json.dumps(dict(event_type=kind,payload=dict(patient_id='P-'+enc,encounter_id=enc,**payload)))
        ward = 'CARDIOLOGY' if kind == 'TRANSFER' else 'ICU'
        silver.append(f"('{key}',{at},'{kind}','P-{enc}','{enc}','{ward}')")
        bronze.append(f"('{key}',{at},0,{i},to_utf8('{raw}'))")
        if enc == 'conflict':
            bronze.append(bronze[-1].replace('ICU-B001','ICU-B002').replace(f',0,{i},', ',0,998,'))
    # A Kafka retry has a distinct offset but must not duplicate an occupant.
    bronze.append(bronze[0].replace(',0,0,to_utf8', ',0,999,to_utf8'))
    sql = (ROOT/'sql/trino/live_room_occupancy.sql').read_text()
    sql = sql.replace('iceberg.silver.realtime_events',
                      '(SELECT * FROM (VALUES '+','.join(silver)+') AS t(event_id,event_time,event_type,patient_id,encounter_id,department))')
    sql = sql.replace('iceberg.bronze.stream_events_raw',
                      '(SELECT * FROM (VALUES '+','.join(bronze)+') AS t(_event_id,event_time,_kafka_partition,_kafka_offset,raw_payload))')
    rooms = snapshot(PROFILE,query(os.environ['TRINO_URL'],sql))['rooms']
    locations = [(p['encounter'],room['id']) for room in rooms for p in room['patients']]
    assert sorted(locations) == sorted([('consult','ED-1'),('stay','ICU-R002'),
                                        ('move','CARDIOLOGY-R003'),('unknown','UNMAPPED-ICU'),('conflict','UNMAPPED-ICU')]), locations
    print('PASS consultation expiry, bed occupancy, transfer, release, terminal stays, unknown beds and Kafka retries')


if __name__=='__main__':
    main()
