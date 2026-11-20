"""Exercise the dashboard query on Trino with an isolated inline event ledger."""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from spark.common.trino import query


def main():
    start = datetime(2026, 11, 19, 17)
    events, raw = [], []
    def add(key, kind, offset=0, **payload):
        at = (start + timedelta(seconds=offset)).isoformat(sep=' ')
        event = f"('{key}',TIMESTAMP '{at} UTC','{kind}','P','E')"
        if event not in events:
            events.append(event)
        body = json.dumps(dict(event_type=kind,payload=dict(patient_id='P',encounter_id='E',**payload)))
        raw.append(f"('{key}',TIMESTAMP '{at} UTC',to_utf8('{body}'))")
    for age in (17,18,59,60):
        add(str(age),'PATIENT_REGISTERED',age_years=age,condition_group='A')
    raw.append(raw[0])
    add('missing','PATIENT_REGISTERED')
    add('conflict','PATIENT_REGISTERED',age_years=38,condition_group='A')
    add('conflict','PATIENT_REGISTERED',age_years=38,condition_group='B')
    for delay in (-1,899,900,1799,1800,3599,3600):
        add(str(delay),'CONSULTATION_STARTED',3600,wait_seconds=delay)
    add('previous','ADMISSION',-1)
    add('future','ADMISSION',7201)
    add('bed','ADMISSION',7200)
    sql = (Path(__file__).resolve().parents[2]/'sql/trino/live_visit_charts.sql').read_text().format(
        start_utc='2026-11-19 17:00:00',end_utc='2026-11-19 19:00:00')
    sql = sql.replace('iceberg.silver.realtime_events', '(SELECT * FROM (VALUES '+','.join(events)+') t(event_id,event_time,event_type,patient_id,encounter_id))')
    sql = sql.replace('iceberg.bronze.stream_events_raw', '(SELECT * FROM (VALUES '+','.join(raw)+') t(_event_id,event_time,raw_payload))')
    rows = query(os.environ.get('TRINO_URL','http://trino:8080'),sql)
    buckets = {tuple(r[:-1]):r[-1] for r in rows}
    assert buckets == {
        ('PATIENT_REGISTERED',0,'child','A',None):1,('PATIENT_REGISTERED',0,'adult','A',None):2,
        ('PATIENT_REGISTERED',0,'senior','A',None):1,('PATIENT_REGISTERED',0,'unknown','Chưa ghi nhận',None):1,
        ('PATIENT_REGISTERED',0,'adult','Chưa ghi nhận',None):1,
        **{('CONSULTATION_STARTED',1,None,None,k):n for k,n in
           [('unknown',1),('short',1),('medium',2),('long',2),('hour_plus',1)]},
        ('ADMISSION',2,None,None,None):1}, buckets
    print('PASS local midnight, future exclusion, retries, conflicting details, missing data and bucket boundaries')


if __name__ == '__main__':
    main()
