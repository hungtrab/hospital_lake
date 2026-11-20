"""Read-only Trino exercise of the same query and filters as the dashboard."""
import os
import re
from dashboard.hospital.server import Application
from spark.common.trino import query


def main():
    # 130 encounters, including one that crosses midnight in Vietnam.
    fixture = """(SELECT 'P-TEST-'||cast(n AS varchar) patient_id,'E-TEST-'||cast(n AS varchar) encounter_id,
        cast(n AS varchar) event_id,date_add('minute',n,TIMESTAMP '2026-11-19 16:58:00 UTC') event_time,
        'ADMISSION' event_type,'ICU' department,false is_abnormal FROM UNNEST(sequence(1,130)) AS t(n)
        UNION ALL SELECT 'P-TEST-1','E-TEST-1','close',TIMESTAMP '2026-11-19 16:59:30 UTC','DISCHARGE','ICU',false
        UNION ALL SELECT 'P-TEST-1','E-TEST-1','lab',TIMESTAMP '2026-11-19 16:59:10 UTC','LAB_RESULT','ICU',true)"""
    app = Application.__new__(Application)
    app.sql = lambda sql: query(os.environ['TRINO_URL'], re.sub(
        r"iceberg\.silver\.realtime_events(?: FOR TIMESTAMP AS OF TIMESTAMP '[0-9 .:UTC-]+')?",
        fixture, sql))
    app.cached = lambda key, load: load()
    assert len(app.patients()) == 100
    old = dict(q='e-test-1', status='discharged', department='ICU', page='1')
    assert [r['encounter_id'] for r in app.patients(dict(old, to='2026-11-19'))['items']] == ['E-TEST-1']
    assert app.patients(dict(old, **{'from':'2026-11-20'}))['items'] == []
    assert app.patients(dict(old, department='OUTPATIENT'))['items'] == []
    assert len(app.patients(dict(old, lab='abnormal'))['items']) == 1
    assert app.patients(dict(old, lab='normal'))['items'] == []
    assert app.patients(dict(old, lab='none'))['items'] == []
    pages = [app.patients(dict(page=str(n))) for n in range(1,7)]
    ids = [r['encounter_id'] for page in pages for r in page['items']]
    assert len(set(ids)) == len(ids) == 130 and ids[-1] == 'E-TEST-1'
    assert [page['more'] for page in pages] == [True]*5 + [False]
    print('PASS 130 encounters, historic ID, full lifecycle, combined filters, Vietnam midnight and paging')


if __name__ == '__main__':
    main()
