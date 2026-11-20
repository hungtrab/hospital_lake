from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4
import json
import pytest

from generators.stream_generator import manual_visits as manual, provincial
from generators.stream_generator.daily_service import DailyService
from spark.streaming.realtime_aggregates import payload_values


def command(action, **fields):
    return dict(request_id=str(uuid4()), action=action, **fields)


def test_manual_journey_and_retry(tmp_path):
    path = tmp_path / 'state.sqlite'
    service = DailyService(path, profile=provincial.PROFILE)
    now = datetime.now(timezone.utc)
    intake = command('admit', code='DEMO-22', department='ICU', age_years=68, condition_group='Tim mạch')
    first = manual.submit(service, intake, now)
    assert manual.submit(service, intake, now) == first
    with pytest.raises(manual.Conflict):
        manual.submit(service, dict(intake, code='CHANGED'))
    with pytest.raises(manual.Conflict):
        manual.submit(service, command('admit', code='DEMO-22', department='SURGERY'))
    info = dict(encounter_id=first['encounter_id'], revision=1)
    second = manual.submit(service, command('transfer', department='CARDIOLOGY', **info), now)
    with pytest.raises(manual.Conflict):
        manual.submit(service, command('discharge', **info), now)
    final = manual.submit(service, command('discharge', **dict(info, revision=2)), now)
    assert final['bed_id'] is None and final['updated_at'] > second['updated_at'] > first['updated_at']
    service = DailyService(path, profile=provincial.PROFILE)
    assert manual.listing(service)['items'] == [final]
    sent = []
    def send(key, body):
        sent.append(json.loads(body))
        return SimpleNamespace(partition=0, offset=len(sent))
    assert not service.enabled()
    with pytest.raises(ConnectionError):
        service.tick(lambda *_: (_ for _ in ()).throw(ConnectionError()), now + timedelta(seconds=1))
    assert service.tick(send, now + timedelta(seconds=1)) == 6
    assert service.tick(send, now + timedelta(seconds=1)) == 0
    assert [e['event_type'] for e in sent] == [
        'PATIENT_REGISTERED', 'ADMISSION', 'BED_ASSIGNED', 'TRANSFER', 'BED_RELEASED', 'DISCHARGE']
    assert len(set(e['event_id'] for e in sent)) == 6
    assert sent[0]['payload']['age_years'] == 68 and sent[0]['payload']['condition_group'] == 'Tim mạch'
    assert sent[3]['payload']['to_bed_id'] == second['bed_id']
    assert sent[3]['payload']['from_bed_id'] == first['bed_id']
    assert not any(payload_values(json.dumps(e['payload']), e['event_type'], 1)[-1] for e in sent)


def test_reserved_and_manual_beds_do_not_overlap(tmp_path):
    service = DailyService(tmp_path / 'state', profile=provincial.PROFILE)
    with service.connect() as db:
        db.execute("INSERT INTO settings VALUES ('resource_calendar',?)", (json.dumps({'ICU-B160':1e11}),))
    first = manual.submit(service, command('admit', code='A', department='ICU'))
    second = manual.submit(service, command('admit', code='B', department='ICU'))
    assert [first['bed_id'], second['bed_id']] == ['ICU-B159','ICU-B158']
    with pytest.raises(ValueError):
        manual.submit(service, command('admit', code='bad name!', department='ICU'))
    for i in range(500):
        events = provincial.journey(f'test-{i}', 1800000000, 7, {})
        assignment = next((e for e in events if e['event_type'] == 'BED_ASSIGNED'), None)
        if assignment:
            bed = assignment['payload']['bed_id']
            retry = provincial.journey(f'test-{i}', 1800000000, 7, {}, blocked_beds={bed})
            assert next(e for e in retry if e['event_type'] == 'BED_ASSIGNED')['payload']['bed_id'] != bed
            break
    else:
        pytest.fail('no admission generated')
