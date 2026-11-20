import pytest
import json
import statistics
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from generators.stream_generator import provincial as m
from generators.stream_generator.daily_service import DailyService
from spark.streaming.kafka_reader import SUPPORTED_EVENT_TYPES
from spark.streaming.realtime_aggregates import payload_values


def test_poisson_counts_and_independent_increments():
    # Homogeneous within each band. Independent replicate seeds avoid mixing
    # weekday and weekend intensities when comparing variance to the mean.
    counts = []
    morning = []
    night = []
    first_wait = []
    opening = datetime(2026, 11, 20, 7, tzinfo=m.LOCAL).timestamp()
    expected_wait = 3 * 3600 / (724 * 1.12 * 0.40)
    for seed in range(1000):
        times = list(m.arrivals("2026-11-20", 724, seed, m.DONG_ANH))
        hours = [datetime.fromtimestamp(t, m.LOCAL).hour for t in times]
        first_wait.append(next(t for t in times if t >= opening) - opening)
        counts.append(len(times))
        morning.append(sum(7 <= h < 10 for h in hours))
        night.append(sum(h < 7 for h in hours))
    # Exponential waiting times: P(W > k / rate) = exp(-k).
    assert 0.32 < sum(w > expected_wait for w in first_wait) / len(first_wait) < 0.42
    assert 0.10 < sum(w > 2 * expected_wait for w in first_wait) / len(first_wait) < 0.18
    mean = statistics.mean(counts)
    assert abs(mean - 724 * 1.12) < 3 and 0.88 < statistics.variance(counts) / mean < 1.15
    assert abs(statistics.correlation(morning, night)) < 0.12
    assert 310 < statistics.mean(morning) < 340 and 32 < statistics.mean(night) < 41


def test_resources_contract_and_reproducibility():
    free = {}
    visits = list(m.plan("2026-11-20", 724, 42, free))
    assert visits == list(m.plan("2026-11-20", 724, 42, {}))
    assert len({x[0]["event_time"] for x in visits}) > len(visits) * 0.99
    occupied = {}
    workers = {}
    for visit in visits:
        assert all(e["event_type"] in SUPPORTED_EVENT_TYPES for e in visit)
        assert all(not payload_values(json.dumps(e["payload"]), e["event_type"], 1)[-1] for e in visit)
        starts = [e for e in visit if e["event_type"] == "CONSULTATION_STARTED"]
        assert len(starts) == 1
        assert starts[0]["payload"]["room_id"] and starts[0]["payload"]["doctor_id"]
        for e in visit:
            payload = e["payload"]
            for key, duration in [
                ("nurse_id", "triage_seconds"),
                ("doctor_id", "consultation_seconds"),
                ("room_id", "consultation_seconds"),
            ]:
                if key in payload and e["event_type"] in ("VITAL_MEASUREMENT", "CONSULTATION_STARTED"):
                    at = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")).timestamp()
                    workers.setdefault(payload[key], []).append((at, at + payload[duration]))
        beds = [e for e in visit if e["event_type"].startswith("BED_")]
        if not beds:
            assert visit[-1]["event_type"] == "OUTPATIENT_COMPLETED"
        else:
            assert len(beds) == 2
            key = beds[0]["payload"]["bed_id"]
            occupied.setdefault(key, []).append((beds[0]["event_time"], beds[1]["event_time"]))
    for stays in [*occupied.values(), *workers.values()]:
        stays.sort()
        assert all(i[1] <= j[0] for i, j in zip(stays, stays[1:]))
    assert free


def test_upgrade_keeps_started_patient_and_paces_overdue_registrations(tmp_path):
    path = tmp_path / "test.sqlite"
    old = DailyService(path, visits=10, start_date="2026-11-20")
    old.plan_day("2026-11-20")
    old.plan_day("2026-11-21")
    old.set_enabled(True)
    with old.connect() as db:
        row = db.execute("select patient_id,event_time from outbox order by event_time limit 1").fetchone()
        db.execute(
            "update outbox set sent_at=event_time,kafka_partition=0,kafka_offset=1 where event_id=(select event_id from outbox where patient_id=? order by event_time,event_id limit 1)",
            (row[0],),
        )
        original = list(
            map(tuple, db.execute("select event_id,body from outbox where patient_id=?", (row[0],)))
        )
    now = datetime.fromisoformat(row[1].replace("Z", "+00:00")) + timedelta(seconds=1)
    new = DailyService(path, visits=366, start_date="2026-11-20", profile=m.PROFILE, clock=now)
    assert new.enabled()
    with new.connect() as db:
        assert original == list(
            map(tuple, db.execute("select event_id,body from outbox where patient_id=?", (row[0],)))
        )
        assert not db.execute("select 1 from outbox where patient_id like 'P-VISIT-20261121-%'").fetchone()
        assert (
            db.execute("select count(*) from arrivals where visit_id like 'BM-20261121-%'").fetchone()[0]
            > 0
        )
    sent = []

    def send(key, body):
        sent.append(json.loads(body))
        return SimpleNamespace(partition=0, offset=len(sent))

    for _ in range(4):
        sent.clear()
        new.tick(send, now + timedelta(hours=4))
        assert sum(e["event_type"] == "PATIENT_REGISTERED" for e in sent) <= 1
    assert (
        DailyService(path, visits=366, start_date="2026-11-20", profile=m.PROFILE).status() == new.status()
    )


def test_pause_does_not_replay_absent_patients_or_reserve_their_rooms(tmp_path):
    service = DailyService(
        tmp_path / "outbox.sqlite", start_date="2026-11-21", visits=366, profile=m.PROFILE
    )
    service.set_enabled(True)
    day = "2026-11-21"
    morning = datetime(2026, 11, 21, 9, tzinfo=m.LOCAL)
    scheduled = list(m.schedule(day, 366, 42))
    first = next((visit, at) for visit, at in scheduled if at > morning.timestamp())
    now = datetime.fromtimestamp(first[1] + 0.5, timezone.utc)
    sent = []

    def send(key, body):
        sent.append(json.loads(body))
        return SimpleNamespace(partition=0, offset=len(sent))

    service.tick(send, now)
    assert [e["event_id"] for e in sent if e["event_type"] == "PATIENT_REGISTERED"] == [first[0] + "-01"]
    with service.connect() as db:
        assert db.execute("SELECT count(*) FROM arrivals WHERE state='started'").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM arrivals WHERE state='missed'").fetchone()[0] == sum(
            at < now.timestamp() - 3 for _, at in scheduled
        )
        assert db.execute("SELECT count(distinct patient_id) FROM outbox").fetchone()[0] == 1
    service.set_enabled(False)
    service.set_enabled(True)
    sent.clear()
    service.tick(send, now + timedelta(hours=6))
    for e in sent:
        if e["event_type"] == "PATIENT_REGISTERED":
            assert (
                abs(
                    (
                        now
                        + timedelta(hours=6)
                        - datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
                    ).total_seconds()
                )
                <= 3.001
            )


def test_upgrade_expires_unpublished_legacy_day(tmp_path):
    path = tmp_path / "upgrade.sqlite"
    old = DailyService(path, visits=100, start_date="2026-11-21")
    old.plan_day("2026-11-21")
    old.set_enabled(True)
    now = datetime(2026, 11, 21, 18, tzinfo=m.LOCAL)
    live = DailyService(path, visits=366, start_date="2026-11-21", profile=m.PROFILE, clock=now)
    with live.connect() as db:
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        assert db.execute("SELECT min(due) FROM arrivals").fetchone()[0] >= now.timestamp()


def test_accepted_patients_finish_after_closing_and_ed_changes_staff():
    slot = datetime(2026, 11, 20, 17, 5, tzinfo=m.LOCAL).timestamp()
    assert m.clinic_time(slot) == slot
    lunch = datetime(2026, 11, 20, 12, 10, tzinfo=m.LOCAL)
    assert datetime.fromtimestamp(m.clinic_time(lunch.timestamp()), m.LOCAL).hour == 13
    shift = datetime(2026, 11, 20, 23, tzinfo=m.LOCAL).timestamp()
    assert m.emergency_staff("BS-ED-1", shift - 1) != m.emergency_staff("BS-ED-1", shift)


@pytest.mark.parametrize("old_profile,old_mean,new_profile,new_mean", [
    (m.LAI_CHAU, 366, m.DONG_ANH, 724), (m.DONG_ANH, 724, m.PROFILE, 5315),
])
def test_profile_upgrade_preserves_outbox_and_reservations(tmp_path, old_profile, old_mean, new_profile, new_mean):
    path = tmp_path / "migration.sqlite"
    day = "2026-11-20"
    old = DailyService(path, visits=old_mean, start_date=day, profile=old_profile)
    old.plan_day(day)
    old.plan_day("2026-11-21")
    old.set_enabled(True)
    with old.connect() as db:
        due = db.execute("SELECT min(due) FROM arrivals").fetchone()[0]
    at = datetime.fromtimestamp(due + 0.1, timezone.utc)
    # Model an uncertain broker acknowledgement at the moment of switching.
    assert old._next_live_event(at) is not None

    def snapshot(service):
        with service.connect() as db:
            return (
                list(map(tuple, db.execute("SELECT * FROM outbox ORDER BY event_id"))),
                db.execute("SELECT value FROM settings WHERE key='resource_calendar'").fetchone()[0],
            )

    before = snapshot(old)
    live = DailyService(path, visits=new_mean, start_date=day, profile=new_profile, clock=at)
    assert live.enabled() and snapshot(live) == before
    with live.connect() as db:
        assert not db.execute(
            "SELECT 1 FROM arrivals WHERE state='scheduled' AND visit_id LIKE ?", (m.HOSPITALS[old_profile].prefix+"-%",)
        ).fetchone()
        assert (
            db.execute("SELECT min(due) FROM arrivals WHERE visit_id LIKE ?", (m.HOSPITALS[new_profile].prefix+"-%",)).fetchone()[0]
            >= at.timestamp()
        )
        assert db.execute("SELECT count(*) FROM arrivals WHERE visit_id LIKE ?", (m.HOSPITALS[new_profile].prefix+"-20261121-%",)).fetchone()[
            0
        ] == len(list(m.arrivals("2026-11-21", new_mean, 42, new_profile)))
    assert live.status() == DailyService(path, visits=new_mean, start_date=day, profile=new_profile).status()


def test_hanoi_weekend_hour_and_case_mix():
    from random import Random
    from generators.stream_generator import hanoi_mix
    means = []
    for day in ('2026-11-20','2026-11-21','2026-11-22'):
        counts = [len(list(m.arrivals(day, 5315, seed))) for seed in range(25)]
        means.append(statistics.mean(counts))
    assert all(abs(a-b)<80 for a,b in zip(means,[5315*.92,5315*1.24,5315*1.16]))
    for weekend in (False, True):
        bands = list(hanoi_mix.arrival_bands(weekend))
        assert sum(x[2] for x in bands) == pytest.approx(1)
        assert sum(x[2]*x[3] for x in bands) == pytest.approx(hanoi_mix.ED_SHARE)
        assert sum(x[2] for x in bands if 5<=x[0]<12) > .85
        assert bands[0][3] == pytest.approx(1) and bands[-1][3] == pytest.approx(1)
    rng = Random(22)
    for urgent in (False, True):
        patients = [hanoi_mix.patient_mix(rng, urgent) for _ in range(20000)]
        ages = [p['age_years'] for p in patients]
        assert all(0 <= a <= 95 for a in ages)
        if urgent:
            assert .54 < sum(18<=a<50 for a in ages)/len(ages) < .58
        else:
            assert min(ages) >= 18 and .70 < sum(a<60 for a in ages)/len(ages) < .735
        digestive = sum(p['condition_group']=='Tiêu hóa' for p in patients)/len(patients)
        assert abs(digestive - (.347 if urgent else .211)) < .015


def test_bach_mai_v4_replan_preserves_admitted_patients(tmp_path):
    path = tmp_path / "state"
    now = datetime(2026, 11, 21, 8, tzinfo=m.LOCAL)
    old = DailyService(path, profile=m.PROFILE, visits=5315)
    with old.connect() as db:
        db.execute("UPDATE settings SET value=? WHERE key='identity'", (json.dumps(dict(old.config,version=4)),))
        db.execute("INSERT INTO plans VALUES ('2026-11-21',2)")
        db.execute("INSERT INTO arrivals VALUES ('BM-20261121-0001',?,'started')", (now.timestamp()-60,))
        db.execute("INSERT INTO arrivals VALUES ('BM-20261121-0002',?,'scheduled')", (now.timestamp()+60,))
        old._enqueue(db,m.journey('BM-20261121-0001',now.timestamp()-60,42,{}))
        saved = list(map(tuple,db.execute('SELECT * FROM outbox ORDER BY event_id')))
    current = DailyService(path,profile=m.PROFILE,visits=5315,clock=now)
    with current.connect() as db:
        assert list(map(tuple,db.execute('SELECT * FROM outbox ORDER BY event_id'))) == saved
        assert db.execute("SELECT state FROM arrivals WHERE visit_id='BM-20261121-0001'").fetchone()[0]=='started'
        assert not db.execute("SELECT 1 FROM arrivals WHERE visit_id='BM-20261121-0002'").fetchone()
        assert db.execute("SELECT count(*) FROM arrivals WHERE state='scheduled' AND visit_id LIKE '%-W%'").fetchone()[0]>0
        assert db.execute("SELECT min(due) FROM arrivals WHERE state='scheduled'").fetchone()[0] >= now.timestamp()


def test_icd_subset_matches_age_and_clinic_group():
    from random import Random
    from generators.stream_generator import hanoi_mix as mix
    rng = Random(22)
    for senior, expected in ((False, .033), (True, .12)):
        observed = 0
        for _ in range(20000):
            group = rng.choices([row[0] for row in mix.OUTPATIENT], mix.outpatient_weights(senior))[0]
            detail = mix.outpatient_diagnosis(dict(age_years=70 if senior else 40, condition_group=group),rng)
            if detail.get('diagnosis_code') == 'I10':
                assert group == 'Tim mạch'
                observed += 1
        assert abs(observed / 20000 - expected) < .01
