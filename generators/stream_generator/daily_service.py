"""Persistent daily patient journeys; SQLite is a producer outbox, not analytics storage."""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
from pathlib import Path
import random
import sqlite3

from generators.stream_generator.daily_schedule import build_schedule
from generators.stream_generator import provincial, manual_visits

LOG = logging.getLogger(__name__)
LOCAL = timezone(timedelta(hours=7))
SLOTS = [{"start": "08:00", "end": "12:00", "weight": 3},
         {"start": "13:00", "end": "17:00", "weight": 2}]
DEPARTMENTS = ["ICU", "CARDIOLOGY", "INTERNAL_MEDICINE", "SURGERY", "EMERGENCY"]


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def journey(visit, seed):
    visit_id = visit["visit_id"]
    rng = random.Random(hashlib.sha256(f"{seed}:{visit_id}".encode()).digest())
    arrival = datetime.fromisoformat(visit["arrival_time"].replace("Z", "+00:00"))
    department = rng.choices(DEPARTMENTS, weights=[1, 2, 3, 1, 3])[0]
    patient, encounter = "P-" + visit_id, "E-" + visit_id
    events = []

    def add(kind, at, **payload):
        events.append(dict(event_id=f"{visit_id}-{len(events)+1:02d}", event_type=kind,
                           event_time=iso(at), source="hospital-simulator", version=1,
                           payload=dict(patient_id=patient, encounter_id=encounter, **payload)))

    add("PATIENT_REGISTERED", arrival, registration_time=iso(arrival))
    add("ADMISSION", arrival, department=department,
        encounter_type="icu" if department == "ICU" else "inpatient")
    add("BED_ASSIGNED", arrival, department=department, bed_id=f"{department}-{visit_id}")
    add("LAB_RESULT", arrival+timedelta(minutes=rng.randint(10, 40)),
        test_code="GLU", test_name="Glucose", unit="mmol/L", result_value=8.5 if rng.random()<0.2 else 5.2,
        reference_low=3.9, reference_high=6.1)
    add("VITAL_MEASUREMENT", arrival+timedelta(minutes=45), heart_rate=rng.randint(65, 95),
        spo2=98, systolic_bp=120, diastolic_bp=80, temperature=36.8)
    if department == "ICU":
        add("TRANSFER", arrival+timedelta(hours=2), department="CARDIOLOGY",
            from_bed_id=f"ICU-{visit_id}", to_bed_id=f"CARDIOLOGY-{visit_id}")
        department = "CARDIOLOGY"
    duration = rng.randint(4, 8) + (24 if rng.random() < 0.2 else 0)
    discharge = arrival + timedelta(hours=duration)
    add("BED_RELEASED", discharge, department=department, bed_id=f"{department}-{visit_id}")
    add("DISCHARGE", discharge, department=department)
    return events


class DailyService:
    def __init__(self, path, *, start_date="2026-11-21", visits=100, seed=42, profile="legacy", clock=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        date.fromisoformat(start_date)
        versions = {provincial.LAI_CHAU: 2, provincial.DONG_ANH: 3, provincial.PROFILE: 5}
        max_visits = {provincial.DONG_ANH: 1000, provincial.PROFILE: 10000}.get(profile, 500)
        if type(visits) is not int or not 1 <= visits <= max_visits:
            raise ValueError(f"daily visits must be 1..{max_visits}")
        if profile != 'legacy' and profile not in versions:
            raise ValueError('unknown simulator profile')
        self.profile = profile
        now = clock or datetime.now(timezone.utc)
        identity = dict(start_date=start_date, visits=visits, seed=seed, version=1)
        if profile != 'legacy':
            identity.update(version=versions[profile], profile=profile)
        self.config = identity
        with closing(self.connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS plans (day TEXT PRIMARY KEY, visits INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS arrivals (
                    visit_id TEXT PRIMARY KEY, due REAL NOT NULL, state TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS next_arrival ON arrivals(state, due);
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY, event_time TEXT NOT NULL, patient_id TEXT NOT NULL,
                    body TEXT NOT NULL, sent_at TEXT, kafka_partition INTEGER, kafka_offset INTEGER);
                CREATE INDEX IF NOT EXISTS pending ON outbox(sent_at, event_time);
            """)
            db.executescript(manual_visits.SCHEMA)
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT value FROM settings WHERE key='identity'").fetchone()
            if old and json.loads(old[0]) != identity:
                previous = json.loads(old[0])
                from_legacy = previous.get('version') == 1 and profile != 'legacy'
                prior_version = versions.get(previous.get('profile'))
                upgrade = (prior_version is not None and previous.get('version') == prior_version
                           and prior_version < versions.get(profile, 0))
                upgrade |= previous.get('profile') == profile == provincial.PROFILE and previous.get('version') == 4
                if (not (from_legacy or upgrade) or
                    any(previous[k] != identity[k] for k in ('seed','start_date'))):
                    raise ValueError("simulation settings differ from persisted state")
                self._upgrade(db, now, from_legacy)
            db.execute("INSERT OR REPLACE INTO settings VALUES ('identity', ?)", (json.dumps(identity),))
            db.execute("INSERT OR IGNORE INTO settings VALUES ('enabled', 'false')")

    def _upgrade(self, db, now, from_legacy):
        # Replace visits with no broker acknowledgement. Already started journeys
        # keep all pending events. Replanning and the version change commit together.
        day = now.astimezone(LOCAL).date().isoformat()
        if from_legacy:
            db.execute("""DELETE FROM outbox WHERE patient_id IN (
                SELECT patient_id FROM outbox GROUP BY patient_id HAVING count(sent_at)=0)""")
        else:
            # Materialized visits may already have reached Kafka even when the
            # acknowledgement is pending. Keep their outbox and resource calendar.
            db.execute("DELETE FROM arrivals WHERE state='scheduled'")
        days = [r[0] for r in db.execute('SELECT day FROM plans WHERE day>=? ORDER BY day',(day,))]
        for planned in days:
            retained = db.execute("""SELECT count(*) FROM outbox WHERE event_id NOT GLOB 'MAN-*' AND json_extract(body,'$.event_type')='PATIENT_REGISTERED'
                AND date(event_time,'+7 hours')=?""",(planned,)).fetchone()[0]
            db.execute('DELETE FROM plans WHERE day=?',(planned,))
            self._plan(db,planned,since=now.timestamp(),retained=retained)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.execute("PRAGMA journal_mode=WAL")
        db.row_factory = sqlite3.Row
        return db

    def status(self):
        with closing(self.connect()) as db:
            enabled = db.execute("SELECT value FROM settings WHERE key='enabled'").fetchone()[0] == "true"
            counts = db.execute("SELECT count(*) AS planned_events, count(sent_at) AS sent_events, max(sent_at) AS last_sent_at FROM outbox").fetchone()
            missed = db.execute("SELECT count(*) FROM arrivals WHERE state='missed'").fetchone()[0]
            days = db.execute("SELECT day, visits FROM plans ORDER BY day DESC LIMIT 7").fetchall()
        return dict(self.config, enabled=enabled, missed_visits=missed, **dict(counts), days=[dict(r) for r in days])

    def enabled(self):
        with closing(self.connect()) as db:
            return db.execute("SELECT value FROM settings WHERE key='enabled'").fetchone()[0]=='true'

    def set_enabled(self, enabled):
        if type(enabled) is not bool:
            raise ValueError("enabled must be boolean")
        with closing(self.connect()) as db, db:
            db.execute("UPDATE settings SET value=? WHERE key='enabled'", (json.dumps(enabled),))
        return self.status()

    def plan_day(self, day):
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM plans WHERE day=?',(day,)).fetchone():
                self._plan(db,day)

    def _plan(self,db,day,since=0,retained=0):
        if self.profile == 'legacy':
            schedule = build_schedule(dict(date=day,timezone="Asia/Ho_Chi_Minh",seed=self.config['seed'],
                                           visits=self.config['visits'],slots=SLOTS))
            visits = (journey(visit,self.config['seed']) for visit in schedule)
        else:
            scheduled = list(provincial.schedule(day, self.config['visits'], self.config['seed'], since, self.profile))
            db.executemany("INSERT INTO arrivals VALUES (?, ?, 'scheduled')", scheduled)
            db.execute("INSERT INTO plans VALUES (?, ?)", (day, retained + len(scheduled)))
            return
        count = retained
        for events in visits:
            count += 1
            self._enqueue(db, events)
        db.execute('INSERT INTO plans VALUES (?,?)', (day, count))

    @staticmethod
    def _enqueue(db, events):
        db.executemany("INSERT INTO outbox(event_id,event_time,patient_id,body) VALUES (?,?,?,?)",
                       [(e['event_id'],e['event_time'],e['payload']['patient_id'],json.dumps(e)) for e in events])

    def _next_live_event(self, now):
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            # A 3 s delivery tolerance covers the 1 s scheduler poll. Unstarted
            # visits during downtime never enter Kafka or reserve a room/bed.
            db.execute("UPDATE arrivals SET state='missed' WHERE state='scheduled' AND due<?",
                       (now.timestamp()-3,))
            pending = db.execute("""SELECT * FROM outbox WHERE sent_at IS NULL AND event_time<=?
                ORDER BY event_time,event_id LIMIT 1""", (iso(now),)).fetchone()
            if pending:
                return pending
            visit = db.execute("""SELECT visit_id,due FROM arrivals WHERE state='scheduled' AND due<=?
                ORDER BY due,visit_id LIMIT 1""", (now.timestamp(),)).fetchone()
            if not visit:
                return None
            row = db.execute("SELECT value FROM settings WHERE key='resource_calendar'").fetchone()
            free = json.loads(row[0]) if row else {}
            occupied = {r[0] for r in db.execute('SELECT bed_id FROM manual_visits WHERE bed_id IS NOT NULL')}
            events = provincial.journey(visit[0], visit[1], self.config['seed'], free, self.profile, blocked_beds=occupied)
            self._enqueue(db, events)
            db.execute("INSERT OR REPLACE INTO settings VALUES ('resource_calendar',?)", (json.dumps(free),))
            db.execute("UPDATE arrivals SET state='started' WHERE visit_id=?", (visit[0],))
            return db.execute("SELECT * FROM outbox WHERE event_id=?", (events[0]['event_id'],)).fetchone()

    def pending_manual(self, now=None, limit=200):
        with closing(self.connect()) as db:
            return db.execute("""SELECT * FROM outbox WHERE sent_at IS NULL AND event_id GLOB 'MAN-*'
                AND event_time<=? ORDER BY event_time,event_id LIMIT ?""",
                (iso(now or datetime.now(timezone.utc)), limit)).fetchall()

    def tick(self, send, now=None, limit=200):
        clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
        instant = clock()
        if instant.tzinfo is None:
            raise ValueError("clock must be timezone-aware")
        if not self.enabled():
            pending = self.pending_manual(instant, limit)
            for row in pending:
                self._publish(row, send, clock)
            return len(pending)
        today = instant.astimezone(LOCAL).date()
        first = date.fromisoformat(self.config['start_date'])
        last = min(today, first + timedelta(days=30))
        if self.profile != 'legacy':
            if first <= today <= last:
                self.plan_day(today.isoformat())
            sent = 0
            while sent < limit and self.enabled():
                row = self._next_live_event(clock())
                if row is None:
                    break
                self._publish(row, send, clock)
                sent += 1
            return sent
        for offset in range((last-first).days+1):
            self.plan_day((first+timedelta(days=offset)).isoformat())
        with closing(self.connect()) as db:
            pending = db.execute("SELECT * FROM outbox WHERE sent_at IS NULL AND event_time<=? ORDER BY event_time,event_id LIMIT ?",
                                 (iso(instant), limit)).fetchall()
        sent = 0
        for row in pending:
            if not self.enabled():
                break
            self._publish(row, send, clock)
            sent += 1
        return sent

    def _publish(self, row, send, clock):
        # An ambiguous broker acknowledgement replays the same stable event ID.
        metadata = send(row['patient_id'], row['body'].encode())
        with closing(self.connect()) as db, db:
            db.execute("UPDATE outbox SET sent_at=?, kafka_partition=?, kafka_offset=? WHERE event_id=?",
                       (iso(clock()), metadata.partition, metadata.offset, row['event_id']))
