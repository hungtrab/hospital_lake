"""Transactional operator-entered visits using the simulator's Kafka outbox."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import re
from uuid import UUID

from generators.stream_generator import hanoi_mix
from generators.stream_generator.provincial import HOSPITALS

GROUPS = sorted({row[0] for row in (*hanoi_mix.OUTPATIENT, *hanoi_mix.ED)})

SCHEMA = """
CREATE TABLE IF NOT EXISTS manual_visits (
    encounter_id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, department TEXT NOT NULL,
    bed_id TEXT UNIQUE, revision INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS manual_active_patient ON manual_visits(patient_id) WHERE bed_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS manual_commands (
    request_id TEXT PRIMARY KEY, command TEXT NOT NULL, result TEXT NOT NULL);
"""
class Conflict(ValueError):
    """The requested change cannot be applied to the current visit."""


def listing(service):
    with closing(service.connect()) as db:
        active = db.execute('SELECT * FROM manual_visits WHERE bed_id IS NOT NULL ORDER BY updated_at DESC')
        items = [dict(row) for row in active]
        recent = db.execute('SELECT * FROM manual_visits WHERE bed_id IS NULL ORDER BY updated_at DESC LIMIT 20')
        items.extend(dict(row) for row in recent)
    hospital = HOSPITALS.get(service.profile)
    return dict(items=items, condition_groups=GROUPS, departments=list(hospital.beds) if hospital else [])


def choose_bed(db, hospital, ward, now):
    row = db.execute("SELECT value FROM settings WHERE key='resource_calendar'").fetchone()
    calendar = json.loads(row[0]) if row else {}
    occupied = {r[0] for r in db.execute('SELECT bed_id FROM manual_visits WHERE bed_id IS NOT NULL AND department=?', (ward,))}
    # Keep the first bed available to the automatic scheduler even at saturation.
    for i in range(hospital.beds[ward], 1, -1):
        bed = f'{ward}-B{i:03}'
        if bed not in occupied and calendar.get(bed, 0) <= now.timestamp():
            return bed
    raise Conflict('Khoa đã hết giường có thể cấp ngay. Hãy chọn khoa khác.')


def submit(service, body, now=None):
    from generators.stream_generator.daily_service import iso

    if not isinstance(body, dict):
        raise ValueError('Yêu cầu phải là một đối tượng JSON.')
    action = body.get('action')
    fields = {'admit': {'code', 'department'}, 'transfer': {'encounter_id', 'revision', 'department'},
              'discharge': {'encounter_id', 'revision'}}
    details = {k: body[k] for k in ('age_years', 'condition_group') if k in body and action == 'admit'}
    if not isinstance(action, str) or action not in fields or set(body) != fields[action] | details.keys() | {'request_id', 'action'}:
        raise ValueError('Thao tác hoặc trường nhập không hợp lệ.')
    request = body['request_id']
    try:
        if not isinstance(request, str) or str(UUID(request)) != request:
            raise ValueError
    except ValueError:
        raise ValueError('Mã yêu cầu không hợp lệ.') from None
    hospital = HOSPITALS.get(service.profile)
    if not hospital:
        raise ValueError('Cấu hình bệnh viện hiện tại chưa có sơ đồ giường.')
    ward = body.get('department')
    if action != 'discharge' and (not isinstance(ward, str) or ward not in hospital.beds):
        raise ValueError('Chọn một khoa có giường nội trú.')
    if action == 'admit':
        if 'age_years' in details and (type(details['age_years']) is not int or not 0 <= details['age_years'] <= 110):
            raise ValueError('Tuổi giả lập phải là số nguyên từ 0 đến 110.')
        if 'condition_group' in details and details['condition_group'] not in GROUPS:
            raise ValueError('Nhóm bệnh không hợp lệ.')
        if not isinstance(body['code'], str) or not re.fullmatch(r'[A-Z0-9][A-Z0-9-]{0,23}', body['code']):
            raise ValueError('Mã demo gồm 1–24 chữ in hoa, chữ số hoặc dấu gạch ngang.')
    elif (not isinstance(body['encounter_id'], str) or type(body['revision']) is not int
          or not 1 <= body['revision'] <= 2147483647):
        raise ValueError('Mã lượt hoặc phiên bản không hợp lệ.')

    command = json.dumps(body, sort_keys=True)
    with closing(service.connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        previous = db.execute('SELECT command,result FROM manual_commands WHERE request_id=?', (request,)).fetchone()
        if previous:
            if previous['command'] != command:
                raise Conflict('Mã yêu cầu này đã dùng cho một thao tác khác.')
            return json.loads(previous['result'])
        now = now or datetime.now(timezone.utc)
        if action == 'admit':
            encounter, patient = 'E-MAN-' + UUID(request).hex, 'P-MAN-' + body['code']
            if db.execute('SELECT 1 FROM manual_visits WHERE patient_id=? AND bed_id IS NOT NULL', (patient,)).fetchone():
                raise Conflict('Mã bệnh nhân đang có một lượt nội trú.')
            revision = 1
        else:
            encounter = body['encounter_id']
            visit = db.execute('SELECT * FROM manual_visits WHERE encounter_id=?', (encounter,)).fetchone()
            if not visit or not visit['bed_id'] or visit['revision'] != body['revision']:
                raise Conflict('Lượt điều trị đã thay đổi. Hãy kiểm tra trạng thái mới nhất.')
            patient, revision = visit['patient_id'], visit['revision'] + 1
            now = max(now, datetime.fromisoformat(visit['updated_at'].replace('Z', '+00:00')) + timedelta(milliseconds=1))
            if action == 'transfer' and ward == visit['department']:
                raise Conflict('Khoa tiếp nhận phải khác khoa hiện tại.')

        at = iso(now)
        if action == 'discharge':
            ward, bed = visit['department'], None
            changes = [('BED_RELEASED', dict(bed_id=visit['bed_id'])), ('DISCHARGE', {})]
        else:
            bed = choose_bed(db, hospital, ward, now)
            if action == 'admit':
                changes = [('PATIENT_REGISTERED', dict(registration_time=at)),
                           ('ADMISSION', dict(encounter_type='icu' if ward == 'ICU' else 'inpatient')),
                           ('BED_ASSIGNED', dict(bed_id=bed))]
            else:
                changes = [('TRANSFER', dict(from_bed_id=visit['bed_id'], to_bed_id=bed, from_department=visit['department']))]
        case = dict(encounter_id=encounter, patient_id=patient, department=ward,
                    bed_id=bed, revision=revision, updated_at=at)
        db.execute("""INSERT INTO manual_visits VALUES (:encounter_id,:patient_id,:department,:bed_id,:revision,:updated_at)
            ON CONFLICT(encounter_id) DO UPDATE SET department=excluded.department, bed_id=excluded.bed_id,
            revision=excluded.revision, updated_at=excluded.updated_at""", case)
        service._enqueue(db, [
            dict(event_id=f'MAN-{encounter}-{revision:010}-{index}', event_type=kind, event_time=at,
                 source='hospital-manual', version=1,
                 payload=dict(patient_id=patient, encounter_id=encounter, department=ward, **details, **extra))
            for index, (kind, extra) in enumerate(changes)])
        db.execute('INSERT INTO manual_commands VALUES (?,?,?)', (request, command, json.dumps(case)))
        return case
