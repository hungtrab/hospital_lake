"""Piecewise Poisson arrivals and finite clinic resources (see docs/hospital-profile.md)."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import random

from generators.stream_generator import hanoi_mix

LOCAL = timezone(timedelta(hours=7))
PROFILE = "bach-mai-v1"
DONG_ANH = "dong-anh-v1"
LAI_CHAU = "lai-chau-v1"
# Arrival shares across disjoint local-time bands. Fractions sum to one.
BANDS = (
    (0, 7, 0.045),
    (7, 10, 0.40),
    (10, 12, 0.15),
    (12, 13, 0.015),
    (13, 16, 0.28),
    (16, 17, 0.05),
    (17, 22, 0.05),
    (22, 24, 0.01),
)
# One entry per physical consultation room. The weight is a simulation assumption.
LAI_CHAU_CLINICS = (
    ("Mạn tính", 8),
    ("Mạn tính", 8),
    ("Nhi", 11),
    ("Khám sức khỏe", 7),
    ("Nội", 12),
    ("Ngoại", 8),
    ("Răng hàm mặt", 5),
    ("Tai mũi họng", 8),
    ("Mắt", 8),
    ("Y học cổ truyền", 4),
    ("Da liễu", 6),
    ("Truyền nhiễm", 4),
    ("Sản", 8),
    ("Tiêm khớp", 3),
)
LAI_CHAU_BEDS = {"ICU": 20, "EMERGENCY": 35, "CARDIOLOGY": 75, "SURGERY": 80, "INTERNAL_MEDICINE": 290}


# Group names follow Đông Anh clinical services. Room counts and shifts are
# simulated capacity; the published staff counts are hospital-wide.
DONG_ANH_CLINICS = tuple(
    (specialty, 1)
    for specialty, rooms in (
        ("Nội tổng hợp", 4),
        ("Nội tiết – đái tháo đường", 3),
        ("Nội tim mạch", 3),
        ("Hô hấp", 2),
        ("Nhi", 3),
        ("Ngoại", 2),
        ("Sản", 2),
        ("Tai mũi họng", 2),
        ("Mắt", 1),
        ("Răng hàm mặt", 1),
        ("Y học cổ truyền", 1),
    )
    for _ in range(rooms)
)
DONG_ANH_BEDS = {"ICU": 20, "EMERGENCY": 55, "CARDIOLOGY": 95, "SURGERY": 115, "INTERNAL_MEDICINE": 355}


@dataclass(frozen=True)
class Hospital:
    prefix: str
    clinics: tuple
    beds: dict
    emergency_rooms: int
    triage_nurses: int
    admission_share: float
    stay_days: tuple


BACH_MAI_CLINICS = tuple(
    (specialty, 1)
    for specialty, rooms in (
        ("Nội tổng hợp", 25), ("Tim mạch", 24), ("Thần kinh", 17),
        ("Nội tiết", 16), ("Hô hấp", 16), ("Tiêu hóa", 18), ("Thận – tiết niệu", 14),
        ("Cơ xương khớp", 14), ("Ngoại", 16), ("Huyết học", 8), ("Da liễu", 8),
    )
    for _ in range(rooms)
)
HOSPITALS = {
    LAI_CHAU: Hospital("LC", LAI_CHAU_CLINICS, LAI_CHAU_BEDS, 2, 10, 31128/134077, (2, 9, 4.9)),
    DONG_ANH: Hospital("DA", DONG_ANH_CLINICS, DONG_ANH_BEDS, 4, 12, 30635/264970, (2, 10, 6)),
    PROFILE: Hospital("BM", BACH_MAI_CLINICS,
                      {"ICU": 160, "EMERGENCY": 260, "CARDIOLOGY": 620, "SURGERY": 560, "INTERNAL_MEDICINE": 2400},
                      28, 72, 263000/1940000, (1, 9, 4.67)),
}


def arrivals(day, mean, seed, profile=PROFILE):
    day = date.fromisoformat(day)
    rng = random.Random(f"{profile}:{seed}:{day}:arrivals")
    # Five weekdays and two weekend days average to the configured annual mean.
    daily_mean = mean * (1.12 if day.weekday() < 5 else 0.70)
    bands = BANDS
    if profile == PROFILE:
        daily_mean = mean * (.92, .92, .92, .92, .92, 1.24, 1.16)[day.weekday()]
        bands = [(start, end, share) for start, end, share, _ in hanoi_mix.arrival_bands(day.weekday() >= 5)]
    midnight = datetime.combine(day, time(), LOCAL).timestamp()
    for start, end, share in bands:
        rate = daily_mean * share / ((end - start) * 3600)
        second = midnight + start * 3600
        while True:
            second += rng.expovariate(rate)
            if second >= midnight + end * 3600:
                break
            yield second


def clinic_time(earliest, opening=7):
    """Finish the queue of accepted patients, including after closing."""
    local = datetime.fromtimestamp(earliest, LOCAL)
    if local.hour < opening:
        return local.replace(hour=opening, minute=0, second=0, microsecond=0).timestamp()
    if local.hour == 12:
        return local.replace(hour=13, minute=0, second=0, microsecond=0).timestamp()
    return earliest


def emergency_staff(position, at):
    # Four teams cover each emergency position in rotating 8-hour
    # shifts, handing newly starting services to the next team at 07/15/23.
    local = datetime.fromtimestamp(at, LOCAL) - timedelta(hours=7)
    team = 1 + (local.date().toordinal() * 3 + local.hour // 8) % 4
    return f"{position}-T{team}"


def reserve(free, keys, earliest, duration, *, clinic=False, opening=7):
    key = min(keys, key=lambda k: free.get(k, 0))
    start = max(earliest, free.get(key, 0))
    if clinic:
        start = clinic_time(start, opening)
    free[key] = start + duration
    return key, start


def schedule(day, mean, seed, since=0, profile=PROFILE):
    prefix = HOSPITALS[profile].prefix
    for index, arrival in enumerate(arrivals(day, mean, seed, profile), 1):
        if arrival >= since:
            marker = "W" if profile == PROFILE else ""
            visit = f"{prefix}-{day.replace('-', '')}-{marker}{index:04}"
            yield visit, arrival


def plan(day, mean, seed, free, since=0):
    for visit, arrival in schedule(day, mean, seed, since):
        yield journey(visit, arrival, seed, free)


def journey(visit, arrival, seed, free, profile=PROFILE, *, blocked_beds=()):
    hospital = HOSPITALS[profile]
    clinics = hospital.clinics
    er_teams, triage_nurses = hospital.emergency_rooms, hospital.triage_nurses
    rng = random.Random(f"{seed}:{visit}:care")
    hour = datetime.fromtimestamp(arrival, LOCAL).hour
    demographics = {}
    if profile == PROFILE:
        at = datetime.fromtimestamp(arrival, LOCAL)
        local_hour = at.hour + at.minute / 60 + at.second / 3600
        chance = next(p for start, end, _, p in hanoi_mix.arrival_bands(at.weekday() >= 5) if start <= local_hour < end)
        urgent = rng.random() < chance
        demographics = hanoi_mix.patient_mix(rng, urgent)
        if not urgent:
            demographics.update(hanoi_mix.outpatient_diagnosis(demographics, random.Random(f"{seed}:{visit}:icd")))
    else:
        urgent = not (7 <= hour < 12 or 13 <= hour < 17) or rng.random() < 0.045
    opening = 6 if profile == PROFILE else 7
    seconds = rng.randint(120, 300)
    nurse, triage = reserve(
        free,
        [f"{'ED' if urgent else 'OP'}-N{i}" for i in range(1, 1 + (er_teams if urgent else triage_nurses))],
        arrival,
        seconds,
        clinic=not urgent, opening=opening,
    )
    if urgent:
        nurse = emergency_staff(nurse, triage)
        specialty = "Cấp cứu"
        rooms = [f"ED-{i}" for i in range(1, er_teams + 1)]
    else:
        specialty = rng.choices([s for s, _ in clinics], [w for _, w in clinics])[0]
        if demographics:
            group = demographics['condition_group']
            if group == 'Khám sức khỏe':
                specialty = 'Nội tổng hợp'
            elif group != 'Khác':
                specialty = group
        rooms = [f"OP-{i:02}" for i, (s, _) in enumerate(clinics, 1) if s == specialty]
        if demographics and specialty != 'Nội tổng hợp':
            rooms.extend(f"OP-{i:02}" for i, (s, _) in enumerate(clinics, 1) if s == 'Nội tổng hợp')
    duration = rng.randint(8, 16) * 60
    room, exam = reserve(free, rooms, triage + seconds, duration, clinic=not urgent, opening=opening)
    end = exam + duration
    base = dict(patient_id="P-" + visit, encounter_id="E-" + visit, specialty=specialty, **demographics)
    doctor = emergency_staff("BS-" + room, exam) if urgent else "BS-" + room
    if profile == LAI_CHAU and room in ("OP-01", "OP-02", "OP-03", "OP-04"):
        shift_day = datetime.fromtimestamp(exam, LOCAL).date().toordinal()
        doctor = f"BS-KKB-{1 + (shift_day + int(room[-2:]) - 1) % 6:02}"
    consult = dict(
        room_id=room, doctor_id=doctor, consultation_seconds=duration, wait_seconds=int(exam - arrival)
    )
    events = []

    def add(kind, at, **kwargs):
        timestamp = (
            datetime.fromtimestamp(at, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        events.append(
            dict(
                event_id=f"{visit}-{len(events) + 1:02}",
                event_type=kind,
                event_time=timestamp,
                source="hospital-simulator",
                version=1,
                payload={**base, **kwargs},
            )
        )

    add("PATIENT_REGISTERED", arrival)
    add(
        "VITAL_MEASUREMENT",
        triage,
        nurse_id=nurse,
        triage_seconds=seconds,
        heart_rate=rng.randint(65, 95),
        spo2=98,
        systolic_bp=120,
        diastolic_bp=80,
        temperature=36.8,
    )
    add("CONSULTATION_STARTED", exam, **consult)
    if rng.random() < 0.32:
        end += rng.randint(15, 60) * 60
        add(
            "LAB_RESULT",
            end,
            test_code="GLU",
            test_name="Glucose",
            unit="mmol/L",
            result_value=5.2 if rng.random() < 0.8 else 8.5,
            reference_low=3.9,
            reference_high=6.1,
        )
    admission_share = hospital.admission_share
    if demographics:
        admission_share = hanoi_mix.admission_probability(admission_share, urgent, demographics['condition_group'])
    if rng.random() < admission_share:
        if demographics:
            ward = hanoi_mix.inpatient_ward(rng, urgent, demographics['condition_group'], specialty)
        else:
            ward = ('SURGERY' if specialty in ('Ngoại', 'Sản') else
                    rng.choices(['ICU', 'EMERGENCY', 'CARDIOLOGY', 'INTERNAL_MEDICINE'], [3, 6, 16, 75])[0])
        stay = rng.triangular(*hospital.stay_days) * 86400
        beds = (f"{ward}-B{i:03}" for i in range(1, hospital.beds[ward] + 1))
        bed, admitted = reserve(free, [key for key in beds if key not in blocked_beds], end, stay)
        add("ADMISSION", admitted, department=ward, encounter_type="icu" if ward == "ICU" else "inpatient")
        add("BED_ASSIGNED", admitted, department=ward, bed_id=bed)
        add("BED_RELEASED", admitted + stay, department=ward, bed_id=bed)
        add("DISCHARGE", admitted + stay, department=ward)
    else:
        add("OUTPATIENT_COMPLETED", end, **consult)
    return events
