"""Realtime hospital event envelope and a deterministic journey simulator.

Every message follows the shared envelope (plan.md §6) that
spark/streaming/kafka_reader.parse_kafka_events validates:

    {"event_id", "event_type", "event_time", "source", "version", "payload"}

The simulator keeps a pool of open encounters and emits events in a plausible
order per encounter: PATIENT_REGISTERED (new patients) -> ADMISSION ->
BED_ASSIGNED -> LAB_RESULT / VITAL_MEASUREMENT / MEDICATION_ORDER / TRANSFER
-> DISCHARGE -> BED_RELEASED. Content depends only on the seed; event times
come from an injectable clock so live runs use wall-clock UTC.

On purpose, a configurable fraction of messages is defective (AGENTS.md §27):
exact duplicates, late events (event_time older than the stream watermark)
and invalid messages that the stream must route to its DLQ/quarantine.
"""

from __future__ import annotations

import json
import random
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Deque, Dict, List, Optional

from generators.batch_generator.schemas import CITIES, DEPARTMENTS, GENDERS, LAB_TESTS, MEDICATION_CATALOG

SOURCE = "hospital-simulator"
SCHEMA_VERSION = 1
EVENT_TYPES = (
    "PATIENT_REGISTERED", "ADMISSION", "DISCHARGE", "TRANSFER", "LAB_RESULT",
    "MEDICATION_ORDER", "VITAL_MEASUREMENT", "BED_ASSIGNED", "BED_RELEASED",
)
INVALID_KINDS = ("not_json", "missing_event_id", "unknown_event_type", "bad_event_time", "payload_not_object")

# encounter_type -> (weight, department or None for a ward department, occupies a bed)
ENCOUNTER_PROFILES = {
    "outpatient": (0.35, None, False),
    "emergency": (0.35, "EMERGENCY", True),
    "inpatient": (0.20, None, True),
    "icu": (0.10, "ICU", True),
}
# Clinical activity for an open encounter; discharge ends it.
ACTIVITY_WEIGHTS = {
    "VITAL_MEASUREMENT": 0.40,
    "LAB_RESULT": 0.25,
    "MEDICATION_ORDER": 0.12,
    "TRANSFER": 0.03,
    "DISCHARGE": 0.20,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class SimulatorConfig:
    seed: int = 42
    max_active_encounters: int = 500
    returning_patient_probability: float = 0.3
    late_event_probability: float = 0.02
    late_delay_minutes: tuple = (11.0, 30.0)
    duplicate_probability: float = 0.01
    invalid_event_probability: float = 0.005

    def __post_init__(self) -> None:
        if self.max_active_encounters < 1:
            raise ValueError("max_active_encounters must be >= 1")
        for name in ("returning_patient_probability", "late_event_probability",
                     "duplicate_probability", "invalid_event_probability"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        low, high = self.late_delay_minutes
        if not 0 < low <= high:
            raise ValueError("late_delay_minutes must be 0 < min <= max")


@dataclass(frozen=True)
class Message:
    """One Kafka record. ``kind`` is test/metrics metadata and is never sent."""

    key: str
    value: bytes
    kind: str  # "valid", "late", "duplicate" or "invalid"
    event_type: Optional[str] = None


@dataclass
class _Encounter:
    patient_id: str
    encounter_id: str
    encounter_type: str
    department: str
    bed_id: Optional[str]
    beds_used: int = 1


@dataclass
class HospitalSimulator:
    config: SimulatorConfig = field(default_factory=SimulatorConfig)
    clock: Callable[[], datetime] = utc_now

    def __post_init__(self) -> None:
        self._rng = random.Random(self.config.seed)
        self._defects = random.Random(self.config.seed + 1)  # defects never change clean content
        self._pending: Deque[Message] = deque()
        self._active: List[_Encounter] = []
        self._returning: List[str] = []  # registered patients with no open encounter
        self._counters: Dict[str, int] = {"patient": 0, "encounter": 0}
        self._wards = tuple(d for d in DEPARTMENTS if d not in ("EMERGENCY", "ICU"))

    # --- public API -----------------------------------------------------------

    def next_message(self) -> Message:
        if not self._pending:
            self._step()
        return self._pending.popleft()

    def messages(self, count: int) -> List[Message]:
        return [self.next_message() for _ in range(count)]

    # --- simulation -----------------------------------------------------------

    def _step(self) -> None:
        rng = self._rng
        fill = len(self._active) / self.config.max_active_encounters
        if not self._active or (fill < 1 and rng.random() < 0.5 * (1 - fill) + 0.05):
            self._admit()
        else:
            self._activity(rng.choice(self._active))
        if self._defects.random() < self.config.invalid_event_probability:
            self._pending.append(self._invalid_message())

    def _admit(self) -> None:
        rng = self._rng
        if self._returning and rng.random() < self.config.returning_patient_probability:
            # A patient is never admitted twice at once: swap-remove from the idle pool.
            index = rng.randrange(len(self._returning))
            self._returning[index], self._returning[-1] = self._returning[-1], self._returning[index]
            patient_id = self._returning.pop()
        else:
            self._counters["patient"] += 1
            patient_id = f"PS{self._counters['patient']:08d}"
            self._emit("PATIENT_REGISTERED", patient_id, {
                "patient_id": patient_id,
                "gender": rng.choices(GENDERS, weights=(0.49, 0.49, 0.02))[0],
                "birth_year": rng.randint(1930, 2024),
                "city": rng.choice(CITIES),
            })
        types = list(ENCOUNTER_PROFILES)
        encounter_type = rng.choices(types, weights=[p[0] for p in ENCOUNTER_PROFILES.values()])[0]
        _, department, needs_bed = ENCOUNTER_PROFILES[encounter_type]
        department = department or rng.choice(self._wards)
        self._counters["encounter"] += 1
        encounter_id = f"ES{self._counters['encounter']:09d}"
        encounter = _Encounter(patient_id, encounter_id, encounter_type, department,
                               f"{department}-{encounter_id}-1" if needs_bed else None)
        self._active.append(encounter)
        self._emit("ADMISSION", patient_id, {
            "patient_id": patient_id, "encounter_id": encounter_id,
            "encounter_type": encounter_type, "department": department,
        })
        if encounter.bed_id:
            self._emit("BED_ASSIGNED", patient_id, self._bed_payload(encounter))

    def _activity(self, encounter: _Encounter) -> None:
        rng = self._rng
        kinds = list(ACTIVITY_WEIGHTS)
        kind = rng.choices(kinds, weights=list(ACTIVITY_WEIGHTS.values()))[0]
        ids = {"patient_id": encounter.patient_id, "encounter_id": encounter.encounter_id}
        if kind == "TRANSFER" and not encounter.bed_id:
            kind = "VITAL_MEASUREMENT"
        if kind == "VITAL_MEASUREMENT":
            self._emit(kind, encounter.patient_id, {
                **ids,
                "heart_rate": rng.randint(50, 130), "spo2": round(rng.uniform(88, 100), 1),
                "systolic_bp": rng.randint(95, 170), "diastolic_bp": rng.randint(55, 105),
                "temperature": round(rng.gauss(36.9, 0.6), 1),
            })
        elif kind == "LAB_RESULT":
            test = rng.choice(LAB_TESTS)
            value = round(max(0.01, rng.gauss(test.mean, test.std)), 2)
            self._emit(kind, encounter.patient_id, {
                **ids, "test_code": test.code, "test_name": test.name, "result_value": value,
                "unit": test.unit, "reference_low": test.reference_low,
                "reference_high": test.reference_high,
                "is_abnormal": not test.reference_low <= value <= test.reference_high,
            })
        elif kind == "MEDICATION_ORDER":
            drug, dose, unit, route = rng.choice(MEDICATION_CATALOG)
            self._emit(kind, encounter.patient_id, {
                **ids, "drug_name": drug, "dose": dose, "dose_unit": unit, "route": route})
        elif kind == "TRANSFER":
            old_bed = encounter.bed_id
            encounter.beds_used += 1
            encounter.bed_id = f"{encounter.department}-{encounter.encounter_id}-{encounter.beds_used}"
            self._emit(kind, encounter.patient_id, {
                **ids, "department": encounter.department,
                "from_bed_id": old_bed, "to_bed_id": encounter.bed_id})
        else:
            self._active.remove(encounter)
            self._returning.append(encounter.patient_id)
            self._emit("DISCHARGE", encounter.patient_id, {**ids, "department": encounter.department})
            if encounter.bed_id:
                self._emit("BED_RELEASED", encounter.patient_id, self._bed_payload(encounter))

    @staticmethod
    def _bed_payload(encounter: _Encounter) -> dict:
        return {"patient_id": encounter.patient_id, "encounter_id": encounter.encounter_id,
                "department": encounter.department, "bed_id": encounter.bed_id}

    # --- envelopes and defects ------------------------------------------------

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _emit(self, event_type: str, patient_id: str, payload: dict) -> None:
        event_time = self.clock()
        kind = "valid"
        if self._defects.random() < self.config.late_event_probability:
            low, high = self.config.late_delay_minutes
            event_time -= timedelta(minutes=self._defects.uniform(low, high))
            kind = "late"
        envelope = {
            "event_id": self._event_id(), "event_type": event_type, "event_time": iso(event_time),
            "source": SOURCE, "version": SCHEMA_VERSION, "payload": payload,
        }
        value = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode()
        self._pending.append(Message(patient_id, value, kind, event_type))
        if self._defects.random() < self.config.duplicate_probability:
            self._pending.append(Message(patient_id, value, "duplicate", event_type))

    def _invalid_message(self) -> Message:
        kind = self._defects.choice(INVALID_KINDS)
        patient_id = f"PS{self._counters['patient']:08d}"
        envelope = {
            "event_id": str(uuid.UUID(int=self._defects.getrandbits(128), version=4)),
            "event_type": "VITAL_MEASUREMENT", "event_time": iso(self.clock()),
            "source": SOURCE, "version": SCHEMA_VERSION,
            "payload": {"patient_id": patient_id, "heart_rate": 80},
        }
        if kind == "not_json":
            return Message(patient_id, b'{"event_id": "truncated', "invalid")
        if kind == "missing_event_id":
            del envelope["event_id"]
        elif kind == "unknown_event_type":
            envelope["event_type"] = "BED_TELEPORTED"
        elif kind == "bad_event_time":
            envelope["event_time"] = "yesterday afternoon"
        else:
            envelope["payload"] = "heart_rate=80"
        return Message(patient_id, json.dumps(envelope, sort_keys=True).encode(), "invalid")
