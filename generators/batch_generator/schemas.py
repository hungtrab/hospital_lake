"""Core hospital entity schemas for the synthetic batch dataset.

These Arrow schemas are the source-file contract consumed by the Bronze
ingestion job (spark/batch/ingest_bronze.py). Field names follow plan.md §5.
All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")

# --- Enumerations -----------------------------------------------------------

GENDERS = ("M", "F", "O")

ENCOUNTER_TYPES = ("emergency", "inpatient", "outpatient", "icu")

ENCOUNTER_STATUSES = ("admitted", "discharged")

# Canonical department names (plan.md §7.2). Silver normalizes aliases to these.
DEPARTMENTS = ("EMERGENCY", "ICU", "CARDIOLOGY", "INTERNAL_MEDICINE", "SURGERY")

# Raw-source spellings that Silver must map back to the canonical name.
DEPARTMENT_ALIASES: dict[str, tuple[str, ...]] = {
    "EMERGENCY": ("Emergency", "ER", "Emergency Department", "ED"),
    "ICU": ("Intensive Care", "icu", "Intensive Care Unit"),
    "CARDIOLOGY": ("Cardiology", "Cardio"),
    "INTERNAL_MEDICINE": ("Internal Medicine", "IM", "internal_medicine"),
    "SURGERY": ("Surgery", "General Surgery", "SURG"),
}

CITIES = (
    "Hanoi",
    "Ho Chi Minh City",
    "Hai Phong",
    "Da Nang",
    "Can Tho",
    "Bac Ninh",
    "Nam Dinh",
    "Thanh Hoa",
)


@dataclass(frozen=True)
class LabTest:
    code: str
    name: str
    unit: str
    reference_low: float
    reference_high: float
    mean: float
    std: float


LAB_TESTS: tuple[LabTest, ...] = (
    LabTest("GLU", "Glucose", "mg/dL", 70.0, 99.0, 85.0, 12.0),
    LabTest("HGB", "Hemoglobin", "g/dL", 12.0, 17.5, 14.0, 2.0),
    LabTest("WBC", "White Blood Cell Count", "10^9/L", 4.0, 11.0, 7.5, 2.5),
    LabTest("NA", "Sodium", "mmol/L", 135.0, 145.0, 140.0, 3.5),
    LabTest("K", "Potassium", "mmol/L", 3.5, 5.1, 4.3, 0.5),
    LabTest("CREA", "Creatinine", "mg/dL", 0.6, 1.3, 0.95, 0.3),
    LabTest("LACT", "Lactate", "mmol/L", 0.5, 2.2, 1.3, 0.6),
)

# --- Arrow schemas -----------------------------------------------------------

PATIENT_SCHEMA = pa.schema(
    [
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("gender", pa.string()),
        pa.field("birth_year", pa.int32()),
        pa.field("city", pa.string()),
        pa.field("created_at", TS),
    ]
)

ENCOUNTER_SCHEMA = pa.schema(
    [
        pa.field("encounter_id", pa.string(), nullable=False),
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("encounter_type", pa.string()),
        pa.field("department", pa.string()),
        pa.field("admission_time", TS),
        pa.field("discharge_time", TS),  # null while the patient is still admitted
        pa.field("status", pa.string()),
    ]
)

LAB_RESULT_SCHEMA = pa.schema(
    [
        pa.field("lab_result_id", pa.string(), nullable=False),
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("encounter_id", pa.string(), nullable=False),
        pa.field("test_code", pa.string()),
        pa.field("test_name", pa.string()),
        pa.field("result_value", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("reference_low", pa.float64()),
        pa.field("reference_high", pa.float64()),
        pa.field("is_abnormal", pa.bool_()),
        pa.field("measured_at", TS),
    ]
)

# Output file stem -> schema. Stems match the Bronze table names without "_raw".
ENTITY_SCHEMAS: dict[str, pa.Schema] = {
    "patients": PATIENT_SCHEMA,
    "encounters": ENCOUNTER_SCHEMA,
    "lab_results": LAB_RESULT_SCHEMA,
}
