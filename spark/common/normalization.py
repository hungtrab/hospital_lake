"""Canonical domain values shared by batch Silver and streaming jobs.

Source systems spell departments differently ("ER", "Cardio", ...). Silver
stores only the canonical names from plan.md §7.2; unknown spellings stay null
so the caller can quarantine them instead of guessing.
"""

from __future__ import annotations

import re

CANONICAL_DEPARTMENTS = ("EMERGENCY", "ICU", "CARDIOLOGY", "INTERNAL_MEDICINE", "SURGERY")
ENCOUNTER_TYPES = ("emergency", "inpatient", "outpatient", "icu")
ENCOUNTER_STATUSES = ("admitted", "discharged")
GENDERS = ("M", "F", "O")

# Only known separators/case are normalized. Never erase digits or arbitrary
# punctuation: ER123 and E/R must not silently turn into a known department.
DEPARTMENT_SEPARATORS = r"[ \t\r\n_-]+"
_DEPARTMENT_SPELLINGS = {
    "EMERGENCY": ("EMERGENCY", "ER", "ED", "EMERGENCY DEPARTMENT"),
    "ICU": ("ICU", "INTENSIVE CARE", "INTENSIVE CARE UNIT"),
    "CARDIOLOGY": ("CARDIOLOGY", "CARDIO"),
    "INTERNAL_MEDICINE": ("INTERNAL_MEDICINE", "INTERNAL MEDICINE", "IM"),
    "SURGERY": ("SURGERY", "GENERAL SURGERY", "SURG"),
}


def department_key(value: str) -> str:
    """Python twin of the Spark expression used by canonical_department()."""
    return re.sub(DEPARTMENT_SEPARATORS, " ", value.upper()).strip(" ")


DEPARTMENT_ALIASES = {
    department_key(spelling): canonical
    for canonical, spellings in _DEPARTMENT_SPELLINGS.items()
    for spelling in spellings
}


def canonical_department(column):
    """Spark Column: canonical department name, or null for unknown spellings.

    Built from CASE WHEN rather than a map lookup so ANSI mode never raises on
    an unknown key.
    """
    from pyspark.sql import functions as F

    key = F.trim(F.regexp_replace(F.upper(column), DEPARTMENT_SEPARATORS, " "))
    return F.coalesce(*[F.when(key == F.lit(alias), F.lit(canonical))
                        for alias, canonical in sorted(DEPARTMENT_ALIASES.items())])


# Supported synthetic source contract, not a clinical reference catalogue.
# Reference ranges remain supplied by each record; only spelling is repaired.
LAB_DEFINITIONS = {
    "GLU": ("Glucose", "mg/dL"),
    "HGB": ("Hemoglobin", "g/dL"),
    "WBC": ("White Blood Cell Count", "10^9/L"),
    "NA": ("Sodium", "mmol/L"),
    "K": ("Potassium", "mmol/L"),
    "CREA": ("Creatinine", "mg/dL"),
    "LACT": ("Lactate", "mmol/L"),
}


def lab_dictionary_value(code, index):
    """Canonical name (0) or unit (1) for an already-normalized test code."""
    from pyspark.sql import functions as F

    return F.coalesce(*[F.when(code == key, F.lit(values[index]))
                        for key, values in sorted(LAB_DEFINITIONS.items())])


def canonical_lab_unit(column):
    """Normalize explicitly supported spellings, without converting numbers.

    Unit prefixes are case-sensitive: MG must not be interpreted as mg.
    Lowercase l is an explicitly accepted litre spelling in this dataset.
    """
    from pyspark.sql import functions as F

    value = F.trim(F.regexp_replace(column, r"[ \t\r\n]+", " "))
    value = F.regexp_replace(value, r" */ *", "/")
    return F.coalesce(*[
        F.when(value.isin(unit, unit.replace("L", "l")), F.lit(unit))
        for unit in sorted({values[1] for values in LAB_DEFINITIONS.values()})
    ])
