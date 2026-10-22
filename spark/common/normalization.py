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

# Keys are spellings reduced by department_key(): letters only, upper case.
_DEPARTMENT_SPELLINGS = {
    "EMERGENCY": ("EMERGENCY", "ER", "ED", "EMERGENCY DEPARTMENT"),
    "ICU": ("ICU", "INTENSIVE CARE", "INTENSIVE CARE UNIT"),
    "CARDIOLOGY": ("CARDIOLOGY", "CARDIO"),
    "INTERNAL_MEDICINE": ("INTERNAL_MEDICINE", "INTERNAL MEDICINE", "IM"),
    "SURGERY": ("SURGERY", "GENERAL SURGERY", "SURG"),
}


def department_key(value: str) -> str:
    """Python twin of the Spark expression used by canonical_department()."""
    return re.sub(r"[^A-Z]", "", value.strip().upper())


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

    key = F.regexp_replace(F.upper(F.trim(column)), "[^A-Z]", "")
    return F.coalesce(*[F.when(key == F.lit(alias), F.lit(canonical))
                        for alias, canonical in sorted(DEPARTMENT_ALIASES.items())])
