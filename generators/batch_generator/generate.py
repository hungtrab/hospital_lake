"""Synthetic hospital batch data generator.

Produces Parquet files for patients, encounters and lab_results with valid
patient -> encounter -> lab_result foreign keys and a deterministic seed.
A small, configurable fraction of raw-data defects (duplicates, department
aliases, invalid discharge times) is injected afterwards so the Silver layer
has realistic cleaning work; the clean data does not depend on those rates.

Usage:
    python -m generators.batch_generator.generate --patients 1000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from generators.batch_generator.schemas import (
    CITIES,
    DEPARTMENT_ALIASES,
    DEPARTMENTS,
    ENCOUNTER_SCHEMA,
    ENTITY_SCHEMAS,
    GENDERS,
    LAB_RESULT_SCHEMA,
    LAB_TESTS,
    PATIENT_SCHEMA,
)

log = logging.getLogger("batch_generator")

DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")
DEFAULT_OUTPUT_DIR = Path("data/generated")
MANIFEST_NAME = "_manifest.json"

# encounter_type -> (selection weight, min LOS hours, max LOS hours, min labs, max labs)
ENCOUNTER_PROFILES: dict[str, tuple[float, float, float, int, int]] = {
    "outpatient": (0.40, 0.5, 3.0, 0, 3),
    "emergency": (0.30, 2.0, 12.0, 1, 5),
    "inpatient": (0.22, 24.0, 240.0, 3, 12),
    "icu": (0.08, 24.0, 336.0, 6, 20),
}


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int
    patients: int
    start_date: datetime
    end_date: datetime
    departments: tuple[str, ...]
    encounters_min: int
    encounters_max: int
    duplicate_rate: float
    department_alias_rate: float
    invalid_discharge_rate: float

    def __post_init__(self) -> None:
        if self.patients <= 0:
            raise ValueError("patients must be > 0")
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        if not 1 <= self.encounters_min <= self.encounters_max:
            raise ValueError("require 1 <= encounters_per_patient.min <= max")
        unknown = set(self.departments) - set(DEPARTMENTS)
        if unknown:
            raise ValueError(f"unknown departments {sorted(unknown)}; allowed: {DEPARTMENTS}")
        if not {"EMERGENCY", "ICU"} <= set(self.departments) or not self.ward_departments:
            raise ValueError("departments must include EMERGENCY, ICU and at least one ward")
        for name in ("duplicate_rate", "department_alias_rate", "invalid_discharge_rate"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")

    @property
    def ward_departments(self) -> tuple[str, ...]:
        return tuple(d for d in self.departments if d not in ("EMERGENCY", "ICU"))

    @classmethod
    def from_yaml(cls, path: Path, **overrides: Any) -> "GeneratorConfig":
        raw = yaml.safe_load(path.read_text())
        raw.update({k: v for k, v in overrides.items() if v is not None})
        epp = raw.get("encounters_per_patient", {})
        dq = raw.get("data_quality", {})
        return cls(
            seed=int(raw["seed"]),
            patients=int(raw["patients"]),
            start_date=_parse_date(raw["start_date"]),
            end_date=_parse_date(raw["end_date"]),
            departments=tuple(raw["departments"]),
            encounters_min=int(epp.get("min", 1)),
            encounters_max=int(epp.get("max", 5)),
            duplicate_rate=float(dq.get("duplicate_rate", 0.0)),
            department_alias_rate=float(dq.get("department_alias_rate", 0.0)),
            invalid_discharge_rate=float(dq.get("invalid_discharge_rate", 0.0)),
        )


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _uniform_time(rng: random.Random, start: datetime, end: datetime) -> datetime:
    seconds = rng.uniform(0, (end - start).total_seconds())
    return (start + timedelta(seconds=seconds)).replace(microsecond=0)


def _empty_columns(schema: pa.Schema) -> dict[str, list[Any]]:
    return {name: [] for name in schema.names}


def _append(columns: dict[str, list[Any]], **row: Any) -> None:
    for name, values in columns.items():
        values.append(row[name])


def generate_clean(cfg: GeneratorConfig) -> dict[str, dict[str, list[Any]]]:
    """Generate referentially-consistent, defect-free columns for every entity."""
    rng = random.Random(cfg.seed)
    patients = _empty_columns(PATIENT_SCHEMA)
    encounters = _empty_columns(ENCOUNTER_SCHEMA)
    labs = _empty_columns(LAB_RESULT_SCHEMA)

    types = list(ENCOUNTER_PROFILES)
    weights = [p[0] for p in ENCOUNTER_PROFILES.values()]
    # Leave room after registration so most patients get at least one encounter.
    latest_registration = cfg.start_date + (cfg.end_date - cfg.start_date) * 0.8
    encounter_seq = 0
    lab_seq = 0

    for p in range(1, cfg.patients + 1):
        patient_id = f"P{p:08d}"
        created_at = _uniform_time(rng, cfg.start_date, latest_registration)
        _append(
            patients,
            patient_id=patient_id,
            gender=rng.choices(GENDERS, weights=(0.49, 0.49, 0.02))[0],
            birth_year=rng.randint(1930, 2024),
            city=rng.choice(CITIES),
            created_at=created_at,
        )

        n_encounters = rng.randint(cfg.encounters_min, cfg.encounters_max)
        arrivals = sorted(_uniform_time(rng, created_at, cfg.end_date) for _ in range(n_encounters))
        available_from = created_at
        for arrival in arrivals:
            # Encounters of one patient never overlap.
            admission = max(arrival, available_from)
            if admission >= cfg.end_date:
                break
            encounter_type = rng.choices(types, weights=weights)[0]
            _, los_min, los_max, labs_min, labs_max = ENCOUNTER_PROFILES[encounter_type]
            if encounter_type == "emergency":
                department = "EMERGENCY"
            elif encounter_type == "icu":
                department = "ICU"
            else:
                department = rng.choice(cfg.ward_departments)
            discharge = (admission + timedelta(hours=rng.uniform(los_min, los_max))).replace(microsecond=0)
            still_admitted = discharge > cfg.end_date

            encounter_seq += 1
            encounter_id = f"E{encounter_seq:09d}"
            _append(
                encounters,
                encounter_id=encounter_id,
                patient_id=patient_id,
                encounter_type=encounter_type,
                department=department,
                admission_time=admission,
                discharge_time=None if still_admitted else discharge,
                status="admitted" if still_admitted else "discharged",
            )

            window_end = cfg.end_date if still_admitted else discharge
            spread = 1.5 if encounter_type == "icu" else 1.0
            for _ in range(rng.randint(labs_min, labs_max)):
                test = rng.choice(LAB_TESTS)
                value = round(max(0.01, rng.gauss(test.mean, test.std * spread)), 2)
                lab_seq += 1
                _append(
                    labs,
                    lab_result_id=f"L{lab_seq:010d}",
                    patient_id=patient_id,
                    encounter_id=encounter_id,
                    test_code=test.code,
                    test_name=test.name,
                    result_value=value,
                    unit=test.unit,
                    reference_low=test.reference_low,
                    reference_high=test.reference_high,
                    is_abnormal=not (test.reference_low <= value <= test.reference_high),
                    measured_at=_uniform_time(rng, admission, window_end),
                )

            if still_admitted:
                break
            available_from = discharge + timedelta(hours=1)

    return {"patients": patients, "encounters": encounters, "lab_results": labs}


def inject_defects(
    cfg: GeneratorConfig, data: dict[str, dict[str, list[Any]]]
) -> dict[str, int]:
    """Mutate clean columns in place with raw-source defects; return injected counts.

    Uses a separate RNG stream so clean values are identical for any defect rate.
    Foreign keys stay valid: defects never reference unknown IDs.
    """
    rng = random.Random(cfg.seed + 1)
    enc = data["encounters"]
    counts = {"department_aliases": 0, "invalid_discharges": 0}

    for i, dept in enumerate(enc["department"]):
        if rng.random() < cfg.department_alias_rate:
            enc["department"][i] = rng.choice(DEPARTMENT_ALIASES[dept])
            counts["department_aliases"] += 1
        discharge = enc["discharge_time"][i]
        if discharge is not None and rng.random() < cfg.invalid_discharge_rate:
            enc["discharge_time"][i] = enc["admission_time"][i] - timedelta(hours=rng.uniform(1, 48))
            counts["invalid_discharges"] += 1

    for entity in ("encounters", "lab_results"):
        columns = data[entity]
        n_rows = len(next(iter(columns.values())))
        dup_rows = [i for i in range(n_rows) if rng.random() < cfg.duplicate_rate]
        for i in dup_rows:
            for values in columns.values():
                values.append(values[i])
        counts[f"duplicate_{entity}"] = len(dup_rows)

    return counts


def generate(cfg: GeneratorConfig) -> tuple[dict[str, pa.Table], dict[str, int]]:
    data = generate_clean(cfg)
    defects = inject_defects(cfg, data)
    tables = {name: pa.Table.from_pydict(data[name], schema=schema) for name, schema in ENTITY_SCHEMAS.items()}
    return tables, defects


def write_dataset(
    cfg: GeneratorConfig, tables: dict[str, pa.Table], defects: dict[str, int], output_dir: Path
) -> Path:
    """Write one Parquet file per entity plus a manifest. Re-running overwrites (idempotent)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        pq.write_table(table, output_dir / f"{name}.parquet")

    config = asdict(cfg)
    config["start_date"] = cfg.start_date.isoformat()
    config["end_date"] = cfg.end_date.isoformat()
    manifest = {
        "config": config,
        "row_counts": {name: t.num_rows for name, t in tables.items()},
        "injected_defects": defects,
    }
    manifest_path = output_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic hospital batch data (Parquet).")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--patients", type=int, help="override config 'patients'")
    parser.add_argument("--seed", type=int, help="override config 'seed'")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = GeneratorConfig.from_yaml(args.config, patients=args.patients, seed=args.seed)

    started = time.monotonic()
    tables, defects = generate(cfg)
    manifest_path = write_dataset(cfg, tables, defects, args.output_dir)
    log.info(
        json.dumps(
            {
                "level": "INFO",
                "component": "batch_generator",
                "seed": cfg.seed,
                "patients": cfg.patients,
                "output_dir": str(args.output_dir),
                "row_counts": {name: t.num_rows for name, t in tables.items()},
                "injected_defects": defects,
                "manifest": str(manifest_path),
                "duration_seconds": round(time.monotonic() - started, 2),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
