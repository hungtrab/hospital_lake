"""Synthetic hospital batch data generator.

Produces Parquet/CSV for all six core entities with valid
patient -> encounter -> lab_result foreign keys and a deterministic seed.
A small, configurable fraction of raw-data defects (duplicates, department
aliases, invalid discharge times) is injected afterwards so the Silver layer
has realistic cleaning work; the clean data does not depend on those rates.

Usage:
    python -m generators.batch_generator.generate --patients 1000
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from generators.batch_generator.schemas import (
    BED_EVENT_SCHEMA,
    CITIES,
    DEPARTMENT_ALIASES,
    DEPARTMENTS,
    ENCOUNTER_SCHEMA,
    ENTITY_SCHEMAS,
    GENDERS,
    LAB_RESULT_SCHEMA,
    LAB_TESTS,
    MEDICATION_CATALOG,
    MEDICATION_SCHEMA,
    PATIENT_SCHEMA,
    VITAL_SCHEMA,
)

log = logging.getLogger("batch_generator")

DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")
DEFAULT_OUTPUT_DIR = Path("data/generated")
MANIFEST_NAME = "_manifest.json"
FIELDS = {name: tuple(schema.names) for name, schema in ENTITY_SCHEMAS.items()}

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
    medications_min: int = 1
    medications_max: int = 3
    vitals_min: int = 1
    vitals_max: int = 5
    bed_transfer_rate: float = 0.15

    def __post_init__(self) -> None:
        if isinstance(self.patients, bool) or not isinstance(self.patients, int) or self.patients <= 0:
            raise ValueError("patients must be > 0")
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        if not 1 <= self.encounters_min <= self.encounters_max:
            raise ValueError("require 1 <= encounters_per_patient.min <= max")
        if not 0 <= self.medications_min <= self.medications_max:
            raise ValueError("require 0 <= medications_per_encounter.min <= max")
        if not 0 <= self.vitals_min <= self.vitals_max:
            raise ValueError("require 0 <= vitals_per_encounter.min <= max")
        unknown = set(self.departments) - set(DEPARTMENTS)
        if unknown:
            raise ValueError(f"unknown departments {sorted(unknown)}; allowed: {DEPARTMENTS}")
        if not {"EMERGENCY", "ICU"} <= set(self.departments) or not self.ward_departments:
            raise ValueError("departments must include EMERGENCY, ICU and at least one ward")
        for name in ("duplicate_rate", "department_alias_rate", "invalid_discharge_rate", "bed_transfer_rate"):
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
        medications = raw.get("medications_per_encounter", {})
        vitals = raw.get("vitals_per_encounter", {})
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
            medications_min=int(medications.get("min", 1)),
            medications_max=int(medications.get("max", 3)),
            vitals_min=int(vitals.get("min", 1)),
            vitals_max=int(vitals.get("max", 5)),
            bed_transfer_rate=float(raw.get("bed_transfer_rate", 0.15)),
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
            birth_year=rng.randint(min(1930, max(1, created_at.year - 95)),
                                   min(2024, created_at.year)),
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

    return {"patients": patients, "encounters": encounters, "lab_results": labs,
            "medications": generate_medications(cfg, encounters),
            "vitals": generate_vitals(cfg, encounters),
            "bed_events": generate_bed_events(cfg, encounters)}


def _encounter_rows(encounters: dict[str, list[Any]]):
    for values in zip(*encounters.values()):
        yield dict(zip(encounters, values))


def generate_medications(cfg: GeneratorConfig, encounters: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Generate orders inside clean encounters before injecting source defects."""
    medications = _empty_columns(MEDICATION_SCHEMA)
    for encounter in _encounter_rows(encounters):
        encounter_id = encounter["encounter_id"]
        rng = random.Random(cfg.seed + 10_000 + int(encounter_id[1:]))
        end = encounter["discharge_time"] or cfg.end_date
        for index in range(1, rng.randint(cfg.medications_min, cfg.medications_max) + 1):
            name, dose, unit, route = rng.choice(MEDICATION_CATALOG)
            start = _uniform_time(rng, encounter["admission_time"], end)
            order_end = _uniform_time(rng, start, end)
            if encounter["discharge_time"] is None and rng.random() < 0.5:
                order_end = None
            _append(medications, medication_id=f"M{encounter_id[1:]}-{index:02d}",
                    patient_id=encounter["patient_id"], encounter_id=encounter_id,
                    drug_name=name, dose=dose, dose_unit=unit, route=route,
                    start_time=start, end_time=order_end)
    return medications


def generate_vitals(cfg: GeneratorConfig, encounters: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Generate synthetic observations with canonical units and UTC times."""
    vitals = _empty_columns(VITAL_SCHEMA)
    for encounter in _encounter_rows(encounters):
        encounter_id = encounter["encounter_id"]
        rng = random.Random(cfg.seed + 20_000 + int(encounter_id[1:]))
        end = encounter["discharge_time"] or cfg.end_date
        times = sorted(_uniform_time(rng, encounter["admission_time"], end)
                       for _ in range(rng.randint(cfg.vitals_min, cfg.vitals_max)))
        for index, measured_at in enumerate(times, 1):
            _append(vitals, measurement_id=f"V{encounter_id[1:]}-{index:02d}",
                    patient_id=encounter["patient_id"], encounter_id=encounter_id,
                    heart_rate=rng.randint(55, 130), spo2=round(rng.uniform(90, 100), 1),
                    systolic_bp=rng.randint(100, 160), diastolic_bp=rng.randint(55, 95),
                    temperature=round(rng.uniform(36, 39), 1), measured_at=measured_at)
    return vitals


def generate_bed_events(cfg: GeneratorConfig, encounters: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Emit a bed journey for non-outpatient encounters.

    Synthetic bed IDs are scoped to an encounter, avoiding double booking.
    A transfer closes the previous assignment and names the new bed, in the
    same department; an open encounter never emits a final release.
    """
    events = _empty_columns(BED_EVENT_SCHEMA)
    for encounter in _encounter_rows(encounters):
        if encounter["encounter_type"] == "outpatient":
            continue
        encounter_id = encounter["encounter_id"]
        rng = random.Random(cfg.seed + 30_000 + int(encounter_id[1:]))
        department = encounter["department"]
        bed_id = f"{department}-{encounter_id}-A"
        journey = [("assigned", encounter["admission_time"], bed_id)]
        end = encounter["discharge_time"] or cfg.end_date
        if rng.random() < cfg.bed_transfer_rate and (end - encounter["admission_time"]).total_seconds() > 2:
            transfer = encounter["admission_time"] + (end - encounter["admission_time"]) / 2
            bed_id = f"{department}-{encounter_id}-B"
            journey.append(("transferred", transfer.replace(microsecond=0), bed_id))
        if encounter["discharge_time"] is not None:
            journey.append(("released", encounter["discharge_time"], bed_id))
        for index, (event_type, event_time, event_bed) in enumerate(journey, 1):
            _append(events, event_id=f"B{encounter_id[1:]}-{index:02d}",
                    patient_id=encounter["patient_id"], encounter_id=encounter_id,
                    department=department, bed_id=event_bed,
                    event_type=event_type, event_time=event_time)
    return events


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


def generate_tables(cfg: GeneratorConfig) -> tuple[dict[str, pa.Table], dict[str, int]]:
    data = generate_clean(cfg)
    defects = inject_defects(cfg, data)
    tables = {name: pa.Table.from_pydict(data[name], schema=schema) for name, schema in ENTITY_SCHEMAS.items()}
    return tables, defects


def write_dataset(
    cfg: GeneratorConfig, tables: dict[str, pa.Table], defects: dict[str, int], output_dir: Path,
    output_format: str = "parquet",
) -> Path:
    """Write one Parquet file per entity plus a manifest. Re-running overwrites (idempotent)."""
    if output_format not in ("csv", "parquet"):
        raise ValueError("output_format must be csv or parquet")
    opposite = "csv" if output_format == "parquet" else "parquet"
    if any((output_dir / f"{name}.{opposite}").exists() for name in tables):
        raise ValueError("mixed CSV/Parquet sources: use a separate output directory when changing format")
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        path = output_dir / f"{name}.{output_format}"
        if output_format == "parquet":
            pq.write_table(table, path)
        else:
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=table.schema.names)
                writer.writeheader()
                for row in table.to_pylist():
                    writer.writerow({key: value.isoformat() if isinstance(value, datetime) else value
                                     for key, value in row.items()})

    config = asdict(cfg)
    config["start_date"] = cfg.start_date.isoformat()
    config["end_date"] = cfg.end_date.isoformat()
    manifest = {
        "config": config,
        "format": output_format,
        "row_counts": {name: t.num_rows for name, t in tables.items()},
        "injected_defects": defects,
    }
    manifest_path = output_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest_path


def generate(output_dir: Path | str, patients: int = 1000, seed: int = 42) -> dict[str, int]:
    """Compatibility CSV API; uses the same schema and engine as Parquet.

    Row counts now reflect realistic multiple encounters/labs per patient.
    This API produces clean fixtures; the CLI uses configurable raw defects.
    """
    from dataclasses import replace

    if isinstance(patients, bool) or not isinstance(patients, int) or patients < 1:
        raise ValueError("patients must be a positive integer")
    cfg = replace(GeneratorConfig.from_yaml(DEFAULT_CONFIG, patients=patients, seed=seed),
                  duplicate_rate=0.0, department_alias_rate=0.0, invalid_discharge_rate=0.0)
    tables, defects = generate_tables(cfg)
    write_dataset(cfg, tables, defects, Path(output_dir), "csv")
    return {name: table.num_rows for name, table in tables.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic hospital batch data (Parquet).")
    parser.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--patients", type=int, help="override config 'patients'")
    parser.add_argument("--seed", type=int, help="override config 'seed'")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = GeneratorConfig.from_yaml(args.config, patients=args.patients, seed=args.seed)

    started = time.monotonic()
    tables, defects = generate_tables(cfg)
    manifest_path = write_dataset(cfg, tables, defects, args.output_dir, args.format)
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
