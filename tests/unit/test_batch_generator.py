"""Unit tests for the synthetic batch generator (Week 1, TV1).

Acceptance (works.md, Week 1): 1,000 patients generated, patient/encounter
foreign keys valid, deterministic for a fixed seed.
"""

from __future__ import annotations

import dataclasses
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from generators.batch_generator.generate import (
    DEFAULT_CONFIG,
    MANIFEST_NAME,
    GeneratorConfig,
    generate_tables as generate,
    main,
)
from generators.batch_generator.schemas import (
    DEPARTMENT_ALIASES,
    DEPARTMENTS,
    ENCOUNTER_STATUSES,
    ENCOUNTER_TYPES,
    ENTITY_SCHEMAS,
    GENDERS,
)

PATIENTS = 1000


@pytest.fixture(scope="module")
def config() -> GeneratorConfig:
    return GeneratorConfig.from_yaml(DEFAULT_CONFIG, patients=PATIENTS, seed=42)


@pytest.fixture(scope="module")
def clean_config(config: GeneratorConfig) -> GeneratorConfig:
    return dataclasses.replace(
        config, duplicate_rate=0.0, department_alias_rate=0.0, invalid_discharge_rate=0.0
    )


@pytest.fixture(scope="module")
def clean(clean_config):
    tables, _ = generate(clean_config)
    return {name: t.to_pylist() for name, t in tables.items()}


@pytest.fixture(scope="module")
def dirty(config):
    return generate(config)


# --- schema / volume ---------------------------------------------------------


def test_generates_requested_patient_count(clean):
    assert len(clean["patients"]) == PATIENTS
    assert len(clean["encounters"]) >= PATIENTS
    assert len(clean["lab_results"]) > 0


def test_tables_match_declared_schemas(dirty):
    tables, _ = dirty
    for name, schema in ENTITY_SCHEMAS.items():
        assert tables[name].schema.equals(schema), name


def test_primary_keys_unique_without_injected_duplicates(clean):
    for entity, key in (("patients", "patient_id"), ("encounters", "encounter_id"), ("lab_results", "lab_result_id")):
        ids = [r[key] for r in clean[entity]]
        assert len(ids) == len(set(ids)), entity


# --- referential integrity ---------------------------------------------------


def test_encounter_patient_fk_valid(dirty):
    tables, _ = dirty
    patient_ids = set(tables["patients"].column("patient_id").to_pylist())
    assert set(tables["encounters"].column("patient_id").to_pylist()) <= patient_ids


def test_lab_fks_valid_and_consistent(dirty):
    tables, _ = dirty
    encounter_patient = dict(
        zip(
            tables["encounters"].column("encounter_id").to_pylist(),
            tables["encounters"].column("patient_id").to_pylist(),
        )
    )
    for lab in tables["lab_results"].to_pylist():
        assert lab["encounter_id"] in encounter_patient
        assert encounter_patient[lab["encounter_id"]] == lab["patient_id"]


# --- domain plausibility -----------------------------------------------------


def test_enums_and_canonical_departments(clean):
    for p in clean["patients"]:
        assert p["gender"] in GENDERS
        assert 1930 <= p["birth_year"] <= 2024
    for e in clean["encounters"]:
        assert e["encounter_type"] in ENCOUNTER_TYPES
        assert e["status"] in ENCOUNTER_STATUSES
        assert e["department"] in DEPARTMENTS
        if e["encounter_type"] == "icu":
            assert e["department"] == "ICU"
        if e["encounter_type"] == "emergency":
            assert e["department"] == "EMERGENCY"


def test_encounter_timeline_plausible(clean, clean_config):
    created = {p["patient_id"]: p["created_at"] for p in clean["patients"]}
    last_discharge: dict[str, object] = {}
    for e in clean["encounters"]:
        assert e["admission_time"] >= created[e["patient_id"]]
        assert e["admission_time"] < clean_config.end_date
        if e["status"] == "discharged":
            assert e["discharge_time"] is not None
            assert e["discharge_time"] >= e["admission_time"]
        else:
            assert e["discharge_time"] is None
        # Encounters of the same patient are emitted in time order and never overlap.
        previous = last_discharge.get(e["patient_id"])
        if previous is not None:
            assert e["admission_time"] > previous
        last_discharge[e["patient_id"]] = e["discharge_time"]


def test_labs_measured_within_encounter(clean, clean_config):
    encounters = {e["encounter_id"]: e for e in clean["encounters"]}
    for lab in clean["lab_results"]:
        e = encounters[lab["encounter_id"]]
        end = e["discharge_time"] or clean_config.end_date
        assert e["admission_time"] <= lab["measured_at"] <= end


def test_abnormal_flag_matches_reference_range(clean):
    flags = []
    for lab in clean["lab_results"]:
        out_of_range = not (lab["reference_low"] <= lab["result_value"] <= lab["reference_high"])
        assert lab["is_abnormal"] == out_of_range
        flags.append(out_of_range)
    rate = sum(flags) / len(flags)
    assert 0.05 < rate < 0.5, f"implausible abnormal rate {rate:.2f}"


# --- determinism -------------------------------------------------------------


def test_same_seed_is_deterministic(config):
    first, _ = generate(config)
    second, _ = generate(config)
    for name in ENTITY_SCHEMAS:
        assert first[name].equals(second[name]), name


def test_different_seed_changes_data(config):
    a, _ = generate(config)
    b, _ = generate(dataclasses.replace(config, seed=config.seed + 100))
    assert not a["patients"].equals(b["patients"])


def test_defect_rates_do_not_change_clean_rows(clean, dirty):
    tables, defects = dirty
    n_clean = len(clean["lab_results"])
    assert tables["lab_results"].slice(0, n_clean).to_pylist() == clean["lab_results"]
    assert tables["patients"].to_pylist() == clean["patients"]


# --- injected raw defects ----------------------------------------------------


def test_injected_defects_are_reported_and_present(clean, dirty):
    tables, defects = dirty
    enc = tables["encounters"]
    assert defects["duplicate_encounters"] > 0
    assert enc.num_rows == len(clean["encounters"]) + defects["duplicate_encounters"]
    assert tables["lab_results"].num_rows == len(clean["lab_results"]) + defects["duplicate_lab_results"]

    all_aliases = {a for aliases in DEPARTMENT_ALIASES.values() for a in aliases}
    alias_rows = pc.sum(pc.is_in(enc.column("department"), value_set=pa.array(sorted(all_aliases)))).as_py()
    assert alias_rows >= defects["department_aliases"] > 0

    bad = [r for r in enc.to_pylist() if r["discharge_time"] is not None and r["discharge_time"] < r["admission_time"]]
    assert len(bad) >= defects["invalid_discharges"] > 0


# --- config validation / CLI -------------------------------------------------


@pytest.mark.parametrize(
    "change",
    [
        {"patients": 0},
        {"departments": ("EMERGENCY", "ICU")},
        {"departments": ("EMERGENCY", "ICU", "ONCOLOGY")},
        {"duplicate_rate": 1.5},
        {"encounters_min": 3, "encounters_max": 2},
    ],
)
def test_invalid_config_rejected(config, change):
    with pytest.raises(ValueError):
        dataclasses.replace(config, **change)


def test_cli_writes_parquet_and_manifest(tmp_path):
    assert main(["--patients", "50", "--seed", "7", "--output-dir", str(tmp_path)]) == 0
    manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
    assert manifest["config"]["patients"] == 50
    for name, schema in ENTITY_SCHEMAS.items():
        table = pq.read_table(tmp_path / f"{name}.parquet")
        assert table.num_rows == manifest["row_counts"][name]
        assert table.schema.names == schema.names


def test_bronze_verifier_reads_generated_parquet(tmp_path, config):
    from scripts.verify_bronze import ENTITY_IDS, source_ids
    from generators.batch_generator.generate import write_dataset

    tables, defects = generate(config)
    write_dataset(config, tables, defects, tmp_path)
    for name, table in tables.items():
        key = ENTITY_IDS[name]
        assert source_ids(tmp_path, name, key) == table[key].to_pylist()


def test_csv_and_parquet_share_data_and_manifest(tmp_path, config):
    import csv
    from generators.batch_generator.generate import write_dataset
    from scripts.verify_bronze import ENTITY_IDS, source_ids

    tables, defects = generate(config)
    parquet_dir, csv_dir = tmp_path / 'parquet', tmp_path / 'csv'
    write_dataset(config, tables, defects, parquet_dir)
    write_dataset(config, tables, defects, csv_dir, 'csv')
    for name, table in tables.items():
        assert source_ids(csv_dir, name, ENTITY_IDS[name]) == source_ids(parquet_dir, name, ENTITY_IDS[name])
        with (csv_dir / f'{name}.csv').open(newline='') as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == table.num_rows
        assert list(rows[0]) == table.schema.names
    assert json.loads((csv_dir / MANIFEST_NAME).read_text())['format'] == 'csv'
    with pytest.raises(ValueError, match='mixed CSV/Parquet'):
        write_dataset(config, tables, defects, parquet_dir, 'csv')
    assert not list(parquet_dir.glob('*.csv'))
