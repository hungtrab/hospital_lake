"""Week 2 entity contracts: FK consistency, time bounds, stable IDs and schemas."""

from dataclasses import replace

import pytest

from generators.batch_generator.generate import DEFAULT_CONFIG, GeneratorConfig, generate_tables
from generators.batch_generator.schemas import ENTITY_SCHEMAS, MEDICATION_CATALOG


@pytest.fixture(scope='module')
def clean_dataset():
    config = replace(GeneratorConfig.from_yaml(DEFAULT_CONFIG, patients=1000),
                     duplicate_rate=0.0, department_alias_rate=0.0, invalid_discharge_rate=0.0)
    tables, _ = generate_tables(config)
    encounters = {row['encounter_id']: row for row in tables['encounters'].to_pylist()}
    return config, tables, encounters


def test_medication_contract(clean_dataset):
    config, tables, encounters = clean_dataset
    rows = tables['medications'].to_pylist()
    assert tables['medications'].schema.equals(ENTITY_SCHEMAS['medications'])
    assert len(rows) >= len(encounters)
    assert len({row['medication_id'] for row in rows}) == len(rows)
    counts = {}
    for row in rows:
        encounter = encounters[row['encounter_id']]
        assert row['patient_id'] == encounter['patient_id']
        assert (row['drug_name'], row['dose'], row['dose_unit'], row['route']) in MEDICATION_CATALOG
        end = encounter['discharge_time'] or config.end_date
        assert encounter['admission_time'] <= row['start_time'] <= end
        if row['end_time'] is None:
            assert encounter['status'] == 'admitted'
        else:
            assert row['start_time'] <= row['end_time'] <= end
        counts[row['encounter_id']] = counts.get(row['encounter_id'], 0) + 1
    assert all(config.medications_min <= count <= config.medications_max for count in counts.values())


def test_medication_zero_count_keeps_schema(clean_dataset):
    config, _, _ = clean_dataset
    tables, _ = generate_tables(replace(config, medications_min=0, medications_max=0))
    assert tables['medications'].num_rows == 0
    assert tables['medications'].schema.equals(ENTITY_SCHEMAS['medications'])


def test_medication_invalid_config(clean_dataset):
    config, _, _ = clean_dataset
    with pytest.raises(ValueError, match='medications_per_encounter'):
        replace(config, medications_min=3, medications_max=1)
