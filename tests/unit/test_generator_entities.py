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


def test_vital_contract(clean_dataset):
    config, tables, encounters = clean_dataset
    rows = tables['vitals'].to_pylist()
    assert tables['vitals'].schema.equals(ENTITY_SCHEMAS['vitals'])
    assert len({row['measurement_id'] for row in rows}) == len(rows)
    counts, previous = {}, {}
    for row in rows:
        encounter = encounters[row['encounter_id']]
        assert row['patient_id'] == encounter['patient_id']
        assert 55 <= row['heart_rate'] <= 130
        assert 90 <= row['spo2'] <= 100
        assert 0 < row['diastolic_bp'] < row['systolic_bp']
        assert 36 <= row['temperature'] <= 39
        assert encounter['admission_time'] <= row['measured_at'] <= (encounter['discharge_time'] or config.end_date)
        key = row['encounter_id']
        if key in previous:
            assert previous[key] <= row['measured_at']
        previous[key] = row['measured_at']
        counts[key] = counts.get(key, 0) + 1
    assert set(counts) == set(encounters)
    assert all(config.vitals_min <= count <= config.vitals_max for count in counts.values())


def test_vital_zero_count_keeps_schema(clean_dataset):
    config, _, _ = clean_dataset
    tables, _ = generate_tables(replace(config, vitals_min=0, vitals_max=0))
    assert tables['vitals'].num_rows == 0
    assert tables['vitals'].schema.equals(ENTITY_SCHEMAS['vitals'])


def test_vital_invalid_config(clean_dataset):
    config, _, _ = clean_dataset
    with pytest.raises(ValueError, match='vitals_per_encounter'):
        replace(config, vitals_min=-1)


def test_bed_event_journeys(clean_dataset):
    from generators.batch_generator.schemas import BED_EVENT_TYPES

    config, tables, encounters = clean_dataset
    rows = tables['bed_events'].to_pylist()
    assert tables['bed_events'].schema.equals(ENTITY_SCHEMAS['bed_events'])
    assert len({row['event_id'] for row in rows}) == len(rows)
    journeys = {}
    bed_owners = {}
    for row in rows:
        encounter = encounters[row['encounter_id']]
        assert row['patient_id'] == encounter['patient_id']
        assert row['department'] == encounter['department']
        assert row['event_type'] in BED_EVENT_TYPES
        assert encounter['admission_time'] <= row['event_time'] <= (encounter['discharge_time'] or config.end_date)
        owner = bed_owners.setdefault(row['bed_id'], row['encounter_id'])
        assert owner == row['encounter_id'], 'bed shared by unrelated encounters'
        journeys.setdefault(row['encounter_id'], []).append(row)
    assert set(journeys) == {key for key, row in encounters.items() if row['encounter_type'] != 'outpatient'}
    transfers = 0
    for key, journey in journeys.items():
        encounter = encounters[key]
        assert journey[0]['event_type'] == 'assigned'
        assert journey[0]['event_time'] == encounter['admission_time']
        assert [r['event_time'] for r in journey] == sorted(r['event_time'] for r in journey)
        current_bed = journey[0]['bed_id']
        for row in journey[1:]:
            if row['event_type'] == 'transferred':
                assert row['bed_id'] != current_bed
                current_bed = row['bed_id']
                transfers += 1
            else:
                assert row['event_type'] == 'released'
                assert row['bed_id'] == current_bed
                assert row['event_time'] == encounter['discharge_time']
        if encounter['discharge_time'] is None:
            assert all(r['event_type'] != 'released' for r in journey)
        else:
            assert journey[-1]['event_type'] == 'released'
    assert transfers > 0


def test_bed_transfer_rate_controls_events(clean_dataset):
    config, _, _ = clean_dataset
    tables, _ = generate_tables(replace(config, bed_transfer_rate=0))
    assert 'transferred' not in tables['bed_events']['event_type'].to_pylist()
    with pytest.raises(ValueError, match='bed_transfer_rate'):
        replace(config, bed_transfer_rate=1.1)
