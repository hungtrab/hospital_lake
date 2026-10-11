"""Actual source corruption must fail the Week 2 generator validation."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from generators.batch_generator.generate import DEFAULT_CONFIG, GeneratorConfig, generate_tables, write_dataset
from scripts.check_generator import check_scale, main, validate_dataset


@pytest.fixture
def dataset(tmp_path):
    config = GeneratorConfig.from_yaml(DEFAULT_CONFIG, patients=20)
    tables, defects = generate_tables(config)
    write_dataset(config, tables, defects, tmp_path)
    return tmp_path


def test_all_six_entities_validate(dataset):
    report = validate_dataset(dataset)
    assert len(report) == 6
    assert report['patients']['rows'] == 20
    assert all(len(item['sha256']) == 64 for item in report.values())


def test_foreign_key_corruption_is_detected(dataset):
    path = dataset / 'medications.parquet'
    table = pq.read_table(path)
    bad = ['nonexistent-patient'] * table.num_rows
    table = table.set_column(table.schema.get_field_index('patient_id'), table.schema.field('patient_id'), pa.array(bad))
    pq.write_table(table, path)
    with pytest.raises(ValueError, match='medications: unknown patient_id'):
        validate_dataset(dataset)


def test_missing_entity_and_wrong_count_fail(dataset):
    path = dataset / '_manifest.json'
    manifest = json.loads(path.read_text())
    manifest['row_counts']['patients'] += 1
    path.write_text(json.dumps(manifest))
    assert main(['--input-dir', str(dataset)]) == 1
    manifest['row_counts']['patients'] -= 1
    path.write_text(json.dumps(manifest))
    (dataset / 'vitals.parquet').unlink()
    assert main(['--input-dir', str(dataset)]) == 1


def test_repeatable_real_cli():
    report = check_scale(20, DEFAULT_CONFIG, 42)
    assert report['deterministic'] is True
    assert len(report['generation_seconds']) == 2
