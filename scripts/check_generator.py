"""Validate Week 2 Parquet source contracts and repeatable generation at scale.

This checks generated files locally. Live Bronze ingestion and Trino verification
remain separate integration gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from generators.batch_generator.generate import DEFAULT_CONFIG, MANIFEST_NAME
from generators.batch_generator.schemas import ENTITY_SCHEMAS
from scripts.verify_bronze import ENTITY_IDS

ROOT = Path(__file__).resolve().parents[1]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_dataset(directory: Path) -> dict:
    """Validate real Parquet schemas, counts, stable keys and all patient/encounter FKs."""
    manifest = json.loads((directory / MANIFEST_NAME).read_text(encoding='utf-8'))
    if manifest.get('format') != 'parquet':
        raise ValueError('Week 2 source validation requires Parquet format')
    counts = manifest['row_counts']
    if set(counts) != set(ENTITY_SCHEMAS):
        raise ValueError('manifest must contain all six entity counts')
    patients = set()
    encounters = {}
    result = {}
    for name, schema in ENTITY_SCHEMAS.items():
        path = directory / f'{name}.parquet'
        if (directory / f'{name}.csv').exists():
            raise ValueError(f'{name}: mixed CSV/Parquet sources')
        source = pq.ParquetFile(path)
        if not source.schema_arrow.equals(schema):
            raise ValueError(f'{name}: Parquet schema differs from source contract')
        if source.metadata.num_rows != counts[name]:
            raise ValueError(f'{name}: row count differs from manifest')
        seen, duplicates = set(), 0
        keys = [ENTITY_IDS[name]]
        if name != 'patients':
            keys.append('patient_id')
        if name not in ('patients', 'encounters'):
            keys.append('encounter_id')
        for batch in source.iter_batches(batch_size=8192, columns=keys):
            for row in batch.to_pylist():
                key = row[ENTITY_IDS[name]]
                if not key:
                    raise ValueError(f'{name}: null/empty primary key')
                if key in seen:
                    duplicates += 1
                seen.add(key)
                if name == 'patients':
                    patients.add(key)
                    continue
                patient = row['patient_id']
                if patient not in patients:
                    raise ValueError(f'{name}: unknown patient_id {patient}')
                if name == 'encounters':
                    if key in encounters and encounters[key] != patient:
                        raise ValueError('encounters: duplicate key references different patients')
                    encounters[key] = patient
                elif encounters.get(row['encounter_id']) != patient:
                    raise ValueError(f'{name}: unknown/inconsistent encounter_id {row["encounter_id"]}')
        expected_duplicates = manifest['injected_defects'].get(f'duplicate_{name}', 0)
        if duplicates != expected_duplicates:
            raise ValueError(f'{name}: duplicate count {duplicates} differs from manifest {expected_duplicates}')
        result[name] = {'rows': source.metadata.num_rows, 'duplicates': duplicates,
                        'sha256': file_sha256(path)}
    if len(patients) != manifest['config']['patients']:
        raise ValueError('patient count differs from requested config')
    return result


def check_scale(patients: int, config: Path, seed: int | None) -> dict:
    """Run the real CLI twice and verify identical Parquet bytes and manifest."""
    with tempfile.TemporaryDirectory(prefix=f'hospital-lake-{patients}-') as temporary:
        directory = Path(temporary)
        command = [sys.executable, '-m', 'generators.batch_generator.generate',
                   '--patients', str(patients), '--config', str(config.resolve()),
                   '--output-dir', str(directory), '--format', 'parquet']
        if seed is not None:
            command.extend(['--seed', str(seed)])
        durations, snapshots = [], []
        for _ in range(2):
            started = time.monotonic()
            subprocess.run(command, cwd=ROOT, check=True)
            durations.append(round(time.monotonic() - started, 3))
            snapshots.append(validate_dataset(directory))
            manifest = (directory / MANIFEST_NAME).read_bytes()
            if len(snapshots) == 1:
                first_manifest = manifest
            elif manifest != first_manifest:
                raise ValueError(f'{patients}: manifest changed on same-seed rerun')
        if snapshots[0] != snapshots[1]:
            raise ValueError(f'{patients}: output changed on same-seed rerun')
        return {'patients': patients, 'generation_seconds': durations,
                'deterministic': True, 'entities': snapshots[0]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, help='only validate existing Parquet sources')
    parser.add_argument('--scales', nargs='+', type=int, default=[10000, 100000])
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--seed', type=int)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    if any(count < 1 for count in args.scales):
        parser.error('--scales must contain positive patient counts')
    try:
        report = {'checked_at': datetime.now(timezone.utc).isoformat()}
        if args.input_dir:
            report['entities'] = validate_dataset(args.input_dir)
        else:
            report['scales'] = [check_scale(count, args.config, args.seed) for count in args.scales]
        rendered = json.dumps(report, indent=2, sort_keys=True) + '\n'
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(rendered, encoding='utf-8')
        print(rendered, end='')
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f'FAIL generator: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
