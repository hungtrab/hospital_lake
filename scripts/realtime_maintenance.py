"""Remove old serving snapshots while keeping the latest published version.

Run with an administrative ClickHouse account. Iceberg source data is untouched.
Versions are time.time_ns() values; only snapshots older than the configured
retention AND older than the current publication may be removed.
"""
from __future__ import annotations
import argparse
import json
import time
from pathlib import Path
from spark.common.clickhouse import ClickHouse
from scripts.grafana_smoke import env_file


def cleanup(client, hours, *, now_ns=None):
    if hours < 1:
        raise ValueError('retention must be at least one hour')
    latest = int(client.rows('SELECT max(snapshot_version) AS v FROM hospital.published_snapshots')[0]['v'])
    if not latest:
        return {'deleted_before_version': 0, 'latest_published': 0}
    cutoff = min(latest, (now_ns or time.time_ns())-int(hours*3600*1_000_000_000))
    for table in ('published_snapshots', 'hospital_snapshots', 'department_snapshots', 'lab_snapshots'):
        client.execute(f'ALTER TABLE hospital.{table} DELETE WHERE snapshot_version < {cutoff} SETTINGS mutations_sync=1')
    return dict(deleted_before_version=cutoff, latest_published=latest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, default=Path('.env'))
    parser.add_argument('--hours', type=float, default=24)
    args = parser.parse_args()
    settings = env_file(args.env_file)
    client = ClickHouse(settings.get('CLICKHOUSE_URL'), 'hospital_admin', settings.get('CLICKHOUSE_ADMIN_PASSWORD'))
    print(json.dumps(cleanup(client, args.hours)))


if __name__ == '__main__':
    main()
