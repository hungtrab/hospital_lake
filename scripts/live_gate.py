"""Run the existing real pipeline gate against the live Compose overlay."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shlex
import subprocess
import sys

from scripts.grafana_smoke import env_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--project', default='hospital-live')
    parser.add_argument('--restart', action='store_true')
    args = parser.parse_args()
    settings = env_file(args.env_file)
    env = {**os.environ, **settings}
    env.update(
        KAFKA_BOOTSTRAP_SERVERS='localhost:' + settings.get('KAFKA_PORT', '29092'),
        CLICKHOUSE_URL='http://localhost:' + settings.get('CLICKHOUSE_PORT', '28123'),
        CLICKHOUSE_USER='hospital_writer',
        TRINO_URL='http://localhost:' + settings.get('TRINO_PORT', '28080'),
        GRAFANA_URL='http://localhost:' + settings.get('GRAFANA_PORT', '23000'),
        COMPOSE=shlex.join(['docker', 'compose', '-p', args.project,
            '--env-file', str(args.env_file), '-f', 'docker-compose.yml',
            '-f', 'infra/deploy/compose.live.yml', '--profile', 'kafka',
            '--profile', 'realtime', '--profile', 'monitoring']),
    )
    command = [sys.executable, '-m', 'scripts.realtime_smoke', '--env-file',
               str(args.env_file), '--output', 'results/live-gate.json']
    if args.restart:
        command.append('--restart')
    return subprocess.call(command, env=env)


if __name__ == '__main__':
    raise SystemExit(main())
