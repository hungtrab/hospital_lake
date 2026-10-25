#!/usr/bin/env bash
# Checkpoint 1 demo (docs/demo-script.md): services -> batch -> Trino queries -> tests.
# Usage: PYTHON=.venv/bin/python scripts/checkpoint_demo.sh        # batch gate
#        STREAM=1 PYTHON=.venv/bin/python scripts/checkpoint_demo.sh  # + Kafka producer
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=${PYTHON:-python3}
STREAM=${STREAM:-0}
step() { printf '\n== %s ==\n' "$*"; }
env_value() { grep -E "^$1=" .env | tail -1 | cut -d= -f2-; }

step "0. Environment"
[ -f .env ] || cp .env.example .env
docker compose version
"$PYTHON" --version

step "1. Lakehouse services (MinIO, Iceberg REST, Trino)"
make up
docker compose ps

step "2. Batch layer: generate -> Bronze -> verify -> Silver -> Gold"
make batch PYTHON="$PYTHON"

step "3. Historical analytics through Trino"
make query PYTHON="$PYTHON"

if [ "$STREAM" = 1 ]; then
    step "4. Speed layer transport: Kafka + simulated hospital events"
    kafka_port=$(env_value KAFKA_PORT || true)
    make kafka-up
    make kafka-smoke PYTHON="$PYTHON"
    make produce PYTHON="$PYTHON" KAFKA_BOOTSTRAP_SERVERS="localhost:${kafka_port:-9092}" RATE=100 DURATION=10
fi

step "5. Tests"
make test PYTHON="$PYTHON"

step "Checkpoint demo finished"
