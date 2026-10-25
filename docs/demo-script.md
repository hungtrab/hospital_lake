# Checkpoint 1 demo script (26/10)

What the checkpoint shows: executable code in Git, data flowing through the
batch layer (Bronze → Silver → Gold on MinIO + Iceberg), historical SQL through
Trino, and the system architecture (`docs/architecture.md`). All data is
synthetic.

The whole flow is scripted:

```bash
git clone https://github.com/hungtrab/hospital_lake.git && cd hospital_lake
python3 -m venv .venv && make setup PYTHON=.venv/bin/python
.venv/bin/python -m pip install -r requirements-spark.txt   # local Spark unit tests
PYTHON=.venv/bin/python scripts/checkpoint_demo.sh          # batch gate
STREAM=1 PYTHON=.venv/bin/python scripts/checkpoint_demo.sh # + Kafka producer
```

Requirements: Docker Compose v2, Python 3 with the requirements files, Java 17
for the local Spark unit tests (`JAVA_HOME`; without PySpark they are skipped).
Batch jobs themselves run inside the Compose Spark image. If ports 9000/9001/8080/9092 are taken, change
`MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `TRINO_PORT`, `KAFKA_PORT` in `.env`
(not `docker-compose.yml`).

## Steps and expected output

Numbers below are from the default dataset (`PATIENTS=1000 SEED=42`) measured
on 25/10. The same seed gives the same numbers on any machine.

### 1. Architecture (slides / `docs/architecture.md`)

Lambda layers: batch (Spark → Iceberg Bronze/Silver/Gold → Trino) and speed
(producer → Kafka → Structured Streaming → Bronze; ClickHouse/Grafana are
Week 5). Point out which parts run today and which are planned.

### 2. Services — `make up`

`docker compose ps` lists `minio`, `iceberg-rest` and `trino` as healthy.
MinIO console: `http://localhost:${MINIO_CONSOLE_PORT}` (bucket `warehouse`).

### 3. Batch layer — `make batch`

| Step | Expected |
|---|---|
| `generate-data` | 6 Parquet files in `data/generated/` plus `manifest.json` |
| `bootstrap-bronze` | `PASS: bronze.<entity>_raw schema/partition/metadata` × 6 |
| `batch-bronze-docker` | first run appends; rerun reports `"status": "skipped"` (idempotent per batch ID) |
| `verify-bronze` | `PASS patients: rows=1000`, `encounters: rows=3094`, `lab_results: 12839`, `medications: 6149`, `vitals: 9261`, `bed_events: 3938` |
| `batch-silver-docker` | valid / quarantine: encounters 3058 / 5, lab_results 12719 / 12, medications 6139 / 10, vitals 9248 / 13, bed_events 3934 / 4 |
| `gold-docker` | `hospital_hourly_metrics` 14343, `department_daily_metrics` 2373, `lab_daily_metrics` 3258, `icu_occupancy` 14343 rows |

Talking points: Bronze keeps raw rows plus `_batch_id`/`_ingested_at`; Silver
collapses exact duplicates, canonicalizes department aliases and quarantines
bad rows with reason codes instead of dropping them; Gold is a full,
deterministic recomputation at `GOLD_AS_OF`.

### 4. Historical analytics — `make query`

Runs the 11 statements in `sql/trino/queries.sql` and ends with
`11/11 queries returned rows`; any SQL error or empty result exits non-zero.
Highlights to show:

- query 7 — average length of stay by department (ICU ≈ 187 h);
- query 8 — abnormal lab rate by test (Glucose ≈ 0.278);
- query 9 — monthly ICU utilization against 20 configured beds (the
  2026-09 bucket is only the single cutoff hour at `GOLD_AS_OF`);
- query 10 — one patient's encounter history joined with lab results;
- query 11 — data-quality view: quarantined rows by entity and reason
  (`DISCHARGE_BEFORE_ADMISSION` 5 encounters → 39 child rows `UNKNOWN_ENCOUNTER`).

Ad-hoc: `docker compose exec trino trino` then e.g.
`SHOW TABLES FROM iceberg.silver;`.

### 5. Speed layer (bonus) — `STREAM=1`

`make kafka-up` creates `hospital.events` and `hospital.dlq` (3 partitions);
`make kafka-smoke` prints `PASS: 24 simulator events, patient keys/partitions, ...`;
`make produce RATE=100 DURATION=10` sends 1000 events including intentional
late/duplicate/invalid ones (counted in its progress log) and exits 0;
any failed delivery is logged as `delivery_failures` and exits 1. The Kafka →
Structured Streaming → Bronze reader in Compose is TV3's PR #17 and is shown
only once merged. The Week 4 throughput gate (1000 events/s for 10 min) is not
claimed yet.

### 6. Tests — `make test`

Unit and contract tests (pytest); expect `passed` with some `skipped`
(opt-in live integration tests). Spark tests pass in both `TZ=UTC` and
`TZ=Asia/Ho_Chi_Minh`.

## Reset between rehearsals

`make down` keeps data volumes, so a second `make batch` skips Bronze and
rewrites Silver/Gold with identical results. For a clean slate use
`docker compose down -v` (deletes MinIO, catalog and Kafka volumes).
