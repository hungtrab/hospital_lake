# sitemap.md — Where Things Live in HospitalLake

> Navigation map for every agent and teammate.
> Read together with [`AGENTS.md`](./AGENTS.md) (how to work), [`plan.md`](./plan.md) (what to build) and [`works.md`](./works.md) (who builds what, when).
>
> Rule: **put code where this map says it belongs.** If you create a new directory or a new shared contract (table, topic, env var, Makefile target), update this file in the same PR.

---

## 1. Current state

The repository has an **initial Week 1 batch generator** in `generators/batch_generator/generate.py`, with tests in `tests/unit/test_batch_generator.py`.
It writes deterministic CSV patients, encounters, and lab results. TV4's `scripts/trino_smoke.py` checks a known table's row count through the Trino CLI; `tests/unit/test_trino_smoke.py` tests its success/failure handling. Local MinIO and Trino deployment configurations exist; MinIO has a persistent volume and readiness check. MinIO now builds locally from pinned upstream source; readiness and authenticated S3 persistence across container recreation passed in the local Docker environment. Iceberg REST Catalog, bucket bootstrap, live integration, Parquet output and the full pipeline remain pending.
TV3's `spark/common/config.py` loads and validates shared Spark/Iceberg/MinIO environment settings; `tests/unit/test_spark_config.py` checks defaults, overrides, validation, and credential redaction. SparkSession and connector setup remain pending.
"Planned files" below are the target names from `plan.md` §4 — create them with those exact names when you implement them.

---

## 2. Top-level tree

```text
.
├── AGENTS.md            # agent operating contract (mandatory)
├── plan.md              # architecture / scope / milestones (source of truth)
├── works.md             # 9-week TV1–TV4 allocation + weekly gates
├── sitemap.md           # this file
├── README.md            # project overview
├── .gitignore
│
├── docs/                # architecture, data model, demo script, benchmark report
├── data/                # local datasets (git-ignored except samples/)
├── generators/          # synthetic batch data + realtime event producer
├── spark/               # Spark batch + Structured Streaming jobs
├── sql/                 # Iceberg DDL, Trino queries, ClickHouse schema
├── dashboard/           # Grafana dashboards + provisioning (as code)
├── infra/               # docker-compose assets, K8s manifests, Helm values
├── tests/               # unit / integration / e2e
└── scripts/             # bootstrap, reset, seed, demo, benchmark
```

Root command/config files (Milestone 0, `plan.md` §22):

| File | Purpose |
|---|---|
| `Makefile` | Implemented `help`, `setup`, `config`, `up`, `down`, `minio-up`, `minio-stop`, `smoke-minio`, `test-minio`, `test`, `trino-config`, `trino-up`, `trino-stop`, `trino-smoke`; other pipeline targets await implementation. |
| `.env.example` | Implemented Trino/MinIO/catalog settings plus `SPARK_MASTER` and `ICEBERG_WAREHOUSE`; synthetic-data local credentials shared by MinIO/Trino; `MINIO_IMAGE`, `MINIO_API_PORT`, `MINIO_CONSOLE_PORT` configure the local storage service. Endpoints use Compose service names. Makefile supports both Compose CLI forms via `COMPOSE`; the existing Python Trino smoke client requires `docker compose` in container mode. Never commit `.env`. |
| `docker-compose.yml` | MinIO and Trino share the `hospital-lake` network; `minio-data` persists storage. Catalog and remaining services await their assigned tasks. |

---

## 3. Directory reference

Legend — **Layer**: Lambda layer the code belongs to (Batch / Speed / Serving / Infra / Shared). **TV**: teammate who touches it most per `works.md` §8 (not exclusive ownership). **Wk**: week it is first implemented.

### 3.1 `docs/`

| Planned file | Content | Wk |
|---|---|---|
| `architecture.md` | Diagram, component responsibilities, batch path, stream path, Lambda mapping | 1+ |
| `data-model.md` | Entities, keys, relationships, Bronze/Silver/Gold tables | 1–3 |
| `demo-script.md` | Exact demo commands (`plan.md` §28) | 9 |
| `benchmark.md` | Raw measurements + interpretation | 8 |

Docs describe **what exists**, never planned-but-unbuilt features (`AGENTS.md` §38).

### 3.2 `data/` — Shared

| Path | Content | In git? |
|---|---|---|
| `raw/` | Raw input files (e.g. optional MIMIC extracts) | No |
| `generated/` | Output of `generators/batch_generator` (Parquet) | No |
| `samples/` | Tiny fixture files for tests/docs (incl. intentional bad/duplicate rows) | Yes |

### 3.3 `generators/` — Batch + Speed input · TV1 (TV2 for load profiles)

| Path | Planned files | Responsibility | Wk |
|---|---|---|---|
| `batch_generator/` | `generate.py`, `config.yaml` | Synthetic patients, encounters, labs, medications, vitals, bed_events → Parquet. Deterministic seed, configurable scale (1k → 1M). | 1–2 |
| `stream_generator/` | `producer.py`, `event_models.py`, `config.yaml` | Kafka producer with common event envelope, `--rate`, key = `patient_id`; injects late / duplicate / invalid events. | 4, 7 |

### 3.4 `spark/` — Batch + Speed processing

| Path | Planned files | Responsibility | TV | Wk |
|---|---|---|---|---|
| `common/` | `config.py`, `schemas.py`, `iceberg.py` | Env-based config, SparkSession + Iceberg/MinIO setup, shared schemas, data-quality/quarantine helpers. **Reuse, don't duplicate.** | TV3 | 1, 3 |
| `batch/` | `ingest_bronze.py`, `bronze_to_silver.py`, `silver_to_gold.py`, `main.py` | raw Parquet → Bronze → Silver → Gold. Idempotent, logs input/output/invalid/duplicate rows + duration. | TV3 (Bronze), TV1/TV2 (Silver), TV4 (Gold) | 2–3 |
| `streaming/` | `kafka_to_bronze.py`, `realtime_aggregates.py`, `main.py` | Kafka → validate → DLQ → dedup by `event_id` → watermark → Iceberg Bronze + realtime aggregates → ClickHouse. | TV3, TV4 (sink/DLQ), TV1/TV2 (aggregates) | 4–5 |

### 3.5 `sql/` — Lakehouse DDL + Serving queries

| Path | Planned files | Responsibility | TV | Wk |
|---|---|---|---|---|
| `iceberg/` | `create_bronze_tables.sql`, `create_silver_tables.sql`, `create_gold_tables.sql` | Iceberg DDL + partitioning (`plan.md` §20) | TV2 | 2–3 |
| `trino/` | `queries.sql` | Historical analytical queries (`plan.md` §12.1) | TV4 | 1–3 |
| `clickhouse/` | `schema.sql`, `views.sql` | Realtime serving tables: `realtime_hospital_state`, `realtime_department_metrics`, `realtime_lab_metrics` | TV3/TV4 | 5 |

### 3.6 `dashboard/grafana/` — Serving · TV4

| Path | Content | Wk |
|---|---|---|
| `provisioning/` | Datasources (Trino, ClickHouse, Prometheus) + dashboard providers | 5 |
| `dashboards/` | JSON: Hospital Overview, Realtime Operations, Historical Analytics, Infrastructure | 5, 7 |

No manually-clicked dashboards; everything provisioned from these files.

### 3.7 `infra/` — Infra

| Path | Content | TV | Wk |
|---|---|---|---|
| `docker/` | Dockerfiles, service configs mounted by `docker-compose.yml` (Trino catalog, MinIO bootstrap, Kafka topic init, …) | TV2 (TV4 for Trino) | 1, 4 |
| `docker/minio/Dockerfile` | Build pinned MinIO upstream source locally with Go; runtime includes curl for health and S3 checks | TV2 | 1 |
| `docker/trino/`, `docker/trino/catalog/` | Implemented single-node Trino config, 1 GiB JVM heap, Iceberg REST/S3 catalog with environment-provided credentials | TV4 | 1 |
| `k8s/namespace.yaml` | Namespace `hospital-lake` | any | 6 |
| `k8s/minio/`, `k8s/iceberg/`, `k8s/trino/` | Storage + catalog + query engine manifests, PVCs | TV1 | 6 |
| `k8s/kafka/` | Strimzi `Kafka` + `KafkaTopic` resources, producer Deployment | TV2 | 6 |
| `k8s/spark/` | Spark Operator + `SparkApplication` (batch & streaming) | TV3 | 6 |
| `k8s/clickhouse/`, `k8s/grafana/`, `k8s/prometheus/` | Serving + observability manifests (= "observability" in `works.md`) | TV4 | 6–7 |
| `helm/` | Helm values for operators/charts (Strimzi, Spark Operator, Prometheus, …) | TV2/TV3/TV4 | 6 |

### 3.8 `tests/` — Shared

| Path | Content | Wk |
|---|---|---|
| `unit/` | Generators, schemas, normalization, validation, aggregation logic (pytest) | 1+ |
| `integration/` | Implemented `test_minio.py`: live authenticated S3 write/read, container recreation and persistence check with temporary fixture cleanup. Planned Spark → Iceberg, Trino → Iceberg, Kafka → Spark, Spark → ClickHouse | 1+ |
| `e2e/` | Event → Kafka → Streaming → ClickHouse/Trino assertions; full regression suite | 5, 8 |

### 3.9 `scripts/` — Automation

| Planned file | Purpose | Wk |
|---|---|---|
| `bootstrap.sh` | One-time local setup | 1 |
| `trino_smoke.py` | TV4: row-count check; `--check-stack` probes engine/catalog/count/data, `--compose` uses container CLI. Uses `TRINO_HOST`/`TRINO_PORT` or `--server`; returns nonzero on failure. | 1 |
| `reset.sh` | Dev reset (not a substitute for idempotency) | 1+ |
| `seed.sh` | Generate + load demo dataset | 2 |
| `demo.sh` | Scripted final demo | 9 |
| `benchmark.sh` | Batch + streaming benchmarks → CSV/JSON | 8 |

Scale/failure-injection scripts (Week 7) also go here.

---

## 4. Shared contracts (search before changing — `AGENTS.md` §34)

| Contract | Canonical names | Defined in |
|---|---|---|
| Iceberg namespaces | `bronze`, `silver`, `gold` | `sql/iceberg/` |
| Bronze tables | `bronze.{patients,encounters,lab_results,medications,vitals,bed_events}_raw` | `plan.md` §7.1 |
| Silver tables | `silver.{patients,encounters,lab_results,medications,vitals,bed_events}` | `plan.md` §7.2 |
| Gold tables | `gold.hospital_hourly_metrics`, `gold.department_daily_metrics`, `gold.icu_occupancy`, `gold.lab_daily_metrics`, `gold.patient_timeline`, `gold.hospital_current_state` | `plan.md` §7.3 |
| Kafka topics | `hospital.{patient,encounter,lab,medication,vitals,bed,dlq}` | `plan.md` §6 |
| Event envelope | `event_id`, `event_type`, `event_time`, `source`, `version`, `payload` | `plan.md` §6 |
| ClickHouse tables | `realtime_hospital_state`, `realtime_department_metrics`, `realtime_lab_metrics` | `plan.md` §12.2 |
| Env vars | `MINIO_*`, `KAFKA_BOOTSTRAP_SERVERS`, `CLICKHOUSE_*`, `TRINO_*`, `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `SPARK_MASTER` | `plan.md` §26 → `.env.example`; Spark loader in `spark/common/config.py` |
| K8s namespace | `hospital-lake` | `plan.md` §14 |

---

## 5. Data flow → directory map

```text
BATCH LAYER
generators/batch_generator ──► data/generated ──► spark/batch/ingest_bronze
    ──► bronze.* ──► spark/batch/bronze_to_silver ──► silver.*
    ──► spark/batch/silver_to_gold ──► gold.* ──► sql/trino ──► Grafana

SPEED LAYER
generators/stream_generator ──► Kafka hospital.* ──► spark/streaming/kafka_to_bronze
    ──► bronze.* (+ hospital.dlq) ──► spark/streaming/realtime_aggregates
    ──► ClickHouse (sql/clickhouse) ──► Grafana

SERVING         Trino (historical) + ClickHouse (realtime) ──► dashboard/grafana
INFRA           infra/docker (local first) ──► infra/k8s + infra/helm (Week 6+)
```

---

## 6. Where do I put …?

| I am writing … | Put it in |
|---|---|
| A new Spark helper used by >1 job | `spark/common/` |
| A new Kafka event type | `generators/stream_generator/event_models.py` + parser in `spark/streaming/` + `plan.md` §6 list |
| A new Gold table | `sql/iceberg/create_gold_tables.sql` + `spark/batch/silver_to_gold.py` + consumer (query/dashboard) |
| A dashboard panel | `dashboard/grafana/dashboards/*.json` |
| A config value | `.env.example` + `spark/common/config.py` (never hardcode) |
| A one-off debug script | Don't commit it — or make it reusable in `scripts/` |
| A new top-level command | `Makefile` target |
