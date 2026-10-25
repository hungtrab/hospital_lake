# sitemap.md — Where Things Live in HospitalLake

> Navigation map for every agent and teammate.
> Read together with [`AGENTS.md`](./AGENTS.md) (how to work), [`plan.md`](./plan.md) (what to build) and [`works.md`](./works.md) (who builds what, when).
>
> Rule: **put code where this map says it belongs.** If you create a new directory or a new shared contract (table, topic, env var, Makefile target), update this file in the same PR.

---

## 1. Current state

TV1's Week 1 generator has shared Arrow schemas (`schemas.py`), YAML config,
Parquet/CSV output, UTC timestamps, stable IDs and raw defects tracked in a manifest.
`make generate-data` defaults to Parquet; `FORMAT=csv` uses the same engine.
The CSV `generate(output_dir, patients, seed)` compatibility API emits clean data;
encounter/lab counts now vary, so consumers must use manifest counts. Use separate
directories for CSV and Parquet: the Bronze verifier requires exactly one source
format per entity. Generator tests cover both formats and source-ID compatibility.
TV2 provides MinIO source build, safe warehouse bucket bootstrap, persistent
SQLite-backed Iceberg REST Catalog, Trino dependencies and a Docker Spark smoke
profile. `week1-smoke` and `week1-recovery` check exact Spark/Trino rows and
container recreation; the live Week 1 recovery gate passed locally on 2026-10-12.
TV4 smoke checks remain available.
TV3's `spark/common/config.py` loads and validates shared Spark/Iceberg/MinIO environment settings; `tests/unit/test_spark_config.py` checks defaults, overrides, validation, and credential redaction. The Week 1 SparkSession setup is now shared in `spark/common/iceberg.py` and reused by fixtures/bootstrap; batch ingestion is implemented in `spark/batch/ingest_bronze.py`, with metadata and serial batch-ID retries. Unit contracts, structured failure logging (`tests/unit/test_bronze_logging.py`), session compatibility checks (`tests/unit/test_iceberg_session.py`) and real local Spark/Iceberg integration tests cover preservation, retry and failure behavior.
TV4's Week 3 `spark/batch/silver_to_gold.py` computes four Gold metrics from Silver; `scripts/run_gold.py` submits it with shared Spark configuration. Exact-value local Spark tests exist; `make query` runs 11 Silver/Gold Trino queries (`scripts/run_queries.py`) and fails on errors or empty results; `scripts/checkpoint_demo.sh` + `docs/demo-script.md` script the Checkpoint 1 demo. All six Silver entities are implemented; `make gold-docker` (Compose service `gold`) rebuilds Gold and is the last step of `make batch`.
TV4's Week 4 `spark/streaming/bronze_sink.py` writes validated Kafka micro-batches into Iceberg Bronze/quarantine; `scripts/stream_bronze_probe.py` checks progress over five minutes. TV3's reader runs in Compose (`make stream-bronze-docker`, internal listener `kafka:29092`, persistent checkpoint volume) and TV1's producer feeds it; live arrival and restart without lost or duplicate Kafka offsets were verified at 10–20 events/s, but the Week 4 throughput gate (1000 events/s, 10 min) has not been run.
TV2 Week 2 provides six Bronze DDL contracts, daily hidden partitions and an idempotent, drift-checking table bootstrap. `bronze-smoke` validates isolated Parquet fixtures, S3 metadata, Trino compatibility, raw preservation and reruns. Production Bronze ingestion is available through `make batch-bronze`; the live Week 2 integration gate passed on 2026-10-22 (see `next_step.md`).
"Planned files" below are the target names from `plan.md` §4 — create them with those exact names when you implement them.

---

TV2 Week 4 adds optional single-node KRaft Kafka, persistent data, safe topic
bootstrap and host/internal keyed transport/recreation checks. `make kafka-up`
starts only the Kafka profile. Spark Compose integration remains TV3 T3.2.

## 2. Top-level tree

```text
.
├── AGENTS.md            # agent operating contract (mandatory)
├── plan.md              # architecture / scope / milestones (source of truth)
├── works.md             # 9-week TV1–TV4 allocation + weekly gates
├── sitemap.md           # this file
├── next_step.md         # current sprint plan: per-TV tasks until Checkpoint 1 (26/10)
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
| `requirements-spark.txt` | Optional pinned PySpark 4.0.2 host runtime plus application dependencies; local Spark/Iceberg tests require Java 17+. `batch-bronze-docker` uses the existing Compose Spark image instead. |
| `requirements.txt`, `pyproject.toml` | Generator dependencies and pytest discovery for both pytest/unittest suites. |
| `Makefile` | `batch` runs the batch path end to end (generate → Bronze → verify → Silver → Gold via `gold-docker`); `batch-silver(-docker)` rebuilds Silver. Includes `batch-bronze` and `test-bronze` for ingestion/local integration. Week 2: `bootstrap-bronze`, `bronze-smoke`. Week 1 commands: `minio-up`, `minio-stop`, `bootstrap-bucket`, `test-minio`, `week1-smoke`, `week1-recovery`. `setup` installs dependencies; `generate-data` writes configurable Parquet/CSV; `test` runs both styles with pytest. `verify-generated` checks Parquet source contracts; `validate-generator` repeats 10k/100k generation and records deterministic hashes. Includes `verify-bronze`, `gold` (host Spark) and `gold-docker` (Compose) for TV4 batch validation and Gold, plus local and Trino targets. |
| `.env.example` | Implemented Trino/MinIO/catalog settings plus `SPARK_MASTER` and `ICEBERG_WAREHOUSE`; synthetic-data local credentials shared by MinIO/Trino; `MINIO_IMAGE`, `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `ICEBERG_REST_PORT` configure the local storage service. Endpoints use Compose service names. Makefile supports both Compose CLI forms via `COMPOSE`; Trino clients accept the `COMPOSE` override, which Make propagates. Never commit `.env`. |
| `docker-compose.yml` | Main provisions MinIO, `minio-init`, Iceberg REST, Trino and optional Spark/Bronze bootstrap/smoke profiles on `hospital-lake`; the TV3 branch adds a manually-run stream reader profile. `minio-data` persists objects, `iceberg-catalog` persists SQLite metadata, `spark-ivy` caches fixture connectors, and `stream-state` persists the streaming connector cache/checkpoint. The stream profile connects to the Kafka network listener supplied by TV2. |

---

## 3. Directory reference

Legend — **Layer**: Lambda layer the code belongs to (Batch / Speed / Serving / Infra / Shared). **TV**: teammate who touches it most per `works.md` §8 (not exclusive ownership). **Wk**: week it is first implemented.

### 3.1 `docs/`

| Planned file | Content | Wk |
|---|---|---|
| `architecture.md` | Lambda batch/speed/serving topology, component status, current Bronze/Silver/stream paths, repository map, and planned Kubernetes deployment; distinguishes runnable pieces from pending integration gates | 1–6 |
| `data-model.md` | Implemented six-entity source contracts, migration, raw defect semantics and local 10k/100k measurements; Bronze and all six Silver contracts; Gold checkpoint integration remains pending | 1–3 |
| `demo-script.md` | Implemented for Checkpoint 1: demo steps, exact commands and measured expected output; extended to the final demo (`plan.md` §28) | 4, 9 |
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
| `batch_generator/` | `generate.py`, `config.yaml`, `schemas.py` | Synthetic patients, encounters, labs, medications, vitals, bed_events → Parquet. Deterministic seed, configurable scale (1k → 1M). | 1–2 |
| `stream_generator/` | `producer.py`, `event_models.py`, `config.yaml` | **Implemented (TV1, W4).** Deterministic journey simulator (no patient has two open encounters) + Kafka producer: shared envelope accepted by `spark/streaming/kafka_reader.py`, `--rate`, key = `patient_id`, configurable late (> 10 min watermark) / duplicate / invalid events. `make produce`, `make produce-dry-run`. Uses `kafka-python`. | 4, 7 |

Batch source contract: `patients`, `encounters`, `lab_results` files use the
fields in `plan.md` §5, with typed UTC timestamps in Parquet and ISO UTC strings
in CSV; IDs are P/E/L-prefixed zero-padded strings. `_manifest.json` contains
`config`, `format`, `row_counts`, `injected_defects`. Week 2 `medications` is implemented with fields from `plan.md` §5 and
`M<encounter-number>-<order-number>` IDs; count is configured by
`medications_per_encounter`. `vitals` uses `V<encounter-number>-<sample-number>`
IDs and `vitals_per_encounter` counts, with canonical bpm/percent/mmHg/Celsius
units and observations inside clean encounters. `bed_events` uses `B<encounter-number>-<event-number>`
IDs and assigned/transferred/released journeys for non-outpatient encounters.
Transfers name the new bed and close the prior assignment in the same department;
open encounters have no release. Bed IDs are synthetic per encounter, not a fixed
inventory. `bed_transfer_rate` controls transfers. All six source entities exist;
Scale validation is reproducible with `make validate-generator`; Bronze
ingestion is available via `make batch-bronze-docker`. CSV API clean fixtures and CLI raw fixtures share the same schema.

### 3.4 `spark/` — Batch + Speed processing

| Path | Planned files | Responsibility | TV | Wk |
|---|---|---|---|---|
| `common/` | `config.py`, `schemas.py`, `iceberg.py`, `bronze.py`, `data_quality.py`, `normalization.py` | Implemented env config, canonical department/encounter values (`normalization.py`), shared SparkSession, Bronze DDL loader/validation, and batch validation/quarantine metrics. Processing schemas remain planned. **Reuse, don't duplicate.** | TV3 | 1, 3 |
| `batch/` | `ingest_bronze.py`, `bronze_to_silver.py`, `silver_to_gold.py`, `main.py` | raw Parquet → Bronze → Silver → Gold. TV1's `bronze_to_silver.py` rebuilds Silver from all Bronze rows with an entity registry (all six entities, including TV2 clinical FK/value validation) plus `silver.quarantine`; `make batch-silver-docker`. TV4's `silver_to_gold.py` builds four historical Gold tables with a deterministic cutoff and full-table overwrite. | TV3 (Bronze), TV1/TV2 (Silver), TV4 (Gold) | 2–3 |
| `streaming/` | `kafka_reader.py`, `kafka_to_bronze.py`, `bronze_sink.py`, `realtime_aggregates.py`, `main.py` | `kafka_to_bronze.py` wires the envelope reader to TV4's idempotent raw-byte Bronze/quarantine sink. `kafka_reader.py` retains bad events for quarantine and provides valid-only event-time dedup with a watermark for downstream consumers. | TV3, TV4 (sink/quarantine), TV1/TV2 (aggregates) | 4–5 |

### 3.5 `sql/` — Lakehouse DDL + Serving queries

| Path | Planned files | Responsibility | TV | Wk |
|---|---|---|---|---|
| `iceberg/` | `create_bronze_tables.sql`, `create_stream_bronze_tables.sql`, `create_silver_tables.sql`, `create_gold_tables.sql` | TV2 six Bronze definitions; TV4 two stream Bronze/quarantine and four Gold definitions; TV1 Silver patients/encounters/quarantine plus TV2 lab_results/bed_events/medications/vitals | TV2/TV4 | 2–4 |
| `trino/` | `verify_bronze.sql`, `queries.sql` | Bronze ID checks and 11 historical Silver/Gold queries run by `make query` | TV4 | 1–3 |
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
| `docker/kafka/` | `bootstrap-topics.sh`: create/validate hospital.events and hospital.dlq without deleting or repartitioning | TV2 | 4 |
| `docker/minio/` | Pinned source Dockerfile; `bootstrap.sh` creates the warehouse bucket safely with authenticated S3 | TV2 | 1 |
| `docker/iceberg-rest/` | REST fixture 1.9.0 image wrapper gives non-root catalog user ownership of persistent `/var/lib/iceberg` | TV2 | 1 |
| `docker/trino/`, `docker/trino/catalog/` | Implemented single-node Trino config, 1 GiB JVM heap, Iceberg REST/S3 catalog with environment-provided credentials | TV4 | 1 |
| `docker-compose.yml` stream profile | Spark 3.5.5 Structured Streaming reader with Iceberg/Kafka connectors and persistent `/tmp` volume; invoked by `make stream-bronze-docker` once Kafka is available | TV3 | 4 |
| `k8s/namespace.yaml` | Namespace `hospital-lake` | any | 6 |
| `k8s/minio/`, `k8s/iceberg/`, `k8s/trino/` | Storage + catalog + query engine manifests, PVCs | TV1 | 6 |
| `k8s/kafka/` | Strimzi `Kafka` + `KafkaTopic` resources, producer Deployment | TV2 | 6 |
| `k8s/spark/` | Spark Operator + `SparkApplication` (batch & streaming) | TV3 | 6 |
| `k8s/clickhouse/`, `k8s/grafana/`, `k8s/prometheus/` | Serving + observability manifests (= "observability" in `works.md`) | TV4 | 6–7 |
| `helm/` | Helm values for operators/charts (Strimzi, Spark Operator, Prometheus, …) | TV2/TV3/TV4 | 6 |

### 3.8 `tests/` — Shared

| Path | Content | Wk |
|---|---|---|
| `unit/` | Generators, schemas, normalization, validation, aggregation logic (unittest); `test_gold_metrics.py` uses local Spark | 1+ |
| `integration/` | Bronze fixture, persistence and verifier checks; five-minute stream progress probe logic; opt-in local Spark/Iceberg stream sink test; live Kafka → Spark and Spark → ClickHouse when services are available | 1+ |
| `e2e/` | Event → Kafka → Streaming → ClickHouse/Trino assertions; full regression suite | 5, 8 |

### 3.9 `scripts/` — Automation

| Planned file | Purpose | Wk |
|---|---|---|
| `bootstrap.sh` | One-time local setup | 1 |
| `bootstrap_bronze.py` | TV2 Week 2: Spark job creating/validating six raw tables without rewriting data | 2 |
| `bronze_smoke.py` | Host orchestration for isolated Parquet/Spark/S3/Trino contract checks, raw defects, reruns and explicit fixture cleanup | 2 |
| `kafka_smoke.py` | TV2 T2.2: host/internal listener probes with patient keys/order, DLQ transport and optional broker recreation persistence; `kafka-smoke`, `kafka-recovery` | 4 |
| `lakehouse_smoke.py` | TV2 Week 1: service checks, repeated bucket bootstrap, exact Spark/Trino row checks, optional storage/catalog recreation and isolated fixture cleanup | 1 |
| `trino_smoke.py` | TV4: row-count check; `--check-stack` probes engine/catalog/count/data, `--compose` uses container CLI. Uses `TRINO_HOST`/`TRINO_PORT` or `--server`; returns nonzero on failure. | 1 |
| `verify_bronze.py` | TV4: compare CSV/Parquet source count and stable-ID checksum with one Bronze `_batch_id` through Trino; `make verify-bronze` | 2 |
| `run_gold.py` | TV4: submit Gold Spark job using shared Iceberg/MinIO settings; `make gold` | 3 |
| `run_queries.py` | Run every titled statement in `sql/trino/queries.sql` through Trino, print results, exit 1 on an error or empty result; `make query` | 3 |
| `checkpoint_demo.sh` | Checkpoint 1 demo: `make up` → `make batch` → `make query` → `make test` (`STREAM=1` adds Kafka + producer); see `docs/demo-script.md` | 4 |
| `stream_bronze_probe.py` | TV4: poll Trino for Bronze growth, quarantine and duplicate offsets over five minutes; `make stream-bronze-probe` | 4 |
| `reset.sh` | Dev reset (not a substitute for idempotency) | 1+ |
| `check_generator.py` | TV1 Week 2: actual six-entity schema/FK/count validation and same-seed scale reruns; `verify-generated`, `validate-generator`; JSON report in ignored `results/` | 2 |
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
| Streaming Bronze tables | `bronze.stream_events_raw`, `bronze.stream_events_quarantine` | `sql/iceberg/create_stream_bronze_tables.sql` |
| Silver tables | `silver.{patients,encounters,lab_results,medications,vitals,bed_events}` | `plan.md` §7.2 |
| Gold tables | `gold.hospital_hourly_metrics`, `gold.department_daily_metrics`, `gold.icu_occupancy`, `gold.lab_daily_metrics`, `gold.patient_timeline`, `gold.hospital_current_state` | `plan.md` §7.3 |
| Kafka topics | Implemented `hospital.events`, `hospital.dlq`; optional per-entity topics remain planned | `next_step.md` T2.2, `plan.md` §6 (single-event-topic alternative) |
| Event envelope | `event_id`, `event_type`, `event_time`, `source`, `version`, `payload` | `plan.md` §6 |
| ClickHouse tables | `realtime_hospital_state`, `realtime_department_metrics`, `realtime_lab_metrics` | `plan.md` §12.2 |
| Env vars | `MINIO_*`, `KAFKA_BOOTSTRAP_SERVERS`, `STREAM_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS`, `KAFKA_STARTING_OFFSETS`, `STREAM_CHECKPOINT_LOCATION[_DOCKER]`, `CLICKHOUSE_*`, `TRINO_*`, `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `SPARK_MASTER`, `GOLD_AS_OF`, `GOLD_BED_CAPACITIES` | `plan.md` §26 → `.env.example`; Compose stream reader in `docker-compose.yml`; Spark loader in `spark/common/config.py`; Gold job in `spark/batch/silver_to_gold.py` |
| Bronze command | `make batch-bronze BATCH_ID=... [INPUT_DIR=data/generated] [ENTITIES="patients encounters ..."]`; alternate `batch-bronze-docker` uses Compose `bronze-ingest` and read-only `BRONZE_INPUT_DIR` mount. Immutable local Parquet, precreated matching Iceberg tables, one writer per table. Submission tests: `tests/unit/test_bronze_submit.py`. | `spark/batch/ingest_bronze.py`, `docker-compose.yml`, `Makefile`, `README.md` |
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

Clinical Silver tests: `tests/unit/test_silver_clinical.py` cover FK propagation,
raw quarantine, interval boundaries/nonfinite values, domain normalization,
duplicates, lineage, reruns and all four DDL output schemas.

Kafka local commands: `kafka-up`, `kafka-topics`, `kafka-stop`, `kafka-smoke`,
`kafka-recovery`. Compose adds optional `kafka`/`kafka-init` and `kafka-data` volume.
Host port/partition defaults are in `.env.example`; `tests/unit/test_kafka_smoke.py`
checks probe failure/order behavior, partition input validation and profile isolation.
