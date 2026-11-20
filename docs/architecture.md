# HospitalLake Architecture

HospitalLake follows a Lambda Architecture: Spark batch jobs build the
historical Lakehouse, while Kafka and Spark Structured Streaming provide a
low-latency path. Both paths use MinIO + Iceberg as the durable data layer.
Trino serves historical SQL; ClickHouse is reserved for realtime dashboard
queries. Grafana is the shared visualization layer for the local stack.

```text
                         HOSPITAL DATA (synthetic)
                         /                       \
            historical Parquet                 event simulator
                     |                                |
                     v                                v
               Spark batch                         Kafka
          ingest → Bronze                         hospital.events
                     |                                |
                     v                                v
                  Silver                  Spark Structured Streaming
                     |                    raw/quarantine + validated state
                     v                                |
                    Gold                              |
                     |                                |
                     +---------------+----------------+
                                     v
                      MinIO object storage + Iceberg
                          /                       \
                         v                         v
                Trino historical SQL       ClickHouse realtime serving
                          \                       /
                           +------ Grafana -------+

  Deployment target: Kubernetes + Spark Operator + Strimzi + Prometheus
```

The diagram shows the intended product boundary. The table below records the
current executable state; a box in the diagram is not evidence that its full
integration gate has passed.

## Component status

| Component | Lambda layer | Code / purpose | Status and reproducible entry point |
|---|---|---|---|
| Synthetic batch generator | Input | Six synthetic entities to Parquet/CSV | Implemented: `make generate-data`; validation: `make verify-generated` and `make validate-generator`. |
| MinIO + Iceberg REST Catalog | Shared storage | S3 objects and Iceberg metadata | Local stack implemented: `make up`; persistent `minio-data` and `iceberg-catalog` volumes. |
| Spark Bronze ingestion | Batch | Raw Parquet plus ingestion metadata to `bronze.*` | Implemented: `make bootstrap-bronze`, then `make batch-bronze-docker BATCH_ID=...`; verifier: `make verify-bronze BATCH_ID=...`. |
| Bronze → Silver | Batch | Normalize and validate all six entities; quarantine rejected rows | Implemented for patients, encounters, labs, medications, vitals, and bed events: `make batch-silver-docker`. |
| Silver → Gold | Batch | Historical hospital, department, lab, and ICU metrics | Implemented: `make gold-docker` (Compose service `gold`), also the last step of `make batch`; local Spark tests check exact values. |
| Trino | Serving | SQL over Iceberg tables | Implemented: `make trino-up`; `make query` runs the 11 Silver/Gold queries in `sql/trino/queries.sql` and fails on errors or empty results. |
| Event simulator | Speed input | Envelope events keyed by patient ID, with configurable defects | Implemented: `make produce-dry-run`; `make produce KAFKA_BOOTSTRAP_SERVERS=localhost:9092` publishes to the local broker. |
| Kafka broker/topics | Speed transport | `hospital.events` and `hospital.dlq` | Implemented: single-node KRaft broker (Compose profile `kafka`): `make kafka-up` creates both topics; `make kafka-smoke`, `make kafka-recovery`. |
| Spark Kafka reader | Speed processing | Decode/validate envelopes, preserve invalid events for quarantine, provide event-time dedup with a 10-minute default watermark | Reader code and local Spark unit tests exist. Host entry point: `make stream-bronze` with Kafka and Iceberg reachable. |
| Stream Bronze/quarantine sink | Shared storage | Preserve Kafka bytes by topic/partition/offset | MERGE prunes by batch topic/offset ranges and safely retries. Raw preservation and restart tests pass. Historical 25/10 Bronze-only gate: 600,000 messages at a target 1,000/s; this does not certify the expanded realtime stack. Current full-stack long-run validation is tracked in `progress.md`. |
| ClickHouse + realtime aggregates | Serving | Published aggregate snapshots from Iceberg event/state processing | Local known-event/restart gate passed: `make realtime-up`, `make realtime-smoke`, `make realtime-recovery`; see `docs/realtime.md`. |
| Grafana dashboards | Visualization | Hospital overview, realtime operations, historical analytics and pipeline monitoring | Provisioned from code; historical and eight realtime panels have queried actual pipeline outputs. |
| Kubernetes, Spark Operator, Strimzi, Prometheus | Infrastructure | Deployment, persistence, scaling, recovery, observability | Prometheus scrapes the local stream. Isolated kind has operators, MinIO and REST; full core integration and scaling gates are pending. See `progress.md`. |

## Batch and speed paths

The batch path preserves source rows in Bronze, then rebuilds normalized Silver
tables and downstream Gold metrics:

```text
generators/batch_generator
  → data/generated/*.parquet
  → spark/batch/ingest_bronze.py
  → Iceberg bronze.*
  → spark/batch/bronze_to_silver.py
  → Iceberg silver.* (+ silver.quarantine)
  → spark/batch/silver_to_gold.py
  → Iceberg gold.*
  → Trino SQL
```

`make batch` generates data, bootstraps and ingests Bronze, verifies the
Bronze batch, rebuilds all six registered Silver entities and then Gold
(`make gold-docker`). `make query` then runs the historical Trino queries, which
completes the Bronze → Silver → Gold → Trino checkpoint path.

The speed path stores each Kafka record in raw Bronze or quarantine, keyed for
retry safety by Kafka coordinates. Event ID deduplication is a valid-only view
for downstream stateful consumers; it does not remove duplicate source messages
from raw Bronze:

```text
generators/stream_generator/producer.py
  → Kafka hospital.events (key = patient_id)
  → spark/streaming/kafka_reader.py
  → spark/streaming/bronze_sink.py
  → Iceberg bronze.stream_events_raw / bronze.stream_events_quarantine
```

The current reader uses event time, validates the common envelope, and attaches
a watermark when building the deduplicated streaming view. The Bronze sink
retains malformed bytes and a reason string. The Compose reader uses the
internal Kafka address (`STREAM_KAFKA_BOOTSTRAP_SERVERS`, default
`kafka:29092`) and a persistent named Docker volume for Spark's Ivy cache and
checkpoint data. Start Kafka and the Lakehouse services first, then run
`make stream-bronze-docker`. `make stream-bronze-probe` checks that Bronze grows
for five minutes without duplicate Kafka coordinates; this and a reader
kill/restart check passed locally at 10–20 events/s. `make stream-gate`
(`scripts/stream_gate.py`) is the Week 4 gate: it runs the producer at
`GATE_RATE` for `GATE_DURATION` seconds, samples Kafka and Bronze, and passes
only if every offset produced during the run is in Bronze exactly once after
the reader catches up; the summary goes to `results/stream-gate.json`.

The realtime application runs a separate valid-only query from the same Kafka
topic. It deduplicates event IDs within the event-time watermark, maintains an
Iceberg Silver event ledger and encounter state, and publishes only aggregates
to ClickHouse. A publication marker hides incomplete snapshots from dashboards.
Iceberg remains the durable source of truth. This is a single-writer local
implementation, not a claim of end-to-end exactly-once delivery.

```text
Kafka → valid envelope/payload + event-time dedup
      → Iceberg Silver event ledger / encounter state
      → ClickHouse aggregate snapshot + publication marker
      → Grafana realtime panels
```

## Repository map

| Directory | Responsibility |
|---|---|
| `generators/batch_generator/` | Historical synthetic Parquet/CSV. |
| `generators/stream_generator/` | Realtime event models and Kafka producer. |
| `spark/common/` | Spark configuration/session, Bronze contracts, validation and normalization helpers. |
| `spark/batch/` | Bronze ingestion, Bronze → Silver transforms, Silver → Gold metrics. |
| `spark/streaming/` | Kafka source parsing, event validation/deduplication, Iceberg Bronze sink. |
| `sql/iceberg/` | Source-controlled Bronze, Silver, Gold and stream Bronze table definitions. |
| `sql/trino/` | Historical verification and analytical SQL examples. |
| `docker-compose.yml`, `infra/docker/` | Local MinIO, Iceberg REST, Trino, Kafka (profile `kafka`) and Spark job configuration. |
| `infra/k8s/`, `helm/` | Planned later Kubernetes deployment assets. |
| `tests/unit/`, `tests/integration/`, `tests/e2e/` | Local contracts, service integrations, and eventual end-to-end checks. |
| `dashboard/grafana/`, `sql/clickhouse/` | Planned serving and visualization assets. |

## Local Lakehouse infrastructure

The current Compose Lakehouse uses MinIO release
`RELEASE.2025-04-22T22-12-26Z`, the Apache Iceberg REST fixture `1.9.0`, Trino
`483`, and Spark `3.5.5` for Compose jobs. MinIO stores table objects in
`minio-data`; the SQLite-backed REST catalog stores namespace and table
registrations in `iceberg-catalog`. The normal `make down` keeps these volumes.
The smoke Spark job's `spark-ivy` volume caches its Maven dependencies. The
streaming service uses a separate `stream-state` volume so its checkpoint
survives removal and recreation of the one-off Spark container.

All containers communicate over the `hospital-lake` network. MinIO, Iceberg
REST, and Trino publish localhost-only ports. Compose waits for MinIO health,
bucket initialization, REST health, and Trino readiness. The optional stream
profile also waits for the Iceberg REST service; Kafka is not a Compose
dependency of the reader, so start it with `make kafka-up` (which waits for
broker health and creates the topics) before `make stream-bronze-docker`.

## Reproducible checks

With Docker available, `.env` copied from `.env.example`, and dependencies
installed:

```bash
make up
make batch
make test
make query
make kafka-up
```

Then start the stream reader in one terminal:

```bash
make stream-bronze-docker
```

While it is running, publish events in a second terminal and run the progress
probe in a third:

```bash
make produce RATE=10 DURATION=300
# Separate terminal:
make stream-bronze-probe
```

Keep the producer active for the five-minute probe window; the probe fails if
Bronze makes no progress for 60 seconds.

`make test` covers implemented local behavior; it does not run the live Kafka,
Iceberg or Gold paths and says nothing about the ClickHouse, Grafana or
Kubernetes gates, which are still planned. `make stream-bronze-probe` is the
five-minute live check of the running stream. For planned work and checkpoint status, see
[`next_step.md`](../next_step.md), [`works.md`](../works.md), and
[`plan.md`](../plan.md).

The separate TV2 checkpointed clinical runner is available through `make stream-clinical`. The standalone TV4 manifests have their own deployment and validation procedure. [Clinical metrics](realtime-clinical.md) · [TV4 observability](../infra/k8s/observability/README.md)
