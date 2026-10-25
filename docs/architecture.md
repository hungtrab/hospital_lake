# HospitalLake Architecture

HospitalLake follows a Lambda Architecture: Spark batch jobs build the
historical Lakehouse, while Kafka and Spark Structured Streaming provide a
low-latency path. Both paths use MinIO + Iceberg as the durable data layer.
Trino serves historical SQL; ClickHouse is reserved for realtime dashboard
queries. Grafana is the planned shared visualization layer.

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
                     |                    validate → raw/quarantine sink
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

  Deployment target (planned): Kubernetes + Spark Operator + Strimzi + Prometheus
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
| Bronze → Silver | Batch | Normalize and validate patients/encounters; quarantine rejected rows | Implemented for patients and encounters: `make batch-silver-docker SILVER_ENTITIES="patients encounters"`. Clinical entities await TV2's registry additions. |
| Silver → Gold | Batch | Historical hospital, department, lab, and ICU metrics | Job and local Spark tests exist; complete execution awaits the required clinical Silver tables and the Compose Gold submission path. |
| Trino | Serving | SQL over Iceberg tables | Local service implemented: `make trino-up`; SQL examples are in `sql/trino/queries.sql`. Historical outputs depend on completed Silver/Gold inputs. |
| Event simulator | Speed input | Envelope events keyed by patient ID, with configurable defects | Implemented: `make produce-dry-run`; Kafka publishing uses `make produce` when a reachable Kafka broker is available. |
| Kafka broker/topics | Speed transport | `hospital.events` and `hospital.dlq` | Local KRaft service is assigned to TV2 and is not present in the current `main`; run `make kafka-up` after that integration is merged. |
| Spark Kafka reader | Speed processing | Decode/validate envelopes, preserve invalid events for quarantine, provide event-time dedup with a 10-minute default watermark | Reader code and local Spark unit tests exist. Host entry point: `make stream-bronze` with Kafka and Iceberg reachable. |
| Stream Bronze/quarantine sink | Shared storage | Persist original Kafka bytes by topic/partition/offset | Idempotent Iceberg sink and integration tests exist. Compose entry point is added by the TV3 branch: `make stream-bronze-docker`; it requires TV2's Kafka listener and the Iceberg services. The branch has not yet passed a live Kafka-to-Iceberg run. |
| ClickHouse + realtime aggregates | Serving | Low-latency realtime metrics | Planned; no current runnable path. |
| Grafana dashboards | Visualization | Hospital overview, realtime operations, historical analytics | Planned; dashboard outputs must be backed by implemented serving tables. |
| Kubernetes, Spark Operator, Strimzi, Prometheus | Infrastructure | Deployment, persistence, scaling, recovery, observability | Planned for the later Kubernetes/observability milestones; no current cluster deployment is claimed. |

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

`make batch` currently generates data, bootstraps and ingests Bronze, verifies
the Bronze batch, and rebuilds the registered Silver entities. It does not yet
include Gold or the `make query` checkpoint target. The six-entity data model
exists, but the current Silver transform registry only covers patients and
encounters; the clinical tables must be integrated before claiming a complete
Bronze → Silver → Gold run.

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
`make stream-bronze-docker`. A fresh deployment still needs the live integration
check: valid events in raw Bronze, malformed events in quarantine, and no
duplicate Kafka coordinates after restart.

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
| `docker-compose.yml`, `infra/docker/` | Local MinIO, Iceberg REST, Trino and Spark job configuration; Kafka is supplied by the TV2 integration. |
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
profile also waits for the Iceberg REST service; Kafka readiness is provided by
TV2's Kafka stack and must be checked before starting the reader.

## Reproducible checks

With Docker available, `.env` copied from `.env.example`, and dependencies
installed:

```bash
make up
make batch
make test
make kafka-up                 # after TV2's Kafka service is merged
make produce RATE=10 DURATION=30
make stream-bronze-docker
make stream-bronze-probe
```

`make test` covers implemented local behavior; it does not imply that the live
Kafka, Iceberg, Gold, ClickHouse, Grafana, or Kubernetes gates have passed.
`make stream-bronze-probe` is a five-minute integration check once Kafka and
the stream are actually running. For planned work and checkpoint status, see
[`next_step.md`](../next_step.md), [`works.md`](../works.md), and
[`plan.md`](../plan.md).
