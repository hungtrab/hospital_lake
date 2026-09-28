# HospitalLake — Implementation Plan

> **Project goal:** Build a cloud-native hospital Data Lakehouse that unifies historical and real-time hospital data, supports both batch and streaming analytics, and runs end-to-end on Kubernetes.

---

## 0. Agent Operating Instructions

This document is intended to be executable as a development plan by a local coding agent.

### Working rules

1. **Implement incrementally.** Do not attempt the full platform in one pass.
2. **Every milestone must end in a runnable system.**
3. **Prefer boring, stable technology choices over unnecessary complexity.**
4. **Do not add AI/ML features until the complete data pipeline works.**
5. **Do not add extra infrastructure unless it has a clear purpose in the architecture.**
6. **All services must have health checks, logs, and reproducible startup instructions.**
7. **All infrastructure must be reproducible from code/configuration.**
8. **Use synthetic healthcare data for development by default.**
9. **MIMIC-IV integration is optional and must not block the core project.**
10. **Treat all healthcare data as de-identified/synthetic. Never hardcode real personal information.**

### Definition of done

The project is considered complete when a reviewer can:

1. Start the platform locally.
2. Generate historical hospital data.
3. Generate live hospital events.
4. Observe batch data landing in the lakehouse.
5. Observe streaming data flowing through Kafka.
6. Query Bronze, Silver, and Gold tables.
7. View historical and real-time metrics in Grafana.
8. Run the system on Kubernetes.
9. Demonstrate worker scaling.
10. Kill a processing pod and show recovery.
11. Reproduce benchmark results.

---

# 1. Project Summary

## 1.1 Name

**HospitalLake — Cloud-Native Hospital Data Lakehouse for Batch and Real-Time Analytics**

## 1.2 Problem statement

A hospital typically has data distributed across multiple operational systems:

- patient registration,
- admissions/discharges/transfers,
- laboratory systems,
- medication systems,
- ICU monitoring,
- bed management,
- departmental systems.

These systems produce both:

- **historical/batch data**, and
- **real-time operational events**.

The goal is to build a unified platform that:

- ingests data from multiple hospital-like sources,
- stores them in a Lakehouse,
- applies Bronze → Silver → Gold transformations,
- supports batch analytics,
- supports streaming analytics,
- exposes fast query interfaces,
- visualizes hospital operations,
- runs on Kubernetes.

## 1.3 Non-goals

The following are explicitly out of scope for the initial project:

- medical diagnosis,
- clinical decision support,
- production-grade HIPAA compliance,
- patient-facing application,
- real-world hospital integration,
- complex ML models,
- ECG signal classification,
- high-availability multi-region deployment.

Optional extensions may be added only after the core pipeline is complete.

---

# 2. High-Level Architecture

```text
                           DATA SOURCES

         Historical files / tables       Real-time event simulator
         MIMIC-IV / Synthea batch        Synthea-derived events
                    |                               |
                    | BATCH                         | STREAM
                    v                               v
               Apache Spark                    Apache Kafka
                    |                               |
                    |                        Spark Structured
                    |                           Streaming
                    |                               |
                    +---------------+---------------+
                                    |
                                    v
                    DATA LAKEHOUSE: MINIO + ICEBERG

                     +------------+------------+
                     |            |            |
                   BRONZE       SILVER        GOLD
                   raw          cleaned       analytics
                   data         unified       ready
                     |            |            |
                     +------------+------------+
                                    |
                        +-----------+-----------+
                        |                       |
                        v                       v
                      Trino                ClickHouse
                  historical SQL          realtime serving
                        \                       /
                         \                     /
                          +--------+----------+
                                   |
                                   v
                                Grafana

Infrastructure:
Kubernetes + Spark Operator + Kafka/Strimzi + Prometheus
```

---

# 3. Core Technology Choices

| Area | Technology | Purpose |
|---|---|---|
| Object storage | MinIO | S3-compatible storage for local/K8s environment |
| Lakehouse format | Apache Iceberg | Table format, schema evolution, snapshots |
| Batch processing | Apache Spark | Historical ingestion and transformation |
| Streaming | Kafka | Event transport |
| Stream processing | Spark Structured Streaming | Reuse Spark stack for stream processing |
| SQL query engine | Trino | Interactive SQL directly over Iceberg |
| Realtime serving | ClickHouse | Fast serving for live dashboard metrics |
| Dashboard | Grafana | Visual analytics |
| Container runtime | Docker | Local packaging |
| Orchestration | Kubernetes | Deployment, scaling, fault recovery |
| Spark K8s integration | Spark Operator | Manage Spark applications |
| Kafka K8s integration | Strimzi | Manage Kafka cluster |
| Monitoring | Prometheus | Infrastructure + application metrics |
| Workflow orchestration | Optional Airflow | Add only after core pipeline works |

Do not add Flink, MLflow, OpenMetadata, Airflow, Debezium, feature stores, or vector databases in the initial version.

---

# 4. Repository Layout

Create the repository with the following structure:

```text
hospital-lake/
├── README.md
├── plan.md
├── Makefile
├── .env.example
├── docker-compose.yml
│
├── docs/
│   ├── architecture.md
│   ├── data-model.md
│   ├── demo-script.md
│   └── benchmark.md
│
├── data/
│   ├── raw/
│   ├── generated/
│   └── samples/
│
├── generators/
│   ├── batch_generator/
│   │   ├── generate.py
│   │   └── config.yaml
│   └── stream_generator/
│       ├── producer.py
│       ├── event_models.py
│       └── config.yaml
│
├── spark/
│   ├── common/
│   │   ├── config.py
│   │   ├── schemas.py
│   │   └── iceberg.py
│   │
│   ├── batch/
│   │   ├── ingest_bronze.py
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   └── main.py
│   │
│   └── streaming/
│       ├── kafka_to_bronze.py
│       ├── realtime_aggregates.py
│       └── main.py
│
├── sql/
│   ├── iceberg/
│   │   ├── create_bronze_tables.sql
│   │   ├── create_silver_tables.sql
│   │   └── create_gold_tables.sql
│   ├── trino/
│   │   └── queries.sql
│   └── clickhouse/
│       ├── schema.sql
│       └── views.sql
│
├── dashboard/
│   └── grafana/
│       ├── dashboards/
│       └── provisioning/
│
├── infra/
│   ├── docker/
│   ├── k8s/
│   │   ├── namespace.yaml
│   │   ├── minio/
│   │   ├── kafka/
│   │   ├── iceberg/
│   │   ├── spark/
│   │   ├── trino/
│   │   ├── clickhouse/
│   │   ├── grafana/
│   │   └── prometheus/
│   └── helm/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
└── scripts/
    ├── bootstrap.sh
    ├── reset.sh
    ├── seed.sh
    ├── demo.sh
    └── benchmark.sh
```

---

# 5. Data Model

The core domain must remain simple enough to understand during presentation.

## 5.1 Core entities

Implement these entities:

### Patient

```text
patient_id
gender
birth_year
city
created_at
```

### Encounter

Represents an admission/visit.

```text
encounter_id
patient_id
encounter_type
department
admission_time
discharge_time
status
```

`encounter_type` examples:

- emergency
- inpatient
- outpatient
- ICU

### Lab Result

```text
lab_result_id
patient_id
encounter_id
test_code
test_name
result_value
unit
reference_low
reference_high
is_abnormal
measured_at
```

### Medication

```text
medication_id
patient_id
encounter_id
drug_name
dose
dose_unit
route
start_time
end_time
```

### Vital Measurement

```text
measurement_id
patient_id
encounter_id
heart_rate
spo2
systolic_bp
diastolic_bp
temperature
measured_at
```

### Bed Event

```text
event_id
patient_id
encounter_id
department
bed_id
event_type
event_time
```

`event_type`:

- assigned
- transferred
- released

---

# 6. Event Model

All streaming events must use a common envelope.

```json
{
  "event_id": "uuid",
  "event_type": "ADMISSION",
  "event_time": "2026-09-28T10:25:00Z",
  "source": "hospital-simulator",
  "version": 1,
  "payload": {}
}
```

Supported event types:

```text
PATIENT_REGISTERED
ADMISSION
DISCHARGE
TRANSFER
LAB_RESULT
MEDICATION_ORDER
VITAL_MEASUREMENT
BED_ASSIGNED
BED_RELEASED
```

Kafka topics:

```text
hospital.patient
hospital.encounter
hospital.lab
hospital.medication
hospital.vitals
hospital.bed
hospital.dlq
```

Alternative acceptable design:

```text
hospital.events
```

with partition key based on `patient_id`.

Prefer multiple topics only if it improves implementation clarity.

---

# 7. Lakehouse Design

Use Apache Iceberg tables stored in MinIO.

## 7.1 Bronze layer

Purpose:

- immutable ingestion layer,
- minimal transformation,
- preserve source record,
- retain ingestion metadata.

Tables:

```text
bronze.patients_raw
bronze.encounters_raw
bronze.lab_results_raw
bronze.medications_raw
bronze.vitals_raw
bronze.bed_events_raw
```

Required metadata fields:

```text
_source
_ingested_at
_batch_id
_event_id
_schema_version
```

Bronze rules:

- never overwrite source data,
- accept duplicates,
- accept malformed rows into quarantine/DLQ where possible,
- preserve original timestamps,
- no business aggregations.

---

## 7.2 Silver layer

Purpose:

- normalized,
- validated,
- deduplicated,
- queryable hospital data.

Tables:

```text
silver.patients
silver.encounters
silver.lab_results
silver.medications
silver.vitals
silver.bed_events
```

Required transformations:

- parse timestamps,
- cast types,
- normalize department names,
- normalize units where applicable,
- deduplicate by primary/event key,
- remove invalid records,
- validate required IDs,
- mark abnormal lab results,
- resolve current encounter state,
- enforce common naming conventions.

Example department normalization:

```text
"Emergency"
"ER"
"Emergency Department"
"ED"
```

becomes:

```text
EMERGENCY
```

---

## 7.3 Gold layer

Gold tables are the actual analytical products.

Create at minimum:

### `gold.hospital_hourly_metrics`

```text
hour
current_patients
admissions
discharges
transfers
active_icu_patients
abnormal_lab_results
```

### `gold.department_daily_metrics`

```text
date
department
admissions
discharges
unique_patients
avg_length_of_stay_hours
bed_utilization
```

### `gold.icu_occupancy`

```text
timestamp
occupied_beds
total_beds
available_beds
occupancy_rate
```

### `gold.lab_daily_metrics`

```text
date
test_name
total_tests
abnormal_tests
abnormal_rate
```

### `gold.patient_timeline`

```text
patient_id
event_time
event_type
encounter_id
summary
```

### `gold.hospital_current_state`

Realtime-oriented table/view:

```text
updated_at
current_patients
current_icu_patients
available_beds
admissions_last_hour
discharges_last_hour
abnormal_labs_last_15m
```

---

# 8. Batch Pipeline

## 8.1 Input

Initial development dataset:

- synthetic CSV/Parquet generated locally.

Optional later:

- MIMIC-IV-derived tables if access is available.

Recommended development scale progression:

```text
Stage 1: 10,000 patients
Stage 2: 100,000 patients
Stage 3: 1,000,000 patients
```

Do not require 1M patients for local development.

---

## 8.2 Batch pipeline stages

### Stage A — Ingest raw files

```text
CSV / Parquet
     |
     v
Spark
     |
     v
Bronze Iceberg
```

Requirements:

- automatic schema application,
- ingestion timestamps,
- reject/quarantine invalid rows,
- idempotent re-run.

### Stage B — Bronze → Silver

```text
Bronze
  |
Spark
  |
clean
deduplicate
normalize
validate
  |
  v
Silver
```

### Stage C — Silver → Gold

```text
Silver
  |
Spark aggregation
  |
  v
Gold
```

The batch job must log:

```text
input_rows
output_rows
invalid_rows
duplicate_rows
duration_seconds
```

---

# 9. Streaming Pipeline

## 9.1 Event generation

Build a simulator that produces realistic hospital events.

Example sequence:

```text
PATIENT_REGISTERED
ADMISSION
BED_ASSIGNED
LAB_RESULT
VITAL_MEASUREMENT
MEDICATION_ORDER
TRANSFER
DISCHARGE
BED_RELEASED
```

The generator must support:

```bash
python producer.py --rate 100
python producer.py --rate 1000
python producer.py --rate 10000
```

Rate is events per second.

Use deterministic random seed support.

---

## 9.2 Kafka partitioning

Partition events using:

```text
patient_id
```

Reason:

Events for the same patient should preserve ordering as much as possible.

Record:

```text
topic
partition
offset
event_time
ingestion_time
```

---

## 9.3 Streaming processing

Spark Structured Streaming must:

1. read Kafka,
2. deserialize JSON,
3. validate schema,
4. send bad events to DLQ,
5. deduplicate using event ID,
6. use event time,
7. apply watermark,
8. write raw/validated events to Iceberg,
9. calculate live aggregations,
10. write serving metrics to ClickHouse.

Suggested watermark:

```text
10 minutes
```

Use window examples:

```text
1 minute
5 minutes
15 minutes
1 hour
```

Required realtime metrics:

```text
admissions_last_5m
discharges_last_5m
abnormal_labs_last_15m
current_icu_occupancy
events_per_second
```

---

# 10. Batch vs Stream Responsibilities

Keep this distinction explicit in code and documentation.

| Requirement | Batch | Stream |
|---|---:|---:|
| Historical ingestion | Yes | No |
| Historical cleaning | Yes | Optional |
| Long-range analytics | Yes | No |
| Current hospital status | No | Yes |
| Last 5-minute metrics | No | Yes |
| Lakehouse persistence | Yes | Yes |
| Gold daily tables | Yes | No |
| Live dashboard metrics | No | Yes |
| Reprocessing | Yes | No |
| Low latency | No | Yes |

---

# 11. Lambda Architecture Mapping

The project should explicitly document the Lambda interpretation.

```text
                         Hospital Data
                         /           \
                        /             \
                       v               v
                Batch Layer        Speed Layer
                   Spark             Kafka
                     |                 |
                     |          Spark Streaming
                     |                 |
                     v                 v
              Historical Gold    Realtime Gold
                       \             /
                        \           /
                         v         v
                         Serving Layer
                     Trino + ClickHouse
                              |
                           Grafana
```

### Batch layer

Optimized for:

- completeness,
- reprocessing,
- large historical scans,
- daily/hourly aggregates.

### Speed layer

Optimized for:

- low latency,
- current state,
- recent metrics.

### Serving layer

Optimized for:

- interactive queries,
- dashboards,
- stable analytical views.

---

# 12. Query Layer

## 12.1 Trino

Use Trino for:

- ad-hoc SQL,
- historical queries,
- direct Iceberg access.

Required queries:

```sql
-- admissions per day
-- average stay by department
-- abnormal lab rate by test
-- ICU utilization by day
-- patient event history
```

Example acceptance query:

```sql
SELECT
    date(admission_time) AS date,
    count(*) AS admissions
FROM iceberg.silver.encounters
GROUP BY 1
ORDER BY 1;
```

---

## 12.2 ClickHouse

Use ClickHouse only for realtime/serving workloads.

Minimum tables:

```text
realtime_hospital_state
realtime_department_metrics
realtime_lab_metrics
```

Do not duplicate the full Lakehouse into ClickHouse.

ClickHouse is a serving store, not the source of truth.

---

# 13. Dashboard Requirements

Grafana must contain at least three dashboards.

## Dashboard 1 — Hospital Overview

Panels:

```text
Current patients
Admissions today
Discharges today
Available beds
ICU occupancy
Admissions by hour
Department occupancy
```

## Dashboard 2 — Realtime Operations

Panels:

```text
Incoming events/sec
Admissions last 5m
Discharges last 5m
Abnormal labs last 15m
Current ICU occupancy
Kafka lag
Streaming job latency
```

## Dashboard 3 — Historical Analytics

Panels:

```text
Admissions by day/month
Average length of stay
Bed utilization trend
Top lab tests
Abnormal lab rate
Patients by department
```

---

# 14. Kubernetes Architecture

Namespace:

```text
hospital-lake
```

Deploy:

```text
hospital-lake
├── kafka
├── spark-operator
├── minio
├── iceberg-rest-catalog
├── trino
├── clickhouse
├── grafana
├── prometheus
└── stream-generator
```

Use:

- StatefulSet/operator for stateful systems,
- Deployment for stateless services,
- PersistentVolumeClaims for persistent storage,
- ConfigMap for non-secret config,
- Secret for passwords/credentials,
- Service for internal access.

---

# 15. Kubernetes Demo Requirements

The project must demonstrate that Kubernetes is useful.

## Demo A — Scale streaming processing

Initial state:

```text
producer rate = 1,000 events/s
Spark executors = 2
```

Increase:

```text
producer rate = 10,000 events/s
```

Observe:

```text
Kafka lag increases
processing latency increases
```

Increase Spark resources:

```text
2 executors → 4 executors
```

Observe:

```text
throughput increases
Kafka lag decreases
```

Autoscaling is optional.

Manual scaling is acceptable for the first version.

---

## Demo B — Failure recovery

While streaming:

```bash
kubectl delete pod <spark-executor-pod>
```

Expected:

- pod is recreated,
- stream recovers,
- no catastrophic pipeline failure.

Also test:

```bash
kubectl delete pod <kafka-pod>
```

only if the Kafka deployment supports safe recovery.

---

# 16. Monitoring

Prometheus should scrape:

- Kafka,
- Spark,
- Kubernetes,
- ClickHouse,
- application metrics if available.

Key metrics:

```text
events_in_per_second
events_processed_per_second
kafka_consumer_lag
stream_processing_latency
spark_executor_count
spark_executor_cpu
spark_executor_memory
batch_job_duration
clickhouse_query_latency
```

Create a small infrastructure dashboard in Grafana.

---

# 17. Data Generation Strategy

## 17.1 Synthetic batch generator

Generate:

```text
patients
encounters
labs
medications
vitals
bed_events
```

Requirements:

- realistic foreign-key relationships,
- configurable patient count,
- deterministic seed,
- configurable date range,
- Parquet output preferred.

Config example:

```yaml
seed: 42
patients: 100000
start_date: "2025-01-01"
end_date: "2026-09-01"
departments:
  - EMERGENCY
  - ICU
  - CARDIOLOGY
  - INTERNAL_MEDICINE
  - SURGERY
```

---

## 17.2 Streaming generator

Streaming generator may sample/replay generated patient journeys.

Config:

```yaml
events_per_second: 1000
late_event_probability: 0.02
duplicate_probability: 0.01
invalid_event_probability: 0.005
```

The generator should intentionally produce:

- duplicates,
- late events,
- occasional invalid messages.

This allows demonstration of robust stream processing.

---

# 18. Data Quality

Minimum checks:

### Patient

```text
patient_id not null
birth_year reasonable
gender valid enum
```

### Encounter

```text
encounter_id not null
patient_id exists
admission_time valid
discharge_time >= admission_time
department valid
```

### Lab

```text
patient_id not null
test_name not null
measured_at valid
```

### Vital

```text
heart_rate > 0
spo2 between 0 and 100
temperature in plausible range
```

Bad data must either:

- be quarantined, or
- be sent to Kafka DLQ.

Do not silently discard invalid data.

---

# 19. Idempotency Requirements

Batch jobs must be safely rerunnable.

Use deterministic:

```text
batch_id
partition keys
primary keys
```

Avoid duplicated Gold metrics after retry.

Streaming deduplication must use:

```text
event_id
```

and watermark where appropriate.

---

# 20. Partition Strategy

Suggested Iceberg partitioning:

### encounters

```text
days(admission_time)
department
```

### lab_results

```text
days(measured_at)
```

### vitals

```text
hours(measured_at)
```

### Gold daily tables

```text
date
```

Do not over-partition.

The agent should measure file counts and avoid generating excessive tiny files.

---

# 21. Local Development Sequence

Do not start with Kubernetes.

Implement in this order.

## Phase 1 — Pure local

Run:

- MinIO
- Iceberg catalog
- Spark
- Trino

Goal:

```text
synthetic files
→ Spark
→ Bronze
→ Silver
→ Gold
→ Trino query
```

## Phase 2 — Add Kafka

Run:

```text
producer
→ Kafka
→ Spark Structured Streaming
→ Iceberg
```

## Phase 3 — Add ClickHouse

Run:

```text
stream processor
→ ClickHouse
→ query realtime state
```

## Phase 4 — Add Grafana

Connect historical and realtime views.

## Phase 5 — Kubernetes

Only after Docker/local version works.

---

# 22. Milestones

## Milestone 0 — Repository bootstrap

Deliverables:

- repository structure,
- README,
- `.env.example`,
- Makefile,
- docker-compose skeleton,
- test setup.

Acceptance:

```bash
make help
```

works.

---

## Milestone 1 — Data generation

Deliverables:

- synthetic data generator,
- Parquet dataset,
- schemas,
- basic unit tests.

Acceptance:

```bash
make generate-data PATIENTS=10000
```

produces all core entity files.

---

## Milestone 2 — Lakehouse bootstrap

Deliverables:

- MinIO,
- Iceberg catalog,
- Spark connectivity,
- Trino connectivity.

Acceptance:

Trino can list:

```text
bronze
silver
gold
```

schemas.

---

## Milestone 3 — Batch Bronze pipeline

Deliverables:

```text
raw Parquet
→ Spark
→ Bronze Iceberg
```

Acceptance:

row counts match expected input.

---

## Milestone 4 — Silver transformations

Deliverables:

- clean,
- dedup,
- validation,
- normalized tables.

Acceptance:

intentional duplicate/invalid fixtures are handled correctly.

---

## Milestone 5 — Gold batch analytics

Deliverables:

minimum Gold tables listed above.

Acceptance:

Grafana not required yet.

Trino SQL returns useful hospital analytics.

---

## Milestone 6 — Kafka streaming

Deliverables:

- Kafka,
- event producer,
- topic configuration,
- schema parser.

Acceptance:

producer sustains at least a locally reasonable configurable rate.

---

## Milestone 7 — Structured Streaming

Deliverables:

```text
Kafka
→ Spark Structured Streaming
→ Iceberg
→ realtime aggregation
```

Acceptance:

stream can run continuously for at least 15 minutes without errors.

---

## Milestone 8 — ClickHouse serving

Deliverables:

- realtime serving tables,
- writer from streaming job,
- sample queries.

Acceptance:

metrics appear within seconds of generated events.

---

## Milestone 9 — Grafana

Deliverables:

three dashboards.

Acceptance:

reviewer can visually see historical and live metrics.

---

## Milestone 10 — Kubernetes

Deliverables:

- manifests/Helm,
- namespace,
- persistent storage,
- Spark Operator,
- Strimzi,
- platform services.

Acceptance:

```bash
kubectl get pods -n hospital-lake
```

shows all required services healthy.

---

## Milestone 11 — Monitoring

Deliverables:

- Prometheus,
- system dashboard,
- Kafka lag metric,
- Spark metric.

---

## Milestone 12 — Benchmark

Deliverables:

benchmark scripts and report.

Test:

```text
1 executor
2 executors
4 executors
```

Measure:

```text
batch runtime
events/sec
consumer lag
processing latency
```

---

## Milestone 13 — Failure demo

Test:

```text
delete Spark executor
delete stream generator
restart Kafka consumer
```

Document recovery behavior.

---

# 23. Benchmark Plan

## Batch benchmark

Dataset sizes:

```text
100k patients
500k patients
1M patients
```

Spark workers:

```text
1
2
4
```

Record:

```text
input rows
input bytes
execution time
rows/sec
CPU
memory
```

---

## Streaming benchmark

Rates:

```text
100 events/s
1,000 events/s
5,000 events/s
10,000 events/s
```

Record:

```text
producer throughput
consumer throughput
Kafka lag
end-to-end latency
Spark executor CPU
Spark executor memory
```

Do not claim scalability beyond measured results.

---

# 24. Testing Strategy

## Unit tests

Test:

- schemas,
- normalization,
- deduplication logic,
- metric calculations,
- event validation.

## Integration tests

Test:

```text
Spark → Iceberg
Kafka → Spark
Spark → ClickHouse
Trino → Iceberg
```

## End-to-end test

Input:

```text
ADMISSION event
```

Expected:

```text
Kafka
→ Structured Streaming
→ Gold metric updated
→ ClickHouse updated
```

Validate through query.

---

# 25. Makefile Targets

Provide these targets:

```text
make setup
make up
make down
make reset
make generate-data
make batch
make stream
make query
make dashboard
make test
make benchmark
make k8s-up
make k8s-down
make demo
```

`make demo` should run the happy path wherever feasible.

---

# 26. Configuration

Use environment variables.

Example:

```env
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123

KAFKA_BOOTSTRAP_SERVERS=kafka:9092

CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123

TRINO_HOST=trino
TRINO_PORT=8080

ICEBERG_CATALOG_URI=http://iceberg-rest:8181
```

Do not hardcode secrets in source files.

---

# 27. Logging

Every application should emit structured logs when practical.

Example:

```json
{
  "level": "INFO",
  "component": "bronze_to_silver",
  "batch_id": "20260928_001",
  "input_rows": 1000000,
  "output_rows": 996421,
  "invalid_rows": 3579,
  "duration_seconds": 42.1
}
```

---

# 28. Documentation Requirements

Create:

## `docs/architecture.md`

Include:

- architecture diagram,
- component responsibilities,
- batch path,
- stream path,
- Lambda mapping.

## `docs/data-model.md`

Include:

- entity definitions,
- keys,
- relationships,
- Bronze/Silver/Gold tables.

## `docs/demo-script.md`

A presentation script with exact commands.

Example:

```text
1. Show K8s pods.
2. Show current dashboard.
3. Start event producer.
4. Increase rate.
5. Show Kafka lag.
6. Scale Spark.
7. Show lag recover.
8. Kill executor.
9. Show restart/recovery.
10. Run Trino historical query.
```

## `docs/benchmark.md`

Include raw measurements and interpretation.

---

# 29. Presentation Story

The project should be explained in this order.

## Problem

Hospital data comes from multiple systems and is difficult to analyze consistently.

## Solution

A unified hospital Lakehouse.

## Batch path

Historical data:

```text
source
→ Spark
→ Bronze
→ Silver
→ Gold
→ Trino
```

## Streaming path

Live events:

```text
event
→ Kafka
→ Structured Streaming
→ realtime Gold
→ ClickHouse
```

## Serving

```text
Trino + ClickHouse
→ Grafana
```

## Infrastructure

Everything runs on Kubernetes.

## Demonstration

Show:

- batch query,
- realtime dashboard,
- scale,
- failure recovery.

---

# 30. Minimum Viable Project

If time becomes limited, the absolute minimum version is:

```text
Synthetic data
    |
    +→ Spark batch
    |    |
    |    v
    |  Iceberg Bronze/Silver/Gold
    |    |
    |    v
    |   Trino
    |
    +→ Kafka
         |
         v
    Spark Structured Streaming
         |
         v
     ClickHouse
         |
         v
       Grafana
```

Run on:

```text
Docker Compose first
Kubernetes second
```

Do not cut:

- Bronze/Silver/Gold,
- batch processing,
- streaming processing,
- Kafka,
- Spark,
- Iceberg,
- dashboard,
- Kubernetes demonstration.

Can cut if necessary:

- MIMIC integration,
- Airflow,
- autoscaling,
- advanced security,
- ML,
- complex data catalog,
- CDC.

---

# 31. Optional Extensions

Only implement after all core milestones are complete.

## Optional A — MIMIC-IV integration

Map selected MIMIC tables into the common hospital schema.

## Optional B — Airflow

Schedule:

```text
bronze ingestion
→ silver transformation
→ gold aggregation
```

## Optional C — CDC

Introduce PostgreSQL + Debezium:

```text
PostgreSQL
→ Debezium
→ Kafka
```

## Optional D — Autoscaling

Autoscale processing based on:

- CPU,
- Kafka lag,
- processing latency.

## Optional E — Simple ML

Example:

```text
length-of-stay prediction
```

Must use Silver/Gold tables as input.

ML must remain a consumer of the Lakehouse, not the center of the project.

---

# 32. Agent Priority Order

When implementing, follow this priority exactly:

```text
P0: Data generation
P0: Iceberg + MinIO works
P0: Spark batch works
P0: Bronze → Silver → Gold works
P0: Trino queries work

P1: Kafka works
P1: Structured Streaming works
P1: Realtime metrics work
P1: ClickHouse works

P1: Grafana works

P2: Kubernetes deployment works
P2: Prometheus works
P2: Scaling demo works
P2: Recovery demo works

P3: MIMIC integration
P3: Airflow
P3: CDC
P3: ML
```

Never work on P3 while a P0 or P1 item is broken.

---

# 33. Final Acceptance Checklist

## Architecture

- [ ] Batch path exists.
- [ ] Streaming path exists.
- [ ] Lambda architecture is explainable.
- [ ] Lakehouse is central to the system.
- [ ] Every architecture block has clear input/output.

## Data

- [ ] Synthetic dataset generator works.
- [ ] Referential integrity is reasonable.
- [ ] Bronze tables exist.
- [ ] Silver tables exist.
- [ ] Gold tables exist.

## Batch

- [ ] Spark historical ingestion works.
- [ ] Spark transformation works.
- [ ] Reprocessing is idempotent.
- [ ] Batch metrics are logged.

## Streaming

- [ ] Kafka producer works.
- [ ] Kafka consumer works.
- [ ] Event-time processing works.
- [ ] Watermark is configured.
- [ ] Duplicate events are handled.
- [ ] DLQ exists.
- [ ] Realtime Gold metrics update continuously.

## Serving

- [ ] Trino queries Iceberg.
- [ ] ClickHouse serves realtime metrics.
- [ ] Grafana dashboards work.

## Infrastructure

- [ ] Docker Compose works.
- [ ] Kubernetes deployment works.
- [ ] Spark Operator is used.
- [ ] Kafka is deployed with Strimzi or equivalent.
- [ ] Prometheus metrics are visible.

## Demo

- [ ] Historical query demonstrated.
- [ ] Realtime event demonstrated.
- [ ] Dashboard updates demonstrated.
- [ ] Processing scale demonstrated.
- [ ] Worker failure/recovery demonstrated.

## Documentation

- [ ] README complete.
- [ ] Architecture documented.
- [ ] Data model documented.
- [ ] Demo script documented.
- [ ] Benchmarks documented.

---

# 34. Recommended First Implementation Task

The coding agent should start with exactly this sequence:

```text
1. Create repository structure.
2. Implement synthetic data schemas.
3. Implement batch generator for 10k patients.
4. Launch MinIO + Iceberg catalog + Trino locally.
5. Create Bronze Iceberg tables.
6. Write Spark raw → Bronze job.
7. Verify row counts using Trino.
8. Implement Bronze → Silver.
9. Implement Silver → Gold.
10. Only after that, add Kafka.
```

The first concrete end-to-end checkpoint is:

```text
Synthetic hospital dataset
        ↓
      Spark
        ↓
 Bronze → Silver → Gold
        ↓
      Trino
        ↓
successful SQL analytics query
```

Do not proceed to streaming until this checkpoint is stable.

---

# 35. One-Sentence Project Description

> HospitalLake is a cloud-native hospital Data Lakehouse that unifies historical and real-time healthcare operational data using Apache Spark, Kafka, Iceberg, MinIO, Trino, ClickHouse, Grafana, and Kubernetes to support both batch analytics and low-latency hospital monitoring.
