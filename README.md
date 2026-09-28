# HospitalLake

**Cloud-Native Hospital Data Lakehouse for Batch and Real-Time Analytics**

Course project — *Big Data Storage and Processing*, Hanoi University of Science and Technology (HUST).

> ⚠️ Academic project. All data is **synthetic / de-identified**. This system makes no clinical claims and is not intended for real hospital use.

---

## 1. Motivation

A hospital produces data from many separate systems — patient registration, admissions/discharges/transfers, laboratory, medication, ICU monitoring, bed management. Some of it arrives as large **historical batches**, some as **real-time operational events**. Analyzing both consistently is hard.

HospitalLake builds a unified platform that brings both kinds of data into a single **Data Lakehouse** and serves historical and live analytics from it.

## 2. Objectives

- Ingest hospital-like historical and real-time data from multiple sources.
- Organize data in a **Bronze → Silver → Gold** medallion Lakehouse on **MinIO + Apache Iceberg**.
- Process historical data with **Apache Spark** (batch layer).
- Process live events with **Kafka + Spark Structured Streaming** (speed layer), handling event time, watermarks, duplicates, late and invalid events.
- Serve historical queries through **Trino** and low-latency metrics through **ClickHouse**, visualized in **Grafana**.
- Run the whole platform on **Kubernetes** (Spark Operator, Strimzi, Prometheus) and demonstrate **scaling**, **fault recovery**, and **reproducible benchmarks**.

Together, these form a clear **Lambda Architecture**:

```text
                  Hospital Data
                  /           \
                 v             v
          Batch Layer       Speed Layer
             Spark       Kafka + Streaming
               |               |
               v               v
        Historical Gold   Realtime Gold
                \             /
                 v           v
                 Serving Layer
              Trino + ClickHouse
                      |
                   Grafana
```

### Tech stack

| Area | Technology |
|---|---|
| Object storage | MinIO |
| Table format | Apache Iceberg |
| Batch processing | Apache Spark |
| Event transport | Apache Kafka |
| Stream processing | Spark Structured Streaming |
| Historical SQL | Trino |
| Realtime serving | ClickHouse |
| Visualization | Grafana |
| Orchestration | Kubernetes (Spark Operator, Strimzi) |
| Monitoring | Prometheus |

### Out of scope

Medical diagnosis, clinical decision support, complex ML models, patient-facing apps, and production-grade compliance/high availability.

## 3. Repository structure

```text
.
├── README.md
├── AGENTS.md          # rules for contributors and coding agents
├── plan.md            # architecture, data model, milestones
├── works.md           # 9-week work allocation
├── sitemap.md         # detailed map of every directory
│
├── docs/              # architecture, data model, demo script, benchmark report
├── data/              # local datasets (raw/, generated/ are git-ignored; samples/ tracked)
├── generators/
│   ├── batch_generator/   # synthetic historical data → Parquet
│   └── stream_generator/  # real-time hospital event producer → Kafka
├── spark/
│   ├── common/        # shared config, schemas, Iceberg/MinIO session setup
│   ├── batch/         # raw → Bronze → Silver → Gold
│   └── streaming/     # Kafka → Bronze + realtime aggregates → ClickHouse
├── sql/
│   ├── iceberg/       # Bronze/Silver/Gold table DDL
│   ├── trino/         # historical analytical queries
│   └── clickhouse/    # realtime serving schema
├── dashboard/
│   └── grafana/       # dashboards + provisioning as code
├── infra/
│   ├── docker/        # local Docker Compose assets
│   ├── k8s/           # Kubernetes manifests per component
│   └── helm/          # Helm values for operators/charts
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── scripts/           # bootstrap, reset, seed, demo, benchmark
```

See [`sitemap.md`](./sitemap.md) for what goes in each directory.

## 4. Status

🚧 **Milestone 0 — repository skeleton.** Only the directory structure exists; implementation starts in Week 1 (see [`works.md`](./works.md)).

## 5. Team

Four equal members (TV1–TV4). Work allocation and weekly integration gates are defined in [`works.md`](./works.md).
