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

🚧 **Week 1 — initial batch generator.** A dependency-free Python generator produces synthetic patients, encounters, and lab results as CSV, using the fields in `plan.md`. It supports configurable scale and seed, valid foreign keys, and UTC timestamps. Parquet, the remaining entities, and the service pipeline are still planned (see [`works.md`](./works.md)).

Run from the repository root with Python 3.10+:

```bash
python generators/batch_generator/generate.py --patients 1000 --seed 42
python -m unittest discover -s tests/unit -v
```

The generator writes `patients.csv`, `encounters.csv`, and `lab_results.csv` under `data/generated/` (one row per patient in each file). Use `--output-dir` to choose another directory. Rerunning replaces these three files; the same seed and patient count reproduce the same bytes. These initial clean fixtures are inputs for future batch ingestion; intentional bad-data cases and streaming are not implemented yet.

### Run local MinIO (TV2, Week 1 — task 1)

Requires Docker Engine, Docker Compose v2+ (either `docker compose` or `docker-compose`), Make and curl.

```bash
make setup
make config
make minio-up
make smoke-minio
make test-minio
```

Expected: MinIO becomes healthy and the smoke check prints `PASS`. Open the console at http://localhost:9001 and sign in with the local credentials in `.env`. The S3 API is at http://localhost:9000; containers on the Compose network use `MINIO_ENDPOINT=http://minio:9000`. Host ports can be changed in `.env`.

`make minio-up` builds only MinIO from the pinned upstream release
`RELEASE.2025-04-22T22-12-26Z` using `infra/docker/minio/Dockerfile`. This avoids
the unavailable upstream MinIO image. The first build downloads the Go toolchain
image and source dependencies; subsequent builds reuse Docker cache. No host Go
installation is required. This follows the upstream
[source installation approach](https://github.com/minio/minio/tree/RELEASE.2025-04-22T22-12-26Z).
Existing `.env` files may still contain the old image
name: set `MINIO_IMAGE=hospital-lake/minio:RELEASE.2025-04-22T22-12-26Z`.

`make test-minio` creates a unique temporary bucket, writes and reads a test
object through the authenticated S3 API, recreates the MinIO container, and
checks the object again. It cleans up the test object and bucket. Run this
before shared workloads: container recreation briefly interrupts MinIO. It
does not create the project warehouse bucket.

`make minio-stop` stops MinIO while preserving data.
`make up` starts both configured services. To start MinIO alone, run
`docker compose up -d --wait minio` (or `docker-compose up -d --wait minio`).

`make down` stops the stack while preserving the `minio-data` volume. Running `make up` again reuses that volume. `docker compose down -v` deletes stored data; it is not part of the normal stop command. Use `docker-compose` instead if that is your installed CLI.

### TV4 Week 1: Trino row-count smoke check

After TV2 starts MinIO/catalog and TV3 writes an Iceberg fixture with a known
row count, install the [Trino CLI](https://trino.io/docs/current/client/cli.html)
and run from the repository root:

```bash
python scripts/trino_smoke.py --table iceberg.bronze.spark_smoke --expected-rows 3
```

The table name and count above are examples: supply the actual table written by
Spark and its expected count. The script does not create or modify data. It runs
`SELECT count(*)` through Trino and prints `PASS` only when the count matches.
Exit codes: `0` for success, `1` for query/connection/timeout/result failures,
`2` for invalid arguments. An empty table succeeds only with `--expected-rows 0`.
Use `--server http://host:8080`, `--cli /path/to/trino`, and `--timeout 60` as needed.
Without `--server`, the script uses `TRINO_HOST` (default `localhost`) and
`TRINO_PORT` (default `8080`). Authentication can use the CLI's own configuration.

Run the unit checks with `python -m unittest discover -s tests/unit -v`.
These tests simulate CLI responses; they do not certify the live integration.
The actual Spark → Iceberg → Trino gate remains pending. A matching row count
alone does not prove all field values.

### TV4 local Trino deployment

`docker-compose.yml` currently provisions **MinIO and Trino**. Trino uses image
`trinodb/trino:483` and mounts the Iceberg REST/S3 catalog in `infra/docker/trino/`, uses a 1 GiB JVM
heap within a 2 GiB container limit, and publishes its port on localhost.

1. Run `make setup` to create `.env`. It includes synthetic-data local credentials shared by MinIO and Trino; existing `.env` files are preserved.
2. MinIO is provided on the shared Compose network. Iceberg REST Catalog and
   bucket bootstrap remain pending TV2 tasks; the default catalog URI targets
   the future `iceberg-rest` service. For
   services running on the Docker Desktop host, use `host.docker.internal` in
   `.env` instead of `localhost` (which refers to the Trino container).
3. Run `docker compose config --quiet`, then `docker compose up -d --wait trino`.
4. Inspect failures with `docker compose logs trino`.

The health check runs `SELECT 1`: it proves query-engine readiness only, not
Iceberg or MinIO connectivity. The table smoke check above verifies the next
stage once TV3 has written a fixture. Stop this service with `docker compose stop trino`.
MinIO and Trino share the `hospital-lake` network. Catalog and Spark deployment
remain pending. MinIO is built locally from pinned upstream source because pulling the upstream
image failed. Readiness and the live S3 persistence test passed on the local
Docker environment; this does not certify catalog/Spark/Trino integration.

Once the table is available, run all four read-only probes using the CLI bundled
in the Trino container (no host CLI installation needed):

```bash
python scripts/trino_smoke.py --compose --check-stack --table iceberg.bronze.spark_smoke --expected-rows 3
# Equivalent, if GNU Make is installed:
make trino-smoke TABLE=iceberg.bronze.spark_smoke EXPECTED_ROWS=3
```

The probes stop at the first failure, labelled `engine`, `catalog`, `count`, or
`data`. `data` executes `SELECT * ... LIMIT 1` so a successful count alone cannot
hide a failure reading a data file. Use a stable fixture: concurrent writes can
change results between queries. Each query has its own `--timeout` (60 seconds
by default). Empty fixtures cannot prove a data-file read. For a remote/local CLI,
omit `--compose`; use `--server` as needed. The script does not print sample values.

`make help` lists the available commands. `make test`, `make trino-config`,
`make trino-up`, and `make trino-stop` wrap the commands above.

Configuration follows the upstream [Trino container documentation](https://trino.io/docs/current/installation/containers.html),
[REST catalog properties](https://trino.io/docs/current/object-storage/metastores.html#rest-catalog),
and [S3 settings](https://trino.io/docs/current/object-storage/file-system-s3.html).

### TV3 Week 1: shared Spark configuration

`spark/common/config.py` provides `SparkConfig.from_env()` for batch and streaming
jobs. It reads exported variables for Spark, Iceberg REST, and MinIO; validates
non-empty settings, HTTP(S) endpoints, and an S3 warehouse URI; and requires
`MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` without including them in its repr.
No additional Python dependencies are needed.

Run `make setup` to create `.env` with local MinIO credentials, or configure
your existing `.env`. To load that file
into a shell and validate the configuration from the repository root:

```bash
set -a
source .env
set +a
python -c 'from spark.common.config import SparkConfig; SparkConfig.from_env(); print("PASS: Spark configuration")'
make test
```

The shared defaults target the Compose network. For future host-side Spark jobs,
set reachable `MINIO_ENDPOINT` and `ICEBERG_CATALOG_URI` addresses in `.env`.
This increment only loads and validates configuration. SparkSession creation,
connectors, and the live Spark → Iceberg → Trino smoke test remain pending.

## 5. Team

Four equal members (TV1–TV4). Work allocation and weekly integration gates are defined in [`works.md`](./works.md).
