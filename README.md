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

**TV1 generator — Week 1 integrated; all six Week 2 entities implemented.** A shared
Arrow schema and generator produce patients, encounters, and lab results in
Parquet (default) or CSV; Week 2 also includes `medications`, `vitals` and `bed_events`. Patient IDs are `P00000001`, encounter IDs
`E000000001`, and lab IDs `L0000000001`; timestamps use UTC. Raw data includes
configurable exact duplicates, department aliases, and invalid discharge times.
`_manifest.json` records configuration, row counts, and injected defect counts.

```bash
make setup
make generate-data PATIENTS=1000 SEED=42
make test
# CSV compatibility output (use a separate directory from Parquet):
make generate-data PATIENTS=1000 FORMAT=csv OUTPUT_DIR=data/generated/csv
```

`config.yaml` controls date range, departments, encounter counts and defect rates.
The Python `generate(output_dir, patients, seed)` API still writes CSV and returns
row counts, using clean data from the same engine. Counts for encounters and labs
now vary with patient journeys; the former one-row-per-patient assumption and
`patient-...` IDs must be replaced with manifest counts and actual stable IDs.
Direct script invocation supports the same flags; add `--format csv` for CSV.
`medications_per_encounter` configures order counts. Medication IDs use
`M<encounter-number>-<order-number>`; orders reference valid patients/encounters
and their times stay inside the clean encounter. `vitals_per_encounter` controls
observation counts; units are heart rate bpm, SpO2 percent, blood pressure mmHg
and temperature Celsius. Measurement IDs use `V<encounter-number>-<sample-number>`.
Bed events use `assigned`, optional `transferred`, then `released` only on
discharge; outpatient encounters have no bed journey. A transfer names the new
bed and closes the previous assignment within the same department. Bed IDs are
synthetic and scoped to the encounter, preventing double booking; they do not
represent a finite hospital inventory. `bed_transfer_rate` controls transfers.
Live batch integration remains pending.

### TV1 Week 2: validate the complete generated dataset

```bash
make generate-data PATIENTS=10000
make verify-generated INPUT_DIR=data/generated
make validate-generator SCALES="10000 100000"
```

`verify-generated` checks actual Parquet schema, counts, non-null primary keys,
patient/encounter foreign keys, and duplicate counts against the manifest for
all six entities. `validate-generator` runs each scale twice in a temporary
directory, validates both outputs and compares every file's SHA-256 plus the
manifest. It writes a JSON report to `results/generator-validation.json`;
`GENERATOR_REPORT` changes that path. Invalid data/contracts return a nonzero
exit code. This validates generated sources; the live Parquet → Bronze → Trino
gate still needs upstream infrastructure and ingestion implementation.

### TV2 Week 1: local Lakehouse bootstrap

Requires Docker Engine, Compose v2+, Make, curl and host Python 3.10+.
Host Java/Spark are not needed for the Week 1 gate: Spark runs in the optional
Compose `smoke` profile. From the repository root:

```bash
python3.12 -m venv .venv  # any installed Python 3.10+ can create this venv
make setup PYTHON=.venv/bin/python
make config
make up
make smoke-minio
make week1-smoke PYTHON=.venv/bin/python
make week1-recovery PYTHON=.venv/bin/python
make test PYTHON=.venv/bin/python
```

MinIO is built from pinned upstream source; the first build downloads Go and
source dependencies. If an existing `.env` has the retired upstream image name,
set `MINIO_IMAGE=hospital-lake/minio:RELEASE.2025-04-22T22-12-26Z`.
`make up` starts MinIO, runs `minio-init` to create the bucket named by
`ICEBERG_WAREHOUSE`, then starts Iceberg REST and Trino. An existing bucket is
checked and preserved. `make bootstrap-bucket` safely repeats that step.
`make minio-up` starts storage and bucket bootstrap only.

MinIO console: http://localhost:9001; S3 API: http://localhost:9000;
REST API: http://localhost:8181/v1/config; Trino: http://localhost:8080.
Ports can be configured in `.env`. Containers use `minio:9000` and
`iceberg-rest:8181`; host-side jobs need reachable host addresses instead.
Credentials are local synthetic-data defaults in `.env`, which must not be committed.

`make week1-smoke` creates a uniquely named temporary Iceberg table in `bronze`,
writes three synthetic rows with Spark, repeats bucket bootstrap, and verifies
all values plus `bronze`/`silver`/`gold` visibility through Trino. It purges only
its temporary fixture afterwards. `make week1-recovery` additionally recreates
MinIO and catalog containers while the fixture exists, checks it with a fresh
Spark process and Trino, then cleans up. Run recovery checks before shared
workloads because they briefly interrupt storage/catalog services.
`make test-minio PYTHON=.venv/bin/python` separately checks S3 object persistence.
Live tests are explicit and are skipped by the ordinary regression suite.

`make down` preserves MinIO data and the SQLite catalog volume. `make up` reuses
them. `docker compose down -v` deletes warehouse and catalog state and is not a
normal stop command. Make detects `docker compose` or standalone `docker-compose`;
use `COMPOSE=...` to override. Catalog fixture is a single-node local academic
setup, not a production HA catalog. See [local architecture](docs/architecture.md).

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
A matching row count alone does not prove all field values. TV2's
`make week1-smoke` runs an actual Spark fixture and compares complete rows.

### TV4 local Trino deployment

`docker-compose.yml` currently provisions **MinIO, Iceberg REST Catalog and Trino**. Trino uses image
`trinodb/trino:483` and mounts the Iceberg REST/S3 catalog in `infra/docker/trino/`, uses a 1 GiB JVM
heap within a 2 GiB container limit, and publishes its port on localhost.

1. Run `make setup` to create `.env`. It includes synthetic-data local credentials shared by MinIO and Trino; existing `.env` files are preserved.
2. MinIO, automatic bucket bootstrap and Iceberg REST Catalog are provided
   on the shared Compose network. The default catalog URI targets
   `iceberg-rest:8181`. For
   services running on the Docker Desktop host, use `host.docker.internal` in
   `.env` instead of `localhost` (which refers to the Trino container).
3. Run `docker compose config --quiet`, then `docker compose up -d --wait trino`.
4. Inspect failures with `docker compose logs trino`.

The health check runs `SELECT 1`: it proves query-engine readiness only, not
Iceberg or MinIO connectivity. The table smoke check above verifies the next
stage once TV3 has written a fixture. Stop this service with `docker compose stop trino`.
MinIO, catalog and Trino share the `hospital-lake` network. A Spark fixture job
is provided in the optional `smoke` profile. Shared SparkSession creation is
available in `spark/common/iceberg.py`; production batch/stream jobs remain pending.

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
This module loads and validates configuration. The Week 1 Docker smoke job and
Week 2 Bronze bootstrap reuse `spark/common/iceberg.py` to create SparkSession.
The production Parquet ingestion job is described below.

### TV2 Week 2: Bronze table contracts

With the Week 1 stack running:

```bash
make bootstrap-bronze
make bronze-smoke PYTHON=.venv/bin/python
```

`sql/iceberg/create_bronze_tables.sql` is Spark SQL for all six
`iceberg.bronze.*_raw` tables. `bootstrap-bronze` runs a short Docker Spark job,
so host Spark/Java are not needed. The tables match the six Parquet source
contracts, keep raw domain fields nullable, and require ingestion metadata:
`_source`, `_ingested_at`, `_batch_id`, `_schema_version`; `_event_id` is optional.
UTC source timestamps use Iceberg timestamps with time zone. Duplicates,
department aliases and invalid domain values remain raw; Silver handles cleaning.
CSV values that cannot be represented by the typed source schema require
traceable quarantine in the future ingestion job, not silent casts to null.

Bootstrap validates existing schemas, current partition specs, format version,
write properties and S3 metadata location before creating missing tables. Normal
reruns do not replace tables, change partitions or rewrite data. If a contract
has drifted, the command fails and requires an explicit migration.

`bronze-smoke` generates a temporary 32-patient six-entity Parquet dataset,
uses a UUID smoke namespace, writes intentional duplicates/invalid vitals,
compares complete Spark values and Trino ID checksums, reads metadata JSON
through authenticated MinIO S3, verifies bootstrap reruns preserve metadata,
and proves incompatible existing schemas/partitions/properties are rejected.
Only its temporary namespace is purged; canonical Bronze tables and datasets
are preserved. It reports file count, average bytes and partition count for
these tiny fixtures, not a throughput benchmark.

Partition choices and the handoff to TV3 ingestion are in
[the data model](docs/data-model.md). Production `ingest_bronze.py` remains TV3's
Week 2 task; this table-contract check does not certify the full weekly
`generate-data → batch-bronze → verify-bronze` integration gate.

### TV4 Week 2: Bronze verification

`make verify-bronze BATCH_ID=<id>` compares generated CSV or Parquet files
against the matching `iceberg.bronze.*_raw` tables for one `_batch_id`. It checks
row counts and a SHA-256 checksum of sorted stable IDs. The queries live in
`sql/trino/verify_bronze.sql`; the harness is `scripts/verify_bronze.py`.
Missing, replaced, or duplicated IDs and query failures return a nonzero exit
code. Each entity reports its own PASS or FAIL.

The current generator writes all six entities as Parquet or CSV. To verify all six sources after Bronze ingestion, run:

```bash
make verify-bronze BATCH_ID=<id> ENTITIES="patients encounters lab_results medications vitals bed_events"
```

For a host Trino CLI, run `python -m scripts.verify_bronze --batch-id <id>
--entities patients encounters lab_results medications vitals bed_events` and optionally supply `--server`.
Parquet input requires `pyarrow`. The checksum covers IDs; it does not validate
non-key values. A live run requires completed Bronze ingestion with `_batch_id`.

### TV4 Week 3: historical Gold metrics

`spark/batch/silver_to_gold.py` computes hourly hospital census and ICU occupancy,
daily department demand/utilization, and daily lab abnormal rates from
`iceberg.silver.encounters`, `lab_results`, and `bed_events`. It creates and fully
replaces four Iceberg Gold tables using `sql/iceberg/create_gold_tables.sql`.
Five example Trino queries are in `sql/trino/queries.sql`.

In Compose (no host Spark needed), `make batch` ends with `make gold-docker`,
which can also be run alone after Silver exists:

```bash
make gold-docker   # GOLD_AS_OF / GOLD_BED_CAPACITIES from .env (defaults in .env.example)
```

The defaults use the generator's `end_date` (`2026-09-01T00:00:00Z`) as the
cutoff and a bed capacity for each of the five canonical departments. Measured
on the 1,000-patient / seed 42 dataset (3,058 Silver encounters, 12,719 labs,
3,934 bed events): 14,343 hourly rows, 2,373 department-day rows, 3,258
lab-day rows and 14,343 ICU rows in about 18 s.

For host-side Spark instead, install Spark 3.5 / PySpark 3.5, use Java 17, and make the Iceberg REST catalog,
MinIO bucket, and Silver tables available. Export the settings in `.env.example`,
using host-reachable endpoints when running Spark on the host. For example:

```bash
export MINIO_ENDPOINT=http://localhost:9000 ICEBERG_CATALOG_URI=http://localhost:8181
export MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... ICEBERG_WAREHOUSE=s3://warehouse/
export GOLD_AS_OF=2026-09-01T00:00:00Z GOLD_BED_CAPACITIES=ICU=20,EMERGENCY=40,CARDIOLOGY=50,INTERNAL_MEDICINE=60,SURGERY=50
make gold
python -m unittest discover -s tests/unit -p 'test_gold_metrics.py' -v
```

`GOLD_AS_OF` is an explicit UTC cutoff for reproducible open-encounter counts.
Capacity must include ICU and every department in Silver. Census and ICU beds
are measured after events in each UTC hour (the final hour is partial at the
cutoff). Transfer count uses `bed_events.event_type = transferred`; ICU status
uses the encounter's department. Daily unique patients are counted per
department among encounters active that day. Length of stay averages discharged
encounters only. Utilization is occupied patient-hours divided by configured
bed-hours. The job rejects missing/duplicate encounter IDs and missing capacity;
it does not silently discard them. Its Iceberg overwrite makes a retry at the
same cutoff replace previous output. A local Spark test checks exact sample
values in any host timezone (collected timestamps are keyed in UTC).

### TV3 Week 2: Parquet → Bronze Iceberg

Install the pinned Spark runtime in an isolated environment (Python 3.10+,
Java 17+). The session loads the matching Iceberg 1.10.1 Spark 4.0 / Scala 2.13
runtime and AWS bundle from Maven on first use. Existing Compose Spark 3.5 jobs
keep their submitted Iceberg packages and the shared `create_spark_session(app_name, config)` API.
Configuration follows the
[Iceberg catalog configuration](https://iceberg.apache.org/docs/1.10.1/spark-configuration/)
and [S3FileIO settings](https://iceberg.apache.org/docs/1.10.1/aws/).

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements-spark.txt
TZ=UTC make test PYTHON=.venv/bin/python
make test-bronze PYTHON=.venv/bin/python
```

`test-bronze` creates temporary raw Parquet and real Iceberg tables in a local
Hadoop catalog. It checks all six entities, exact raw values including duplicates
and null IDs, metadata, retry through a fresh SQL session, a new batch, recovery
after an interrupted multi-table run, and visible failures for missing inputs,
missing tables, schema drift, and corrupt Parquet. It does not exercise REST,
MinIO, or Trino. It also verifies generated Parquet against the source-controlled
Bronze DDL.

For the actual REST/MinIO pipeline, generate immutable Parquet inputs with
`make generate-data`, start the stack with `make up`, then run
`make bootstrap-bronze` to create the existing source-controlled table contracts.
Export the settings from `.env`
as shown above; host-side jobs need reachable endpoints (for example,
`http://localhost:9000` and `http://localhost:8181` if those ports are published).
Then run:

```bash
make batch-bronze PYTHON=.venv/bin/python BATCH_ID=demo-001
# Or ingest only an available subset while teammates finish their modules:
make batch-bronze PYTHON=.venv/bin/python BATCH_ID=demo-001 ENTITIES="patients encounters lab_results"
```

To use the existing Compose Spark 3.5 image instead of installing host Java/Spark,
run after generating inputs and bootstrapping Bronze:

```bash
make batch-bronze-docker BATCH_ID=demo-001
make verify-bronze BATCH_ID=demo-001
# An external local directory is also supported:
make batch-bronze-docker BATCH_ID=demo-002 INPUT_DIR="/path/to/parquet inputs"
```

This runs the same ingestion job using the shared service endpoints and
credentials. `INPUT_DIR` becomes the read-only `/opt/hospital-input` mount through
`BRONZE_INPUT_DIR`; an absent host directory fails rather than creating an empty
one. The Spark job runs as the upstream image's non-root user, so mounted files
must be readable by that user. The job requires Maven access on first use.
This command is an alternate submission path; the full live gate still needs
verification on a machine with Docker daemon access.

`INPUT_DIR` defaults to `data/generated`. Each selected entity must have exactly
one `<entity>.parquet` file/directory or `<entity>/` directory, using the canonical
names `patients`, `encounters`, `lab_results`, `medications`, `vitals`, `bed_events`.
The target is `iceberg.bronze.<entity>_raw`. Raw field names and types must match
the target's domain fields exactly; source timestamps are preserved without
normalization. The five added metadata columns are `_source STRING` (original
file URI), `_ingested_at TIMESTAMP` (UTC), `_batch_id STRING`, `_event_id STRING`
(source `event_id`, otherwise null), and `_schema_version INT` (1). Source columns
must not use these reserved names. Inputs and schemas are checked for all selected
entities before any table is written. Unreadable Parquet fails the job visibly;
domain validation/quarantine is handled by the shared Silver entity registry.

Each table is appended in one Iceberg commit. A retry with the same `BATCH_ID`
compares the raw rows with the committed batch and skips identical tables, so it
also resumes after a later table fails. Comparison preserves duplicate counts
and ignores row order and ingestion metadata. Changed values, added/removed rows,
or duplicate counts fail visibly; use a new ID for changed inputs. This exact
retry check scans and compares both datasets; it is a correctness check rather
than a constant-time lookup.
Use one writer at a time per target table and keep inputs immutable for a given
batch ID; concurrent same-ID submissions are not protected by a distributed lock.
Do not reuse an ID to load changed data or delete rows from a committed Bronze
batch. A new ID appends a new raw batch, preserving source duplicates. Empty
inputs produce zero rows. JSON logs report table, batch ID, written/skipped/empty
status, input/output rows and duration; invalid/duplicate counts are null because
this raw stage does not evaluate domain quality.
Failures also emit a JSON event with batch ID, table (when known), phase
(`startup`, `preflight`, or `ingest`), exception type and elapsed time, then
propagate the original exception so the command fails. The structured event
omits exception messages and raw values; Python/Spark error diagnostics may
still include the original exception.

Once the live stack is available, TV4's existing row-count smoke command can query
each table (substitute the actual source count):

```bash
make trino-smoke TABLE=iceberg.bronze.patients_raw EXPECTED_ROWS=1000
```

The full Week 2 gate is `make generate-data`, `make bootstrap-bronze`,
`make batch-bronze BATCH_ID=demo-001`, then `make verify-bronze BATCH_ID=demo-001`.
The local integration suite does not certify this live MinIO/REST/Trino gate.

### TV3 Week 4: Kafka envelope reader

`spark/streaming/kafka_reader.py` reads configured Kafka topics, decodes the
shared event envelope, validates event ID/type/time/source/version/payload, and
keeps malformed messages in the stream with a `validation_error`. Kafka topic,
partition, offset, timestamp, original bytes, and extracted payload JSON remain
available to the sink and downstream consumers.
`deduplicate_events()` supplies a valid-only view deduplicated by `event_id`
with a configurable event-time watermark (10 minutes by default). The Bronze
path keeps distinct Kafka offsets, including repeated event IDs, for raw replay;
stateful downstream consumers can use the deduplicated view.

After the Kafka service and stream Bronze tables are available, install
`requirements-spark.txt`, export the local service settings, and start the reader:

```bash
set -a; source .env; set +a
export MINIO_ENDPOINT=http://localhost:9000
export ICEBERG_CATALOG_URI=http://localhost:8181
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPICS=hospital.events
make stream-bronze PYTHON=.venv/bin/python CHECKPOINT_LOCATION="$PWD/checkpoints/stream-bronze"
```

The checkpoint directory is ignored by Git and must persist across process
restarts. The reader downloads the Spark Kafka connector matching the installed
Spark 3.5 or 4.0 runtime on first use. Local Spark tests cover valid and
malformed envelopes, retained raw bytes, event-time parsing, duplicate filtering,
and watermark attachment. They do not certify the live Kafka → Spark → Iceberg
integration gate.

### TV4 Week 4: streaming Bronze and quarantine

`spark/streaming/bronze_sink.py` exposes `start_stream(validated,
checkpoint_location=...)` for TV3's Structured Streaming Kafka reader. Its input
must retain Kafka `topic`, `partition`, `offset`, `timestamp`, and binary `value`,
plus parsed `event_id`, `event_time` (UTC timestamp), `source`, `version`, and
`validation_error` (null means valid). It creates two Iceberg tables from
`sql/iceberg/create_stream_bronze_tables.sql`:
`iceberg.bronze.stream_events_raw` and
`iceberg.bronze.stream_events_quarantine`. Both retain original Kafka bytes;
quarantine additionally stores the validation reason. Kafka
topic/partition/offset prevent duplicate rows on micro-batch retry, while
distinct Kafka messages with the same `event_id` remain in raw Bronze.
The checkpoint path must persist across restarts.

When the Kafka producer, reader, REST catalog, and MinIO are running, start the
reader with `make stream-bronze`, then run the continuous Trino probe:

```bash
make stream-bronze-probe
# Or use a host Trino CLI and require a bad event during the window:
python -m scripts.stream_bronze_probe --duration 300 --interval 10 \
  --min-events 1 --require-quarantine
```

The probe checks that Bronze grows for five minutes, that Kafka coordinates do
not repeat across raw/quarantine, and (optionally) that malformed events reach
quarantine. It fails on query errors or 60 seconds without a new row; adjust
that limit with `--max-idle`. Contract tests run
with `make test`; a live five-minute result remains pending until Kafka, REST,
and MinIO are available.

An isolated Spark/Iceberg integration test exercises raw bytes, malformed-event
quarantine, and retries without Kafka or Docker. Install `requirements-spark.txt`
and run `make test-stream-bronze`; the first run needs Maven access for Iceberg.

## 5. Team

Four equal members (TV1–TV4). Work allocation and weekly integration gates are defined in [`works.md`](./works.md).

### TV2 Week 3: clinical Silver

```bash
make batch-silver-docker  # all six entities; requires populated Bronze
# Or rebuild clinical tables against current valid Silver encounters:
make batch-silver-docker SILVER_ENTITIES="lab_results bed_events medications vitals"
```

The shared registry now includes all four clinical entities. Labs recalculate
`is_abnormal`; beds normalize departments/event types; medications and vitals
validate values/times. Orphan encounters, patient mismatches and invalid values
are recorded in `silver.quarantine` with raw JSON and reason codes. Reruns
recompute Silver and replace each entity's quarantine partition. See
[clinical contracts](docs/data-model.md#week-3-clinical-silver-tv2-t21) for
validation bounds, lineage and monthly partitions. This enables Gold's lab/bed
inputs; the complete checkpoint gate still requires TV4's Gold/query integration.

### TV2 Week 4: local Kafka (T2.2)

```bash
# From a fresh checkout, create .env first; Kafka needs Docker, not host Java.
test -f .env || cp .env.example .env
make kafka-up
make kafka-smoke PYTHON=.venv/bin/python  # install requirements.txt first
make produce PYTHON=.venv/bin/python KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RATE=100 DURATION=10
make kafka-recovery PYTHON=.venv/bin/python  # optional: recreate broker, retain volume
make kafka-stop
```

`kafka-up` starts only the optional `kafka` profile broker and then bootstraps
`hospital.events` and `hospital.dlq`. Regular `make up` remains batch-only.
The pinned `apache/kafka:3.9.1` service runs one combined KRaft broker/controller,
with a persistent `kafka-data` volume and no ZooKeeper. Configuration follows
[Apache's single-node Docker example](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).

| Client | Bootstrap address |
|---|---|
| Host producer / Python smoke | `localhost:9092` (published on loopback only) |
| Compose Spark / container clients | `kafka:29092` |

TV3's stream Compose service must use `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` and
depend on successful `kafka-init` completion. The host address is not usable
from inside Spark containers. Set `KAFKA_PORT` in `.env` to change the host port;
pass the corresponding address to host `make produce`. Smoke discovers the
published port automatically. `KAFKA_TOPICS=hospital.events` is the existing
reader/producer contract; DLQ is bootstrapped separately.

Both topics start with `KAFKA_TOPIC_PARTITIONS=3`, replication factor 1 and
broker retention 24 hours. This is a local development setting, not HA or a
throughput claim. Producer keys are UTF-8 `patient_id`, preserving per-patient
partition routing and order; there is no global ordering across partitions.
Automatic topic creation is disabled. `make kafka-topics` safely reruns bootstrap,
validates existing partition/replica counts and fails on drift. It never deletes
or automatically repartitions an existing topic, which could change key routing.

Smoke sends 24 valid TV1 simulator events, checks exact keyed bytes and
per-patient order/partitions, reads host-produced data using an internal client,
and sends an internal keyed event back to the host consumer. It also checks DLQ
transport. Probes remain in the topics until retention; consumer groups and
existing offsets are untouched. `kafka-recovery` additionally rereads acknowledged
event/DLQ keys, bytes and original offsets after recreating the broker container.
It is a persistence check, not an active-workload fault/recovery benchmark.

This completes Kafka local transport for T2.2. Spark streaming Compose/checkpoint
integration remains TV3's T3.2. Neither smoke nor recovery certifies Spark DLQ
routing or the postponed 1,000 events/s, 10-minute Week 4 gate.

Local validation (2026-10-24): topic bootstrap/rerun passed; a requested
partition-count mismatch failed without altering existing topics. Host/internal
keyed smoke passed with kafka-python 3.0.11 and the minimum supported 2.1.0.
Acknowledged event/DLQ offsets and bytes survived broker recreation. A separate
Compose project started with `.env.example`, fresh Kafka volume/network and host
port 19092, then passed smoke; its test resources were removed afterward. The
existing TV1 producer also delivered 20 events successfully. These are functional
checks, not throughput benchmarks or a full-system fresh-clone certification.
UTC regression: 145 passed, 10 opt-in integration checks skipped, 85 subtests.
At `TZ=Asia/Ho_Chi_Minh`: 144 passed, 10 skipped, 85 subtests, with the unchanged
TV4 Gold hourly timezone failure documented in `next_step.md` T4.1.
