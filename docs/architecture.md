# Local Week 1 Lakehouse

The implemented local infrastructure is the storage/query foundation for the
Lambda batch layer. It currently supports a fixture gate, not historical batch
ingestion or the streaming path.

```text
minio-init ──creates/checks──> MinIO warehouse bucket
                                     ↑
Spark smoke job ──REST metadata──> Iceberg REST Catalog <── Trino
       │                             │                      │
       └──────── S3 data ────────────┴──────────────────────┘
```

| Component | Version / purpose | Persistence |
|---|---|---|
| MinIO | Source release `RELEASE.2025-04-22T22-12-26Z`, built locally because the upstream image pull fails | `minio-data:/data` stores Iceberg files |
| Bucket initializer | Same local image with curl/SigV4, checks before creating the bucket from `ICEBERG_WAREHOUSE` | Never removes existing objects |
| Iceberg REST | Apache `iceberg-rest-fixture:1.9.0`, JDBC SQLite backend | `iceberg-catalog:/var/lib/iceberg` stores namespace/table registrations |
| Trino | `trinodb/trino:483`, REST catalog, S3 path-style access | Queries shared metadata/data; no duplicated lakehouse |
| Spark fixture | `apache/spark:3.5.5-python3`, local single worker, Iceberg 1.9.0 connectors | Optional `smoke` profile; `spark-ivy:/tmp` caches downloaded JARs |

The REST fixture is a local, single-node catalog. Its SQLite database is mounted
on a persistent volume rather than using the fixture's default in-memory backend.
The wrapper initializes volume ownership for the image's non-root catalog user.
Credentials use environment variables; the catalog uses AWS credential-provider
variables instead of logging credentials as catalog properties.

All services share the `hospital-lake` network. Only MinIO API/console, REST and
Trino ports are published, on host localhost. Dependencies are explicit: MinIO
healthy → bucket initializer successful → REST healthy → Trino ready.

## Reproduce

From the repository root with Docker running and Python 3.10+:

```bash
python3.12 -m venv .venv
make setup PYTHON=.venv/bin/python
make up
make week1-smoke PYTHON=.venv/bin/python
make week1-recovery PYTHON=.venv/bin/python
make test PYTHON=.venv/bin/python
```

First runs need network access for base images, MinIO build dependencies and
Spark Maven connectors. No host Spark/Java installation is required by this gate.
The gate uses the shared `SparkConfig` and a dedicated temporary fixture job;
the reusable SparkSession module for batch/stream remains TV3's task.

The smoke job writes three exact rows to a UUID-named Iceberg table, re-runs
bucket bootstrap, checks all values through Trino, and verifies three namespaces.
Recovery recreates both storage and catalog containers while this table exists,
then reads it again with a fresh Spark process and Trino. Temporary tables are
purged; the warehouse bucket and namespaces remain for later work. Recovery
briefly interrupts services and should run before shared workloads.

`make down` preserves all volumes. `down -v` is a destructive development reset,
not part of normal reruns. Week 2 requires real Bronze DDL/ingestion and validation;
a passing fixture does not certify those later pipelines.

## Local validation (2026-10-12)

`make up`, repeated bucket bootstrap, `make week1-recovery` and the live
`make test-minio` passed on Docker Desktop ARM64. The recovery gate verified
three exact rows before/after storage and catalog recreation; its temporary
Iceberg table was purged. The host Python 3.12 regression suite returned
69 passed, 4 skipped and 46 successful subtests. Three skipped tests require
host PySpark for TV4 Gold logic; the fourth is the explicitly opt-in live MinIO
test, which was run separately and passed. A deliberately unavailable Compose
command made the harness fail at the service stage with exit code 1.

Local console logs are in ignored `results/week1-startup.log`,
`results/week1-recovery.log` and `results/week1-minio-persistence.log`.
These checks prove the Week 1 fixture gate, not Week 2 Bronze ingestion,
distributed executor scaling, or streaming recovery.

Configuration references: [Iceberg REST fixture source](https://github.com/apache/iceberg/tree/apache-iceberg-1.9.0/docker/iceberg-rest-fixture),
[Spark catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/),
and [MinIO pinned source](https://github.com/minio/minio/tree/RELEASE.2025-04-22T22-12-26Z).
