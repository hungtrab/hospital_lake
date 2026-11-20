# Measured validation — 16 November 2026

These are local synthetic-data experiments, not production capacity claims.
The tests run on one machine with approximately 30.6 GiB RAM and 16 processors
visible to Docker. The realtime profile uses Spark `local[2]`, a 1 GiB driver
heap and a 3 GiB container limit. Trino has a 1 GiB heap and a 2 GiB limit.
The early mixed-workload runs shared the machine with a partial kind cluster;
that cluster is stopped for the final realtime-only run.

## Current evidence

| Experiment | Observed result | Evidence under `results/completion/` |
|---|---|---|
| Known admission/lab/duplicate/late/invalid events, Grafana, stream restart, discharge | PASS; 58.14 seconds after catalog fix | `realtime-catalog-recovery.json` |
| Recovery after SQLite catalog snapshot-lock failure | All 207,295 Kafka offsets present exactly once after restart; checkpoints preserved | `catalog-recovery.json` |
| Batch image, 1,000 patients, seed 42 | PASS; 36,281 source rows through all stages in 159.41 seconds | `batch-image-live.log` |
| Replay of the same batch image | PASS; all six Bronze entities skipped existing batch data; Silver/Gold rebuilt | `batch-image-rerun.log` |
| Historical queries against that batch | All 11 source-controlled SQL queries returned rows | `batch-image-queries.json` |
| Realtime workload, 100 events/s for 30 minutes, restart halfway | PASS; 180,000 Kafka messages and 180,000 Bronze coordinates; both queries drained | `stress-light-probes-100/validated-summary.json` |
| Iceberg compaction on the stopped stream table | 1,821 files → 1; 452,410 rows and checksum unchanged | `compaction-live.json`, `compaction-offsets.json` |
| Full repository test suite | 226 passed, 12 skipped, 88 subtests passed | `full-tests-final.log` |
| Kubernetes manifests | 13 files accepted by server-side dry-run; this does not execute their workloads | `k8s-server-dryrun.log` |

The batch image ran with two CPUs and a 2 GiB memory limit while the 100 event/s
stream was active. Its single-run time includes generation and all pipeline
stages. It is not an executor-scaling comparison. A replay of the same batch
skips Bronze writes and must not be compared with first ingestion as a speedup.

## Sustained stream experiment

```bash
COMPOSE='docker compose' .venv/bin/python -m scripts.realtime_benchmark \
  --rate 100 --duration 1800 --restart-at 900
.venv/bin/python -m scripts.analyze_benchmark results/realtime-benchmark
```

The runner measures Kafka offsets, raw/quarantine coverage, Spark progress,
serving snapshots and Docker resources. PASS requires the full producer quota,
coverage without missing/duplicate coordinates, actual measurements and serving
catch-up. A planned restart has a bounded observation grace period; gaps
elsewhere fail. The analyzer preserves the original report and never turns a
failed runner result into PASS.

Earlier attempts are retained as **FAIL**:

- `stress-100/`: deliberately stopped short to fix independent audit findings.
- `stress-final-100/`: Trino was OOM-killed around minute 18.
- `stress-http-100/`: SQLite reported `SQLITE_BUSY_SNAPSHOT` after catalog
  recreation, causing repeated stream restarts; stopped after about four minutes.
- `stress-catalog-http-100/`: offset CLI timed out after 60 seconds during severe
  host memory pressure (under 1 GiB available), so the gate stopped near minute 12.
  Trino, Kafka and Spark remained running; all 275,829 accumulated Kafka coordinates
  were checked after recovery (`probe-recovery.json`).

Trino probes now use HTTP/curl rather than starting a CLI JVM per probe.
The SQLite-backed local catalog uses `CATALOG_CLIENTS=1`. Kafka offset probes now use Python, and the broker's health-check JVM heap is
bounded to 64 MiB. The final run `stress-light-probes-100/` passed. No batch, build or full test suite runs alongside that measurement.
The host still runs other desktop applications, so this is not an isolated
maximum-throughput benchmark.

`latency_p95_seconds` is a per-batch estimate from Kafka arrival to publication,
excluding Grafana refresh. The maximum of observed batch p95 values is not a
global p95. Spark's reported input row rate may count repeated scans within
`foreachBatch`; the analyzer uses Kafka offset differences for event throughput.

## Final local measurements

The producer sent 180,000 messages at 100/s. At the last coverage sample
(`t=1820.2s`), all were in Bronze once, and both queries subsequently showed the
same workload end offsets. The stream restart was requested at `t=908.56s`;
both queries had completed batches again by the sample at `t=941.84s`.
There were no unexpected stream restarts or container OOM kills in this run.

The maximum **observed nonempty workload batch p95** latency was 19.99 seconds;
the peak sampled Spark container memory was approximately 2.75 GiB. The analyzer
excludes idle snapshots carrying latency from an earlier run. The original
runner's `catch_up_seconds=0.5` field underestimated time because it noticed
producer exit late; **do not use that field as a measurement**. The conservative
observed bound is about 20.2 seconds after the 1,800-second workload, within the
600-second gate limit. A separate producer-exit waiter fixes future measurements;
its real 10-second/100-message regression run passed (`timing-gate.json`).

With stream writers stopped, raw-table compaction rewrote 1,821 files into one
in 8.306 seconds. Current-snapshot data bytes changed from 43,019,900 to
34,030,166; all 452,410 rows and their hash sum were preserved. Trino then
confirmed all 455,829 accumulated raw/quarantine coordinates. The before/after
scan measurements were 10.9804s and 0.4821s; cache/JIT effects and the single-run
sample prevent treating that ratio as a general speedup. Old snapshot files
are retained, so these byte counts do not imply reclaimed disk space. Resuming
the stream naturally creates new files again.

## Remaining performance gates

Actual Kubernetes executor scaling, active-workload pod failure recovery,
controlled batch size/executor comparisons and a fresh full deployment are
not yet certified. The MinIO compaction experiment is complete for this local
table, not for a production-scale dataset.

The local catalog configuration follows Iceberg's
[JDBC client-pool implementation](https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/core/src/main/java/org/apache/iceberg/jdbc/JdbcClientPool.java).
WAL is retained as described in the upstream
[SQLite fixture issue](https://github.com/apache/iceberg/issues/18043); the measured
snapshot-lock failure required further serializing this local catalog's JDBC
connections. This remains a single-instance demo catalog.
