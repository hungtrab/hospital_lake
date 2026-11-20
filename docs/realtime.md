# Realtime pipeline

The local implementation now connects Kafka → Spark Structured Streaming →
Iceberg/ClickHouse → Grafana. All data is synthetic. Historical analytics remain
in Iceberg/Trino; ClickHouse receives only aggregate snapshots.

## Run

```bash
make setup
make up
make realtime-up
make stream
make monitoring                  # optional Prometheus + Pipeline Monitoring dashboard
make realtime-recovery PYTHON=.venv/bin/python
make produce PYTHON=.venv/bin/python KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RATE=100 DURATION=60
```

Copy new ClickHouse settings from `.env.example` if upgrading an older `.env`.
The example passwords are only for the local synthetic demo. `realtime-up`
creates the writer and the Grafana read-only account. The latter may change only
the query timeout setting required by the plugin, as described in the
[Grafana configuration guide](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/).

Run the known-event smoke/recovery test on a quiet stack, without another producer.
It checks current patients N→N+1→N, duplicates, late events after watermark
advancement, recomputed lab flags, all eight realtime panels, optional process
restart, and exact Kafka coordinates/content hashes through Trino. A failed test
can leave its clearly prefixed synthetic encounter active; its logs identify the
run. Use an isolated Compose project for repeatable testing.

For an isolated project, pass the same project/env file to every command:

```bash
make realtime-recovery PYTHON=.venv/bin/python \
  COMPOSE='docker compose -p hospital-demo --env-file results/demo.env' \
  REALTIME_ENV=results/demo.env
```

Set distinct host ports in that env file if another stack is running. Keep
`CLICKHOUSE_URL` and `KAFKA_BOOTSTRAP_SERVERS` consistent with those host ports.
`make realtime-stop` stops the realtime worker and preserves state. Stop the
optional services with `docker compose --profile kafka --profile realtime
--profile monitoring stop`; do not delete volumes to simulate recovery.

## Processing contract

- The raw query retains every Kafka record, including duplicate event IDs.
  Valid records go to `bronze.stream_events_raw`; malformed/invalid payloads go
  to `bronze.stream_events_quarantine` with their original bytes and reasons.
  Both are idempotent by topic/partition/offset. The two writes are retry-safe,
  but are not a multi-table transaction. There is no Kafka DLQ publisher in this
  path; the persistent Iceberg quarantine is the implemented error sink.
- The serving query validates payloads, normalizes departments, rejects timestamps
  more than five minutes in the future, and uses Spark's ten-minute event-time
  watermark with event-ID deduplication. Watermarks propagate over completed
  micro-batches; late records are not guaranteed to be rejected immediately at
  startup. See [Spark's watermark/deduplication semantics](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#streaming-deduplication).
- `silver.realtime_events` is a narrow accepted-event ledger partitioned by day.
  `silver.realtime_encounters` holds the latest lifecycle state and discharged
  tombstones. Encounter identity belongs to the first observed patient. Identity
  conflicts and admission/transfer events after discharge are retained in
  `silver.realtime_rejected`, linked to Bronze by event ID. A new admission after
  discharge needs a new encounter ID.
- An older admission can establish that a transfer belongs to an admitted
  encounter; it cannot reopen a discharged encounter. Current-patient and ICU
  counts use distinct patient IDs among admitted, non-discharged encounters.
  They reflect accepted realtime events, start from empty state and are not
  seeded from historical batch data. Events discarded beyond the watermark
  require a separate replay/reconciliation; this is not a clinical census.
- Spark calculates department admission/discharge and abnormal-lab counts in
  one-second event-time buckets. ClickHouse views apply rolling 5/15-minute
  bounds at query time, so windows expire even when no new events arrive.
- A writer inserts department/lab/hospital snapshots, then publishes their common
  version. Unpublished partial writes stay invisible. ReplacingMergeTree with
  `FINAL` makes HTTP insert retries safe for these keys. This is a single-writer
  design, not a general exactly-once or cross-query transaction guarantee.
- Both streaming queries have separate persistent checkpoints beneath
  `REALTIME_CHECKPOINT_LOCATION`. Do not run another serving writer on the same
  Silver tables/checkpoints. Bronze and serving can temporarily be at different
  Kafka offsets; monitor both.

## Metrics and retention

`http://localhost:8090/metrics` exposes completed micro-batch rates, duration,
late/duplicate counts and state memory. Kafka backlog is broker log-end minus
each query's last completed end offset. Structured Streaming manages progress in
its checkpoint, so a Kafka consumer-group lag exporter alone would be misleading.
`/progress` exposes Spark's last progress JSON. Prometheus is at port 9090.

ClickHouse's `latency_p50_seconds` / `latency_p95_seconds` estimate per-batch time
from Kafka arrival to the hospital snapshot write (approximate timestamp
percentiles). They are not global percentiles, and exclude subsequent Grafana
refresh time. `invalid_events` counts records already visible in Bronze
quarantine when that snapshot is calculated; the raw query can lag behind.

```bash
COMPOSE='docker compose' .venv/bin/python -m scripts.realtime_benchmark \
  --rate 100 --duration 1800 --restart-at 900
.venv/bin/python -m scripts.analyze_benchmark results/realtime-benchmark
.venv/bin/python -m scripts.realtime_maintenance --hours 24
```

The benchmark writes JSONL samples, Docker resource use, a Bronze coverage
report and a final summary under `results/realtime-benchmark/`. A successful
run must also drain the serving query to the workload's final Kafka offsets.
The runner queries Trino through HTTP at `TRINO_URL`, or
`http://localhost:${TRINO_PORT:-8080}`. The standalone Bronze gate accepts
`--trino-url URL` / `TRINO_URL` too; its older CLI mode remains available.
Compose uses Trino's shipped curl health check, avoiding another Java process
inside the server's memory limit for every probe. The gate obtains Kafka end
offsets using a short-lived Python consumer without a consumer group or commits;
it uses the same host endpoint as the producer. The broker's protocol health
check retains its CLI but bounds that extra JVM's heap to 16–64 MiB.

The analyzer writes `validated-summary.json` with evidence hashes. It checks
observation gaps, actual latency/resource measurements, duration, restart and
both queries' final offsets. It never upgrades a failed runner result to PASS.
Its maximum per-batch p95 is not a global event latency percentile. Kafka offset
differences, rather than Spark's repeated input scans inside `foreachBatch`,
measure the number of records processed per observed batch.

The maintenance command needs the admin password in the env file. It removes
old aggregate snapshots, always keeping the latest publication and its data,
including when the stream has been idle longer than the retention period.
Run it periodically; there is no automatic maintenance scheduler. Iceberg
history/tombstones are retained for replay and require explicit maintenance.

To measure file compaction, first stop the producer, wait until both queries
reach Kafka's end offsets, and stop the stream writer. Then run:

```bash
make realtime-stop
make compact PYTHON=.venv/bin/python COMPACT_TABLE=iceberg.bronze.stream_events_raw
make stream
```

`results/compaction.json` records before/after file counts, total bytes, full-row
hash sum, row count, rewrite duration and scan duration. Count/hash changes fail
the job. A single scan timing includes cache/JIT effects; it is not proof of a
general query speedup. Compaction preserves prior snapshots and does not expire
them or automatically reclaim their old files.

## Validation

```bash
make test PYTHON=.venv/bin/python
RUN_REALTIME_TESTS=1 .venv/bin/python -m pytest tests/integration/test_realtime_state.py -q
make test-stream-bronze PYTHON=.venv/bin/python
```

Use Java 17 for host PySpark. The local state test uses an isolated temporary
Iceberg warehouse and verifies order, identity conflicts, terminal discharge
and replay, including on UTC+7 hosts. `tests/integration/realtime_contract_smoke.py`
additionally checks actual Spark→Iceberg→ClickHouse publication/idle-window
behavior; it requires a new empty isolated stack and `LIVE_SMOKE_ISOLATED=1`.
See `progress.md` for the latest measured results and remaining milestones.
