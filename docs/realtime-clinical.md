# TV2 Week 5 realtime clinical metrics

`spark/streaming/clinical_metrics.py` implements TV2's ICU, department and lab
aggregation assignment in `works.md`. Existing batch/Bronze paths are unchanged.
The input is the shared Kafka envelope. The output is absolute aggregate
snapshots with increasing `revision`, plus raw quarantined events with reasons.

## Run locally

Install `requirements-spark.txt` in the host environment (Python 3.10+, Java
17+). Start Kafka with `make kafka-up`. The host runner uses the shared Spark
session; export the settings from `.env`, with host-reachable MinIO/catalog URLs
if also using Lakehouse jobs. No table reads/writes are needed by this runner.

```bash
set -a
. ./.env
set +a
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"  # macOS example; Java 17+ elsewhere
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
make stream-clinical PYTHON=.venv/bin/python
# Another terminal, same Python environment:
make produce PYTHON=.venv/bin/python KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RATE=100 DURATION=60
```

The default Kafka source uses `earliest` so ICU includes admissions already
retained in the topic. This is a census of observed ICU journeys: the source
must include admissions for all active encounters. Kafka retention/truncation
or starting at `latest` cannot reconstruct pre-existing patients. `TRANSFER`
currently changes beds within a department in the simulator; cross-department
transfers need an explicit event-contract extension before counting them.

`CLINICAL_METRICS_STATE_DIR` (or Make's `CLINICAL_STATE_DIR`) points to a
persistent **local filesystem/PVC**, owned by one driver. `CLINICAL_ARGS` passes
CLI overrides, for example `--bootstrap-servers localhost:19092`. It holds:

```text
identity.json                source/settings and revision epoch
buffers/accepted/            valid payloads before watermark/dedup
buffers/dedup/               valid events deduplicated within 10-minute watermark
checkpoints/<query>/         independent Spark source/state/sink recovery
output/quarantine/           original Kafka bytes (JSON base64) and reasons
output/department/batch-*/   five-minute department-window updates
output/lab/batch-*/          non-overlapping one-minute abnormal-lab counts
output/lab15m/batch-*/       explicit rolling 15-minute abnormal-lab windows
output/icu/batch-*/          current ICU census fragment for TV1
```

The Parquet buffers are internal replay state, not alternative analytical tables
in MinIO. Raw durable Lakehouse ingestion remains the existing Bronze job. Keep
the whole state directory across restarts. Source/settings changes or missing
child checkpoints/buffers fail visibly. A driver lock prevents two processes
sharing a state directory. Multiple different state directories must not write
the same ClickHouse keys. No concurrent-writer reconciliation is implemented.

## Metric meanings and handoff

All times are UTC. Event-time windows are `[window_start, window_end)` and
slide by one minute. Update-mode outputs are provisional until the watermark
passes the end; they do not wait ten minutes before first appearing. Duplicate
IDs within the watermark are counted once. Too-late events remain in the
accepted buffer for replay but do not alter window counts. Spark's dedup and
window aggregates run in separate queries because Spark 3.5 does not support
chaining them in update mode. Window state is evicted as event time advances;
an idle source does not advance the watermark using the wall clock.

| Output | Columns before `revision` | Consumer |
|---|---|---|
| department | `window_start`, `department`, `admissions_5m`, `discharges_5m` | TV3 `realtime_department_metrics` |
| lab | `window_start`, `abnormal_labs` | TV3 `realtime_lab_metrics`; sum disjoint minute buckets for a 15m range |
| lab15m | `window_start`, `abnormal_labs_last_15m` | Explicit 15m-window inspection; never load these overlapping windows into the lab serving table |
| icu | `event_time`, `icu_occupancy` | TV1 hospital-state composition; never write an incomplete `realtime_hospital_state` row |

Lab abnormal flags are recalculated from finite result/reference numbers, with
the endpoints normal. Payloads missing required IDs, unknown departments,
unsupported versions, nonfinite values or reversed reference ranges go to
quarantine. Other supported event types do not contribute to these metrics.

ICU uses a latest-event register per ICU encounter. A discharge wins a tie;
duplicates/older events cannot resurrect a discharged encounter. It reads valid
events **before** window-watermark rejection: a long stay must remain in the
census after its admission is older than ten minutes. Its complete-mode state
retains one register per observed ICU encounter, including closed encounters.
That state and the local replay/output logs grow with the run; pruning them
requires a coordinated checkpoint/replay policy and has not been implemented.
This is a configurable local milestone runner, not a long-running performance
or storage-boundedness claim. Seven queries share the Spark driver; begin at
100 events/s on a development machine and measure before increasing load.

Window rows are absolute counts, not increments. Read the highest revision per
logical key when inspecting local output. Department windows overlap: never sum
adjacent windows, and select a completed window (`start + 5 minutes <= as_of`)
when presenting a full five-minute period. The current Grafana department panel
selects the newest start, which can represent a still-partial window; its serving
handoff must reconcile this before claiming an exact trailing-five-minute KPI.
Similarly the existing lab panel's wall-clock filter excludes the first partial
minute; exact minute-aligned reporting should use fifteen completed buckets.

`revision` is a persisted microsecond epoch plus the query's batch ID. Checkpoint
restarts reuse it; a retry repeats the same values/revision. Atomic snapshot
directory publication is the local commit marker. A fresh independent root is
a new lifetime, not an ordinary restart: replay a complete source, ensure its
revision epoch exceeds prior published revisions, and coordinate serving keys
before cutover. Do not delete a checkpoint to fix an ordinary retry.

## Optional ClickHouse publishing

TV3's prerequisite PR #29 is not on `main` at implementation time. With its
`spark.streaming.clickhouse_sink` installed and ClickHouse configured, run:

```bash
make stream-clinical PYTHON=.venv/bin/python CLINICAL_ARGS=--clickhouse
```

This sends only department and one-minute lab snapshots through TV3's HTTP sink.
The command fails clearly if that dependency is missing. ICU/lab15m snapshots
remain available locally. TV1's hospital-state job is not implemented here, and
the full Kafka -> ClickHouse -> Grafana Week 5 gate is not certified by this PR.

## Validation

```bash
TZ=Asia/Ho_Chi_Minh make test PYTHON=.venv/bin/python
make test-clinical-stream PYTHON=.venv/bin/python
make kafka-up
make test-clinical-kafka PYTHON=.venv/bin/python
```

Set `JAVA_HOME` and `PYSPARK_PYTHON` as above for local Spark checks. The first
stream test uses actual file-source Structured Streaming queries, duplicates
across microbatches, quarantine, event-time lateness and a persistent-checkpoint
restart. The Kafka test creates/removes only its uniquely named fixture topic.
Opt-in service tests are skipped by ordinary `make test`.

Local evidence on 2026-11-17: known-value/payload/retry and real file-stream
checks passed (9 tests, 4 subtests, `TZ=Asia/Ho_Chi_Minh`). The Kafka fixture
also passed against Apache Kafka 3.9.1 using host Spark 4.0.2 and Java 21,
including actual malformed bytes and duplicates. The existing `kafka-smoke`
passed host/internal patient-key and DLQ transport probes. No throughput,
long-run stability, ClickHouse live or full Week 5 gate result is claimed.
