# Realtime serving contract (Week 5, TV3)

This directory implements the table/column proposal in the Grafana handoff.
It stores absolute aggregate snapshots from TV1/TV2, not raw source events.
Run `make clickhouse-up` after copying `.env.example` to `.env`. The optional
`serving` profile starts ClickHouse with a persistent volume, creates database
`hospital`, three version tables, public query views, and `grafana_reader`.
Existing batch commands do not require starting this profile. Grafana uses the
same reader password; restart Grafana after changing its configuration.

Host Spark uses exported `CLICKHOUSE_ENDPOINT=http://localhost:8123`,
`CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD`. Container Spark uses
`http://clickhouse:8123`. The local bootstrap user is named `hospital_writer`;
it has administrative rights for initialization. Credentials in `.env.example`
are synthetic local defaults, not production secrets. Reader access is SELECT
only on `hospital.*`. The init script runs on fresh ClickHouse data volumes;
credential/schema migrations on existing volumes must be applied explicitly,
without deleting the volume. `CREATE ... IF NOT EXISTS` does not repair drift.

| Public query view | Aggregate input columns / logical key |
|---|---|
| `realtime_hospital_state` | `event_time` (key), `current_patients`, `icu_occupancy`, `admissions_5m`, `discharges_5m` |
| `realtime_department_metrics` | `window_start`, `department` (composite key), `admissions_5m`, `discharges_5m` |
| `realtime_lab_metrics` | `window_start` (key), `abnormal_labs` |

All inputs additionally require `revision` (UInt64). Time columns are Spark
TimestampType, session UTC (microseconds preserved as DateTime64(6)); counters and revision are nonnegative integers.
`icu_occupancy` is an occupied patient/bed count, not a percentage. Lab windows
are **non-overlapping one-minute buckets**, so Grafana's last-15-minute SUM does
not add overlapping rolling counts. Department snapshots are five-minute
rolling counts at each `window_start`; hospital state is a snapshot at event time.
TV1/TV2 own these calculations; the sink does not compute or invent them.

Use one writer per logical key. The producer must supply a monotonic revision
for each key and repeat exactly the same values for a key/revision on replay.
A corrected snapshot increments its revision; it replaces, rather than adds to,
the previous absolute value. Do not derive revisions solely from Spark batch ID
if the checkpoint may be reset. Resetting checkpoints requires a coordinated
revision strategy; it is not normal restart behavior. Deleting keys is not
supported. No atomic snapshot spanning tables/chunks is promised.

`ReplacingMergeTree(revision)` stores versions, while the public views query
`FINAL`, so repeated writes do not inflate dashboard SUMs before background
merges. Physical duplicates may remain until merges; the views expose the highest
revision. This is query-level replay safety, not an exactly-once transport claim.
See [ClickHouse's versioned replacement semantics](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).

```python
from spark.streaming.clickhouse_sink import start_stream
query = start_stream(aggregate_stream, table="realtime_department_metrics",
                     checkpoint_location="/persistent/checkpoints/department-serving")
query.awaitTermination()
```

This API attaches to TV1/TV2's aggregate DataFrames with `update` or `append`
output mode. Each callback sends bounded chunks via HTTP, retries transient
transport/server failures with a short backoff, and propagates exhausted errors
so Spark can retry the microbatch. Auth/validation failures stop immediately.
A partially successful callback can be retried because snapshots are immutable.
The sink retains the dashboard view/table names and needs no Kafka connector
or new Python library. It is not yet wired into a production aggregate job:
those TV1/TV2 aggregate modules are absent on the current main.

To run opt-in Spark → ClickHouse checks on a disposable local service:

```bash
make clickhouse-up
set -a; . ./.env; set +a
make test-clickhouse PYTHON=.venv/bin/python
```

The integration test writes unique lab snapshots, repeats a batch, writes a
correction, then replays an older revision and verifies one row with the newest
value through the public view. It also restarts a real file-source Structured Streaming query against a
persistent checkpoint, with explicit replay and a correction, and verifies one
public row with the corrected value. It cleans only its fixture timestamps.
It does not prove the Kafka → aggregations → dashboards weekly gate. Docker is unavailable in the implementation workspace; live validation
is pending. Unit tests prove HTTP retry/failure behavior and Spark serialization.
