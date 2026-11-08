# Week 5 Grafana (TV4)

Run `make dashboard-build` after editing panel SQL, then `make dashboard` (or
`docker compose up -d --wait`). Grafana is served at `http://localhost:3000`
unless `GRAFANA_PORT` is changed. Provisioning is written to load three
dashboards without UI setup; **this has not yet been run against a live stack**.
`GRAFANA_ADMIN_PASSWORD` falls back to `admin` when an older `.env`
lacks it; the port is published on loopback only.

After `make batch`, `make dashboard-smoke` confirms all three dashboards are
provisioned and runs every Trino-backed panel through Grafana, failing on query
errors, empty results, or a time series/stat panel with no numeric field.
It waits for the plugins, which Grafana installs in the background after
reporting healthy. ClickHouse panels are reported as `SKIP`.

Batch panels use the fixed default generator window (2025-01-01 to 2026-09-01)
because Gold holds nothing after it. A unit test ties these bounds to
`generators/batch_generator/config.yaml`; a custom window needs a new build.

The ClickHouse panels are prepared for the Week 5 TV1–TV3 serving handoff. They
expect database `hospital` with these columns:

| Table | Dashboard columns |
|---|---|
| `realtime_hospital_state` | `event_time`, `current_patients`, `icu_occupancy`, `admissions_5m`, `discharges_5m` |
| `realtime_department_metrics` | `window_start`, `department`, `admissions_5m`, `discharges_5m` |
| `realtime_lab_metrics` | `window_start`, `abnormal_labs` |

Those tables and the ClickHouse service are not on `main` yet, so the column
names and the `grafana_reader` account above are a proposal to reconcile with
TV3's schema and sink. Set `CLICKHOUSE_GRAFANA_PASSWORD` in `.env` to the
read-only account password. The realtime panels show query errors until the
serving tables exist; they do not display synthetic fallback numbers.

Overview's available-beds panel covers the ICU only because Gold has no
hospital-wide bed inventory. Its admissions and discharges use the latest Gold
day until a realtime "today" metric exists. The Realtime dashboard's incoming
events/sec, Kafka lag and streaming latency panels from `plan.md` need
Prometheus (Week 7) and are not included.

`python -m unittest discover -s tests/unit -p "test_grafana_*.py" -v` checks
that generated JSON is current and matches the Gold and provisioning contracts.
