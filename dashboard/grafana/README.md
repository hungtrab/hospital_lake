# Week 5 Grafana definitions (TV4)

This directory holds the dashboards and provisioning as code. **No Grafana
service runs from `main` yet**: the Compose service and the live smoke check are
in a separate change until they have been run on a machine with Docker, so
these files have not been loaded by a real Grafana.

Run `make dashboard-build` after editing panel SQL. Three dashboards are
generated: Hospital Overview, Realtime Operations and Historical Analytics.

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
TV3's schema and sink.

Overview's available-beds panel covers the ICU only because Gold has no
hospital-wide bed inventory. Its admissions and discharges use the latest Gold
day until a realtime "today" metric exists. The Realtime dashboard's incoming
events/sec, Kafka lag and streaming latency panels from `plan.md` need
Prometheus (Week 7) and are not included.

`python -m unittest discover -s tests/unit -p "test_grafana_*.py" -v` checks
that generated JSON is current and matches the Gold and provisioning contracts.
