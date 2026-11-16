"""Build checked-in Grafana dashboards from the repository's serving contracts."""

import json
from pathlib import Path


HERE = Path(__file__).resolve().parent
TRINO = {"type": "trino-datasource", "uid": "hospital-trino"}
CLICKHOUSE = {"type": "grafana-clickhouse-datasource", "uid": "hospital-clickhouse"}

# Default synthetic batch window (generators/batch_generator/config.yaml). Gold
# holds nothing after it, so relative "now-30d" ranges would render empty.
HISTORY_FROM = "2025-01-01T00:00:00Z"
HISTORY_TO = "2026-09-01T00:00:00Z"
RECENT_FROM = "2026-08-25T00:00:00Z"
LATEST_DAY = "(SELECT max(date) FROM iceberg.gold.department_daily_metrics)"


def panel(number, title, source, sql, *, kind="timeseries", y=0, x=0, width=12):
    # Both plugins read numeric formats (0=time series, 1=table); left unset,
    # the backend treats stat and table queries as time series.
    target = {"refId": "A", "format": 0 if kind == "timeseries" else 1}
    if source == CLICKHOUSE:
        target.update({"rawSql": sql, "editorType": "sql", "queryType": "table"})
    else:
        target["rawSQL"] = sql
    result = {
        "id": number, "title": title, "type": kind, "datasource": source,
        "targets": [target], "gridPos": {"h": 8, "w": width, "x": x, "y": y},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if kind == "stat":
        result["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
        result["gridPos"]["h"] = 5
    return result


def dashboard(uid, title, panels, *, refresh="", from_time="now-30d", to_time="now"):
    return {
        "uid": uid, "title": title, "schemaVersion": 39, "version": 1,
        "editable": False, "tags": ["HospitalLake", "synthetic-data"],
        "timezone": "utc", "time": {"from": from_time, "to": to_time},
        "refresh": refresh, "panels": panels, "templating": {"list": []},
    }


def build():
    # The ClickHouse columns below are a proposed TV1/TV2/TV3 handoff for
    # Week 5. Historical panels only reference Gold columns already in main.
    overview = dashboard("hospital-overview", "Hospital Overview", [
        panel(1, "Current patients", CLICKHOUSE,
              "SELECT current_patients FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", width=4),
        panel(2, "ICU occupancy", CLICKHOUSE,
              "SELECT icu_occupancy FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", x=4, width=4),
        panel(3, "Available ICU beds (latest Gold)", TRINO,
              'SELECT available_beds FROM iceberg.gold.icu_occupancy ORDER BY "timestamp" DESC LIMIT 1', kind="stat", x=8, width=4),
        panel(4, "Admissions (latest Gold day)", TRINO,
              f"SELECT sum(admissions) AS admissions FROM iceberg.gold.department_daily_metrics WHERE date = {LATEST_DAY}", kind="stat", x=12, width=6),
        panel(5, "Discharges (latest Gold day)", TRINO,
              f"SELECT sum(discharges) AS discharges FROM iceberg.gold.department_daily_metrics WHERE date = {LATEST_DAY}", kind="stat", x=18, width=6),
        panel(6, "Admissions by hour", TRINO,
              "SELECT hour AS time, admissions FROM iceberg.gold.hospital_hourly_metrics WHERE hour >= (SELECT max(hour) FROM iceberg.gold.hospital_hourly_metrics) - INTERVAL '7' DAY ORDER BY hour", y=5),
        panel(7, "Department occupancy", TRINO,
              f"SELECT department, unique_patients, bed_utilization FROM iceberg.gold.department_daily_metrics WHERE date = {LATEST_DAY} ORDER BY department", kind="table", y=5, x=12),
    ], refresh="1m", from_time=RECENT_FROM, to_time=HISTORY_TO)

    realtime = dashboard("realtime-operations", "Realtime Operations", [
        panel(1, "Current patients", CLICKHOUSE,
              "SELECT current_patients FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", width=6),
        panel(2, "Admissions last 5m", CLICKHOUSE,
              "SELECT admissions_5m FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", x=6, width=6),
        panel(3, "Discharges last 5m", CLICKHOUSE,
              "SELECT discharges_5m FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", y=5, width=6),
        panel(4, "Abnormal labs last 15m", CLICKHOUSE,
              "SELECT sum(abnormal_labs) AS abnormal_labs FROM hospital.realtime_lab_metrics WHERE window_start >= now() - INTERVAL 15 MINUTE", kind="stat", y=5, x=6, width=6),
        panel(6, "Current ICU occupancy", CLICKHOUSE,
              "SELECT icu_occupancy FROM hospital.realtime_hospital_state ORDER BY event_time DESC LIMIT 1", kind="stat", x=12, width=6),
        panel(5, "Department activity", CLICKHOUSE,
              "SELECT department, admissions_5m, discharges_5m FROM hospital.realtime_department_metrics WHERE window_start = (SELECT max(window_start) FROM hospital.realtime_department_metrics) ORDER BY department", kind="table", y=10),
    ], refresh="5s", from_time="now-1h")

    historical = dashboard("historical-analytics", "Historical Analytics", [
        panel(1, "Admissions by day", TRINO,
              "SELECT CAST(date AS timestamp) AS time, sum(admissions) AS admissions FROM iceberg.gold.department_daily_metrics GROUP BY date ORDER BY date"),
        panel(2, "Average length of stay (hours)", TRINO,
              "SELECT CAST(date AS timestamp) AS time, sum(avg_length_of_stay_hours * discharges) / nullif(sum(discharges), 0) AS hours FROM iceberg.gold.department_daily_metrics GROUP BY date ORDER BY date", x=12),
        panel(3, "Bed utilization trend (mean across departments)", TRINO,
              "SELECT CAST(date AS timestamp) AS time, avg(bed_utilization) AS utilization FROM iceberg.gold.department_daily_metrics GROUP BY date ORDER BY date", y=8),
        panel(4, "Top lab tests", TRINO,
              "SELECT test_name, sum(total_tests) AS total_tests FROM iceberg.gold.lab_daily_metrics GROUP BY test_name ORDER BY total_tests DESC LIMIT 10", kind="table", y=8, x=12),
        panel(5, "Abnormal lab rate", TRINO,
              "SELECT CAST(date AS timestamp) AS time, CAST(sum(abnormal_tests) AS double) / nullif(sum(total_tests), 0) AS abnormal_rate FROM iceberg.gold.lab_daily_metrics GROUP BY date ORDER BY date", y=16),
        panel(6, "Patients by department (patient-days)", TRINO,
              "SELECT department, sum(unique_patients) AS patient_days FROM iceberg.gold.department_daily_metrics GROUP BY department ORDER BY patient_days DESC", kind="table", y=16, x=12),
    ], from_time=HISTORY_FROM, to_time=HISTORY_TO)
    return {
        "hospital-overview.json": overview,
        "realtime-operations.json": realtime,
        "historical-analytics.json": historical,
    }


def main():
    output = HERE / "dashboards"
    output.mkdir(exist_ok=True)
    for filename, value in build().items():
        (output / filename).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
