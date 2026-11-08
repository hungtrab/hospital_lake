"""Verify provisioned Grafana dashboards and run their Trino panels through Grafana."""

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED = ("hospital-overview", "realtime-operations", "historical-analytics")
TRINO_UID = "hospital-trino"
PLUGINS = ("trino-datasource", "grafana-clickhouse-datasource")
ERRORS = (ValueError, KeyError, OSError)


def env_file(path):
    """Read KEY=VALUE pairs so host runs use the same login as Compose."""
    if not path.is_file():
        return {}
    lines = (line.strip() for line in path.read_text(encoding="utf-8").splitlines())
    return dict(line.split("=", 1) for line in lines if line and not line.startswith("#") and "=" in line)


def request(base, auth, path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(base + path, data=data, headers={
        "Authorization": f"Basic {auth}", "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise ValueError(f"HTTP {error.code}: {error.read().decode('utf-8', 'replace')[:500]}") from error


def panel_rows(base, auth, panel):
    """Run one panel through Grafana so plugin, datasource and SQL are all exercised."""
    # Panel SQL carries its own bounds; Grafana only requires some time range.
    query = {**panel["targets"][0], "datasource": panel["datasource"]}
    response = request(base, auth, "/api/ds/query", {"queries": [query], "from": "now-1h", "to": "now"})
    result = response["results"][query["refId"]]
    if result.get("error"):
        raise ValueError(result["error"])
    frames = result.get("frames", [])
    rows = sum(len(frame["data"]["values"][0]) for frame in frames if frame["data"]["values"])
    fields = [field.get("type") for frame in frames for field in frame["schema"]["fields"]]
    if rows and panel["type"] != "table" and "number" not in fields:
        raise ValueError(f"no numeric field to plot, got {fields}")
    return rows


def wait_for_plugins(base, auth, timeout):
    """Preinstall runs in the background, so /api/health can pass before plugins exist."""
    deadline = time.monotonic() + timeout
    while True:
        missing = []
        for plugin in PLUGINS:
            try:
                request(base, auth, f"/api/plugins/{plugin}/settings")
            except ValueError:
                missing.append(plugin)
        if not missing:
            return
        if time.monotonic() >= deadline:
            raise ValueError(f"plugins not installed after {timeout}s: {', '.join(missing)}")
        time.sleep(5)


def check_dashboard(base, auth, uid):
    """Return the failure count for one dashboard, labelling each failure."""
    try:
        panels = request(base, auth, f"/api/dashboards/uid/{uid}")["dashboard"]["panels"]
    except ERRORS as error:
        print(f"FAIL {uid}: {error}", file=sys.stderr)
        return 1
    failures = 0
    for panel in panels:
        label = f"{uid} / {panel.get('title', 'untitled')}"
        if (panel.get("datasource") or {}).get("uid") != TRINO_UID:
            print(f"SKIP {label}: not Trino-backed (ClickHouse needs the Week 5 serving tables)")
            continue
        try:
            rows = panel_rows(base, auth, panel)
            if not rows:
                raise ValueError("query returned no rows")
            print(f"PASS {label}: {rows} rows")
        except ERRORS as error:
            failures += 1
            print(f"FAIL {label}: {error}", file=sys.stderr)
    return failures


def main(argv=None):
    settings = {**env_file(ROOT / ".env"), **os.environ}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=f"http://localhost:{settings.get('GRAFANA_PORT') or 3000}")
    parser.add_argument("--plugin-timeout", type=int, default=180, help="seconds to wait for background plugin install")
    args = parser.parse_args(argv)
    login = f"{settings.get('GRAFANA_ADMIN_USER') or 'admin'}:{settings.get('GRAFANA_ADMIN_PASSWORD') or 'admin'}"
    auth = base64.b64encode(login.encode("utf-8")).decode("ascii")
    try:
        wait_for_plugins(args.url, auth, args.plugin_timeout)
        found = {item.get("uid") for item in request(args.url, auth, "/api/search?type=dash-db")}
    except ERRORS as error:
        print(f"FAIL grafana: {error}", file=sys.stderr)
        return 1
    missing = [uid for uid in EXPECTED if uid not in found]
    if missing:
        print(f"FAIL provisioned: missing {', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"PASS provisioned: {', '.join(EXPECTED)}")
    return 1 if sum(check_dashboard(args.url, auth, uid) for uid in EXPECTED) else 0


if __name__ == "__main__":
    raise SystemExit(main())
