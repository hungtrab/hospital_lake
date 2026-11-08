"""Verify provisioned Grafana dashboards and run their Trino panels through Grafana."""

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED = ("hospital-overview", "realtime-operations", "historical-analytics")
TRINO_UID = "hospital-trino"


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
    return sum(len(frame["data"]["values"][0]) for frame in result.get("frames", []) if frame["data"]["values"])


def main(argv=None):
    settings = {**env_file(ROOT / ".env"), **os.environ}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=f"http://localhost:{settings.get('GRAFANA_PORT') or 3000}")
    args = parser.parse_args(argv)
    login = f"{settings.get('GRAFANA_ADMIN_USER') or 'admin'}:{settings.get('GRAFANA_ADMIN_PASSWORD') or 'admin'}"
    auth = base64.b64encode(login.encode("utf-8")).decode("ascii")
    try:
        found = {item["uid"] for item in request(args.url, auth, "/api/search?type=dash-db")}
    except (urllib.error.URLError, ValueError, OSError) as error:
        print(f"FAIL grafana: {error}", file=sys.stderr)
        return 1
    missing = [uid for uid in EXPECTED if uid not in found]
    if missing:
        print(f"FAIL provisioned: missing {', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"PASS provisioned: {', '.join(EXPECTED)}")
    failures = 0
    for uid in EXPECTED:
        for panel in request(args.url, auth, f"/api/dashboards/uid/{uid}")["dashboard"]["panels"]:
            label = f"{uid} / {panel['title']}"
            if panel["datasource"]["uid"] != TRINO_UID:
                print(f"SKIP {label}: needs the Week 5 ClickHouse serving tables")
                continue
            try:
                rows = panel_rows(args.url, auth, panel)
                if not rows:
                    raise ValueError("query returned no rows")
                print(f"PASS {label}: {rows} rows")
            except (urllib.error.URLError, ValueError, KeyError, OSError) as error:
                failures += 1
                print(f"FAIL {label}: {error}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
