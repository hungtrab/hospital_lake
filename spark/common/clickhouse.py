"""Small synchronous HTTP client; publishing retries never hide a failed batch."""
from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


class ClickHouse:
    def __init__(self, url=None, user=None, password=None, retries=3):
        self.url = (url or os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")).rstrip("/")
        self.user = user or os.environ.get("CLICKHOUSE_USER", "hospital_writer")
        self.password = password if password is not None else os.environ.get("CLICKHOUSE_PASSWORD", "")
        if not self.password:
            raise ValueError("CLICKHOUSE_PASSWORD must be set")
        self.retries = retries
        if retries < 1:
            raise ValueError("retries must be positive")

    def execute(self, sql, data=b""):
        auth = base64.b64encode(f"{self.user}:{self.password}".encode()).decode()
        url = self.url + "/?" + urllib.parse.urlencode({"query": sql, "wait_end_of_query": 1})
        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(url, data=data, headers={"Authorization": "Basic " + auth})
                with urllib.request.urlopen(req, timeout=60) as response:
                    return response.read().decode()
            except urllib.error.HTTPError as exc:
                message = exc.read().decode("utf-8", "replace")[:1000]
                if exc.code < 500 or attempt + 1 == self.retries:
                    raise RuntimeError(f"ClickHouse HTTP {exc.code}: {message}") from exc
            except (urllib.error.URLError, TimeoutError):
                if attempt + 1 == self.retries:
                    raise
            time.sleep(2 ** attempt)

    def rows(self, sql):
        return [json.loads(line) for line in self.execute(sql + " FORMAT JSONEachRow").splitlines()]

    def insert(self, table, rows):
        allowed = {"hospital_snapshots", "department_snapshots", "lab_snapshots", "published_snapshots"}
        if table not in allowed:
            raise ValueError("unknown serving table")
        def encode(value):
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            raise TypeError(type(value).__name__)
        # Stream bounded chunks; no event-level writes or unbounded collect.
        chunk = []
        for row in rows:
            chunk.append(json.dumps(row, default=encode, allow_nan=False))
            if len(chunk) == 1000:
                self.execute(f"INSERT INTO hospital.{table} FORMAT JSONEachRow", ("\n".join(chunk)+"\n").encode())
                chunk = []
        if chunk:
            self.execute(f"INSERT INTO hospital.{table} FORMAT JSONEachRow", ("\n".join(chunk)+"\n").encode())
