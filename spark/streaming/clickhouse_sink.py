"""Serve absolute aggregate snapshots, never raw hospital events, over HTTP.

TV1/TV2 supply one row per logical key and an immutable UInt64 revision.
A correction increases revision; retries repeat the same key/revision/values.
Spark batch IDs alone are not revisions across a checkpoint reset.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import json
import logging
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)
CONTRACTS = {
    "realtime_hospital_state": (
        "hospital_state_versions", "event_time",
        ("current_patients", "icu_occupancy", "admissions_5m", "discharges_5m"), ()),
    "realtime_department_metrics": (
        "department_metric_versions", "window_start", ("admissions_5m", "discharges_5m"), ("department",)),
    "realtime_lab_metrics": (
        "lab_metric_versions", "window_start", ("abnormal_labs",), ()),
}


@dataclass(frozen=True)
class ClickHouseConfig:
    endpoint: str = "http://localhost:8123"
    username: str = "hospital_writer"
    password: str = field(default="", repr=False)
    timeout: float = 10
    attempts: int = 3
    chunk_rows: int = 1000

    def __post_init__(self):
        url = urlsplit(self.endpoint)
        if (url.scheme not in ("http", "https") or not url.hostname
                or url.username or url.password or url.query or url.fragment):
            raise ValueError("CLICKHOUSE_ENDPOINT must be an HTTP(S) URL without credentials/query")
        if not self.username or not self.password:
            raise ValueError("ClickHouse username/password are required")
        if self.timeout <= 0 or self.attempts < 1 or self.chunk_rows < 1:
            raise ValueError("timeout, attempts and chunk_rows must be positive")

    @classmethod
    def from_env(cls):
        return cls(endpoint=os.environ.get("CLICKHOUSE_ENDPOINT", "http://localhost:8123"),
                   username=os.environ.get("CLICKHOUSE_USER", "hospital_writer"),
                   password=os.environ.get("CLICKHOUSE_PASSWORD", ""))


class ClickHouseClient:
    def __init__(self, config):
        self.config = config

    def execute(self, sql, data=b""):
        """Retry ambiguous transport errors; deterministic snapshots make replay safe."""
        config = self.config
        token = base64.b64encode(f"{config.username}:{config.password}".encode()).decode()
        request = Request(config.endpoint.rstrip("/") + "/?" + urlencode({"query": sql}),
                          data=data, headers={"Authorization": "Basic " + token}, method="POST")
        for attempt in range(config.attempts):
            try:
                with urlopen(request, timeout=config.timeout) as response:
                    return response.read()
            except HTTPError as error:
                # Auth/schema/validation errors are permanent. Do not expose response
                # bodies, SQL or credentials in our diagnostic exception.
                if error.code not in (429, 500, 502, 503, 504):
                    error.close()
                    raise RuntimeError(f"ClickHouse HTTP {error.code}") from None
                error.close()
                if attempt + 1 == config.attempts:
                    raise RuntimeError(f"ClickHouse HTTP {error.code} after retries") from None
            except (URLError, TimeoutError, OSError):
                if attempt + 1 == config.attempts:
                    raise RuntimeError("ClickHouse transport failed after retries") from None
            time.sleep(min(0.25 * 2 ** attempt, 2))

    def insert(self, table, rows):
        storage, timestamp, metrics, dimensions = CONTRACTS[table]
        fields = (timestamp, *dimensions, *metrics, "revision")
        for row in rows:
            if set(row) != set(fields):
                raise ValueError("aggregate row does not match ClickHouse contract")
            for name in (*metrics, "revision"):
                value = row[name]
                if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**64:
                    raise ValueError(f"{name} must be UInt64")
            if not isinstance(row[timestamp], str) or not row[timestamp]:
                raise ValueError("UTC timestamp string is required")
            if any(not isinstance(row[name], str) or not row[name].strip() for name in dimensions):
                raise ValueError("aggregate dimensions must be non-empty strings")
        payload = "".join(json.dumps(row, allow_nan=False) + "\n" for row in rows).encode()
        if rows:
            self.execute(f"INSERT INTO hospital.{storage} FORMAT JSONEachRow", payload)


def write_microbatch(frame, batch_id, *, table, config=None):
    """foreachBatch adapter for absolute aggregate updates with bounded HTTP chunks.

    Single writer per logical key. A key cannot change values at a fixed revision.
    Partial chunk success fails visibly; Spark retries repeat immutable snapshots.
    No atomicity across chunks/tables is claimed. Public FINAL views hide retry
    duplicates immediately, before background merges.
    """
    from pyspark.sql import functions as F

    config = config or ClickHouseConfig.from_env()
    storage, timestamp, metrics, dimensions = CONTRACTS[table]
    fields = (timestamp, *dimensions, *metrics, "revision")
    if set(frame.columns) != set(fields):
        raise ValueError("aggregate DataFrame columns must match the serving contract")
    if frame.isStreaming or batch_id < 0:
        raise ValueError("write_microbatch expects a batch DataFrame and nonnegative ID")
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC")
    # toJSON serializes Spark timestamps with an explicit UTC format instead of
    # converting collected naive datetime values through the host's timezone.
    schema = dict(frame.dtypes)
    if schema[timestamp] != "timestamp":
        raise ValueError("aggregate time column must be Spark TimestampType")
    conflicts = (frame.groupBy(timestamp, *dimensions, "revision")
                 .agg(F.countDistinct(F.struct(*metrics)).alias("variants"))
                 .where("variants > 1").limit(1).count())
    if conflicts:
        raise ValueError("conflicting values for the same aggregate key/revision")
    client = ClickHouseClient(config)
    count = 0
    chunk = []
    encoded = frame.select(F.to_json(F.struct(*fields), options={
        "timestampFormat": "yyyy-MM-dd HH:mm:ss.SSSSSS", "timeZone": "UTC",
        "ignoreNullFields": "false"}).alias("json"))
    for row in encoded.toLocalIterator():
        chunk.append(json.loads(row.json))
        if len(chunk) == config.chunk_rows:
            client.insert(table, chunk)
            count += len(chunk)
            chunk = []
    client.insert(table, chunk)
    count += len(chunk)
    LOG.info("job=clickhouse_sink table=%s batch_id=%d output_rows=%d", table, batch_id, count)


def start_stream(aggregates, *, table, checkpoint_location, config=None, output_mode="update"):
    """Attach to TV1/TV2 aggregate streams; checkpoint path must be persistent."""
    if table not in CONTRACTS or not checkpoint_location:
        raise ValueError("known serving table and persistent checkpoint are required")
    if output_mode not in ("append", "update"):
        raise ValueError("use append or update mode for versioned aggregate snapshots")
    config = config or ClickHouseConfig.from_env()
    return (aggregates.writeStream.outputMode(output_mode)
            .foreachBatch(lambda frame, batch_id: write_microbatch(
                frame, batch_id, table=table, config=config))
            .option("checkpointLocation", checkpoint_location).start())
