"""Week 4 integration gate: producer -> Kafka -> Structured Streaming -> Bronze Iceberg.

Usage (reader already running via `make stream-bronze-docker`):
    python -m scripts.stream_gate --compose --rate 1000 --duration 600   # make stream-gate

The gate records Kafka end offsets, runs the producer at the requested rate,
samples Bronze (raw + quarantine) and Kafka every --interval seconds, then waits
for the reader to catch up. It passes only if every Kafka offset produced during
the run is in Bronze exactly once (contiguous per partition, no duplicates)
within --catch-up seconds after the producer stops. A JSON summary is written to
--report so the measurement can be compared across runs.
"""

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

from .trino_smoke import cli_command, query

ROOT = Path(__file__).resolve().parents[1]


def kafka_end_offsets(compose, topic, timeout=60):
    """Return {partition: end offset} from the Compose broker."""
    command = [*compose, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", "kafka:29092", "--topic", topic]
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise ValueError(f"kafka-get-offsets exited {result.returncode}: {result.stderr.strip()}")
    offsets = {}
    for line in result.stdout.split():
        match = re.fullmatch(re.escape(topic) + r":(\d+):(\d+)", line)
        if match:
            offsets[int(match.group(1))] = int(match.group(2))
    if not offsets:
        raise ValueError(f"no partitions reported for {topic}")
    return offsets


def coverage_sql(catalog, topic, start):
    """Per-partition rows/distinct/min/max for offsets produced after ``start``."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("catalog must be a simple SQL identifier")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", topic):
        raise ValueError("topic must be a plain Kafka topic name")
    since = " OR ".join(f"(_kafka_partition = {p} AND _kafka_offset >= {o})"
                        for p, o in sorted(start.items()))
    tables = [f'"{catalog}"."bronze"."stream_events_raw"',
              f'"{catalog}"."bronze"."stream_events_quarantine"']
    union = " UNION ALL ".join(
        f"SELECT _kafka_partition AS p, _kafka_offset AS o FROM {table} "
        f"WHERE _kafka_topic = '{topic}' AND ({since})" for table in tables)
    return (f"SELECT p, count(*), count(DISTINCT o), min(o), max(o) FROM ({union}) "
            "GROUP BY p ORDER BY p")


def read_coverage(command, sql, timeout):
    rows = query(command, sql, timeout)
    coverage = {}
    for row in rows:
        if len(row) != 5 or any(not re.fullmatch(r"[0-9]+", value) for value in row):
            raise ValueError(f"Trino returned malformed coverage row: {row}")
        partition, count, distinct, low, high = (int(value) for value in row)
        coverage[partition] = (count, distinct, low, high)
    return coverage


def check_coverage(start, end, coverage):
    """Return a list of problems; empty means every produced offset is in Bronze exactly once."""
    problems = []
    for partition in sorted(end):
        expected = end[partition] - start.get(partition, 0)
        count, distinct, low, high = coverage.get(partition, (0, 0, None, None))
        if count != distinct:
            problems.append(f"partition {partition}: {count - distinct} duplicate offsets")
        if expected and (distinct != expected or low != start.get(partition, 0)
                         or high != end[partition] - 1):
            problems.append(f"partition {partition}: {distinct}/{expected} offsets "
                            f"(range {low}..{high}, expected {start.get(partition, 0)}..{end[partition] - 1})")
    return problems


def total(offsets):
    return sum(offsets.values())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=float, default=1000)
    parser.add_argument("--duration", type=float, default=600)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--catch-up", type=float, default=180,
                        help="seconds allowed after the producer stops for Bronze to catch up")
    parser.add_argument("--topic", default="hospital.events")
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--compose", action="store_true", help="query Trino through the Compose service")
    parser.add_argument("--server")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--report", type=Path, default=ROOT / "results" / "stream-gate.json")
    args = parser.parse_args(argv)

    compose = [*shlex.split(os.environ.get("COMPOSE", "docker compose")), "--project-directory", str(ROOT),
               "-f", str(ROOT / "docker-compose.yml")]
    command = cli_command("trino", args.compose, args.server)
    samples = []
    try:
        start = kafka_end_offsets(compose, args.topic)
        sql = coverage_sql(args.catalog, args.topic, start)
        print(f"start offsets {start}; producing {args.rate:g} events/s for {args.duration:g}s", flush=True)
        producer = subprocess.Popen(
            [args.python, "-m", "generators.stream_generator.producer", "--bootstrap-servers",
             args.bootstrap_servers, "--topic", args.topic, "--rate", str(args.rate),
             "--duration", str(args.duration)], cwd=ROOT, stderr=subprocess.PIPE, text=True)
        started = time.monotonic()
        producer_done_at = None
        while True:
            time.sleep(args.interval)
            now = time.monotonic() - started
            end = kafka_end_offsets(compose, args.topic)
            ingested = sum(c[1] for c in read_coverage(command, sql, args.timeout).values())
            produced = total(end) - total(start)
            sample = {"t": round(now, 1), "kafka": produced, "bronze": ingested, "lag": produced - ingested}
            samples.append(sample)
            print(json.dumps(sample), flush=True)
            if producer_done_at is None and producer.poll() is not None:
                producer_done_at = now
            if producer_done_at is not None and sample["lag"] <= 0:
                break
            if producer_done_at is not None and now - producer_done_at > args.catch_up:
                break
        producer_log = producer.stderr.read().strip().splitlines()
        end = kafka_end_offsets(compose, args.topic)
        coverage = read_coverage(command, sql, args.timeout)
    except (OSError, subprocess.TimeoutExpired, ValueError, csv.Error) as error:
        print(f"FAIL stream gate: {error}", file=sys.stderr)
        return 1

    produced = total(end) - total(start)
    problems = check_coverage(start, end, coverage)
    if producer.returncode:
        problems.append(f"producer exited {producer.returncode}")
    last_stats = next((json.loads(line) for line in reversed(producer_log)
                       if line.startswith("{") and "events_sent" in line), {})
    report = {
        "rate_target": args.rate, "duration_seconds": args.duration,
        "kafka_messages": produced, "bronze_rows": sum(c[1] for c in coverage.values()),
        "producer": last_stats,
        "producer_events_per_second": last_stats.get("events_per_second"),
        "max_lag_messages": max((s["lag"] for s in samples), default=None),
        "catch_up_seconds": (round(samples[-1]["t"] - producer_done_at, 1)
                             if samples and producer_done_at is not None else None),
        "start_offsets": start, "end_offsets": end, "samples": samples,
        "problems": problems, "status": "FAIL" if problems else "PASS",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    summary = {k: report[k] for k in ("status", "kafka_messages", "bronze_rows",
                                      "producer_events_per_second", "max_lag_messages",
                                      "catch_up_seconds")}
    print(json.dumps(summary), flush=True)
    for problem in problems:
        print(f"FAIL stream gate: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
