"""Week 4 integration gate: producer -> Kafka -> Structured Streaming -> Bronze Iceberg.

Usage (reader already running via `make stream-bronze-docker`):
    python -m scripts.stream_gate --compose --rate 1000 --duration 600   # make stream-gate

The gate records Kafka end offsets, runs the producer at the requested rate,
samples Bronze (raw + quarantine) and Kafka every --interval seconds, then waits
for the reader to catch up. It passes only if every Kafka offset produced during
the run is in Bronze exactly once (contiguous per partition, no duplicates)
within --catch-up seconds after the producer stops, and only if Kafka received the
full rate x duration quota. A JSON summary is written to --report (producer log
beside it) so the measurement can be compared across runs.
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


def evaluate(start, end, coverage, *, rate, duration, producer_returncode,
             catch_up_seconds, catch_up_limit):
    """Return gate problems; empty means PASS."""
    problems = check_coverage(start, end, coverage)
    quota = int(rate * duration)
    produced = total(end) - total(start)
    if produced < quota:
        problems.append(f"only {produced}/{quota} messages reached Kafka "
                        f"({rate:g} events/s for {duration:g}s not sustained)")
    if producer_returncode:
        problems.append(f"producer exited {producer_returncode}")
    if catch_up_seconds is None:
        problems.append(f"Bronze did not catch up within {catch_up_limit:g}s after the producer stopped")
    elif catch_up_seconds > catch_up_limit:
        problems.append(f"catch-up took {catch_up_seconds:g}s (limit {catch_up_limit:g}s)")
    return problems


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
    if args.rate <= 0 or args.duration <= 0 or args.interval <= 0 or args.catch_up <= 0:
        parser.error("--rate, --duration, --interval and --catch-up must be positive")

    compose = [*shlex.split(os.environ.get("COMPOSE", "docker compose")), "--project-directory", str(ROOT),
               "-f", str(ROOT / "docker-compose.yml")]
    command = cli_command("trino", args.compose, args.server)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    producer_log_path = args.report.with_suffix(".producer.log")
    samples = []
    producer = None
    producer_done_at = catch_up_seconds = None
    try:
        start = kafka_end_offsets(compose, args.topic)
        sql = coverage_sql(args.catalog, args.topic, start)
        print(f"start offsets {start}; producing {args.rate:g} events/s for {args.duration:g}s", flush=True)
        with producer_log_path.open("w") as producer_log:  # a file never blocks the producer
            producer = subprocess.Popen(
                [args.python, "-m", "generators.stream_generator.producer", "--bootstrap-servers",
                 args.bootstrap_servers, "--topic", args.topic, "--rate", str(args.rate),
                 "--duration", str(args.duration)], cwd=ROOT, stderr=producer_log)
        started = time.monotonic()
        deadline = args.duration + args.catch_up + 3 * args.interval + 120
        while time.monotonic() - started < deadline:
            time.sleep(args.interval)
            # Detect the producer's exit before the slow queries so catch-up is not overstated.
            if producer_done_at is None and producer.poll() is not None:
                producer_done_at = time.monotonic() - started
            end = kafka_end_offsets(compose, args.topic)
            ingested = sum(c[1] for c in read_coverage(command, sql, args.timeout).values())
            now = time.monotonic() - started
            produced = total(end) - total(start)
            sample = {"t": round(now, 1), "kafka": produced, "bronze": ingested, "lag": produced - ingested}
            samples.append(sample)
            print(json.dumps(sample), flush=True)
            if producer_done_at is not None:
                if sample["lag"] <= 0:
                    catch_up_seconds = round(now - producer_done_at, 1)
                    break
                if now - producer_done_at > args.catch_up:
                    break
        end = kafka_end_offsets(compose, args.topic)
        coverage = read_coverage(command, sql, args.timeout)
    except (OSError, subprocess.TimeoutExpired, ValueError, csv.Error) as error:
        print(f"FAIL stream gate: {error}", file=sys.stderr)
        return 1
    finally:
        if producer is not None and producer.poll() is None:
            producer.terminate()
            producer.wait(timeout=30)

    producer_lines = producer_log_path.read_text().splitlines()
    last_stats = next((json.loads(line) for line in reversed(producer_lines)
                       if line.startswith("{") and "events_sent" in line), {})
    problems = evaluate(start, end, coverage, rate=args.rate, duration=args.duration,
                        producer_returncode=producer.returncode, catch_up_seconds=catch_up_seconds,
                        catch_up_limit=args.catch_up)
    report = {
        "rate_target": args.rate, "duration_seconds": args.duration,
        "interval_seconds": args.interval, "catch_up_limit_seconds": args.catch_up,
        "kafka_messages": total(end) - total(start),
        "bronze_rows": sum(c[1] for c in coverage.values()),
        "producer": last_stats,
        "producer_events_per_second": last_stats.get("events_per_second"),
        "max_lag_messages": max((s["lag"] for s in samples), default=None),
        # Measured at sampling granularity: an upper bound within one --interval.
        "catch_up_seconds": catch_up_seconds,
        "start_offsets": start, "end_offsets": end, "samples": samples,
        "problems": problems, "status": "FAIL" if problems else "PASS",
    }
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
