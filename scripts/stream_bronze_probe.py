"""Watch Iceberg Bronze ingest through Trino for five minutes (default)."""

import argparse
import csv
import re
import subprocess
import sys
import time

from .trino_smoke import cli_command, query


def monitor(read_stats, *, duration, interval, min_events, require_quarantine,
            max_idle=None, monotonic=time.monotonic, sleep=time.sleep):
    """Return new raw/quarantine rows; fail on stalls, loss, or duplicate offsets."""
    if duration <= 0 or interval <= 0 or min_events < 0 or (max_idle is not None and max_idle <= 0):
        raise ValueError("duration and interval must be positive; min_events nonnegative")
    initial = read_stats()
    if initial[2]:
        raise ValueError(f"duplicate Kafka coordinates: {initial[2]}")
    previous = initial
    last_progress = monotonic()
    deadline = last_progress + duration
    while monotonic() < deadline:
        sleep(min(interval, deadline - monotonic()))
        current = read_stats()
        if current[2]:
            raise ValueError(f"duplicate Kafka coordinates: {current[2]}")
        if current[0] < previous[0] or current[1] < previous[1]:
            raise ValueError("Bronze or quarantine row count decreased")
        if current[0] + current[1] > previous[0] + previous[1]:
            last_progress = monotonic()
        elif max_idle is not None and monotonic() - last_progress >= max_idle:
            raise ValueError(f"ingestion idle for {max_idle} seconds")
        previous = current
    new_raw = previous[0] - initial[0]
    new_bad = previous[1] - initial[1]
    if new_raw < min_events:
        raise ValueError(f"only {new_raw} valid events; expected at least {min_events}")
    if require_quarantine and new_bad == 0:
        raise ValueError("no new quarantined events")
    return new_raw, new_bad


def read_counts(command, catalog, timeout):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("catalog must be a simple SQL identifier")
    raw = f'"{catalog}"."bronze"."stream_events_raw"'
    bad = f'"{catalog}"."bronze"."stream_events_quarantine"'
    sql = ("WITH events AS ("
           f"SELECT _kafka_topic, _kafka_partition, _kafka_offset, false AS bad FROM {raw} "
           "UNION ALL "
           f"SELECT _kafka_topic, _kafka_partition, _kafka_offset, true AS bad FROM {bad}"
           ") SELECT count_if(NOT bad), count_if(bad), "
           "count(*) - count(DISTINCT concat(_kafka_topic, '/', "
           "cast(_kafka_partition AS varchar), '/', cast(_kafka_offset AS varchar))) "
           "FROM events")
    rows = query(command, sql, timeout)
    if len(rows) != 1 or len(rows[0]) != 3 or any(not re.fullmatch(r"[0-9]+", value)
                                                 for value in rows[0]):
        raise ValueError("Trino returned malformed Bronze counts")
    counts = tuple(int(value) for value in rows[0])
    print(f"Bronze raw={counts[0]} quarantine={counts[1]} duplicate_offsets={counts[2]}",
          flush=True)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=300, help="monitor seconds (default 300)")
    parser.add_argument("--interval", type=int, default=10, help="poll seconds (default 10)")
    parser.add_argument("--max-idle", type=int, default=60,
                        help="fail if no rows arrive for this many seconds (default 60)")
    parser.add_argument("--min-events", type=int, default=1)
    parser.add_argument("--require-quarantine", action="store_true")
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--server")
    parser.add_argument("--cli", default="trino")
    parser.add_argument("--compose", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    command = cli_command(args.cli, args.compose, args.server)
    try:
        new_raw, new_bad = monitor(
            lambda: read_counts(command, args.catalog, args.timeout),
            duration=args.duration, interval=args.interval, max_idle=args.max_idle,
            min_events=args.min_events, require_quarantine=args.require_quarantine)
    except (OSError, subprocess.TimeoutExpired, ValueError, csv.Error) as error:
        print(f"FAIL stream Bronze: {error}", file=sys.stderr)
        return 1
    print(f"PASS stream Bronze: +{new_raw} raw, +{new_bad} quarantined")
    return 0


if __name__ == "__main__":
    sys.exit(main())
