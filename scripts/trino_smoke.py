"""Verify an existing Spark-created Iceberg table through the local Trino CLI."""

import argparse
import csv
import io
import os
import re
import subprocess
import sys
from pathlib import Path


def query(command, sql, timeout):
    """Execute one read-only probe; preserve CLI diagnostics for the failing stage."""
    result = subprocess.run([*command, "--execute", sql], capture_output=True,
                            text=True, encoding="utf-8", timeout=timeout, check=False)
    if result.returncode:
        raise ValueError(f"Trino exited {result.returncode}: {result.stderr.strip()}")
    return list(csv.reader(io.StringIO(result.stdout), strict=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="catalog.schema.table created by Spark")
    parser.add_argument("--expected-rows", required=True, type=int)
    parser.add_argument("--server", default=None, help="Trino URL; defaults to TRINO_HOST/TRINO_PORT")
    transport = parser.add_mutually_exclusive_group()
    transport.add_argument("--cli", default="trino", help="Trino CLI executable path")
    transport.add_argument("--compose", action="store_true", help="use the CLI inside the Compose trino service")
    parser.add_argument("--check-stack", action="store_true", help="also verify engine, namespace, and data read")
    parser.add_argument("--timeout", type=int, default=60, help="timeout per query in seconds")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}", args.table):
        parser.error("--table must be catalog.schema.table using simple SQL identifiers")
    if args.expected_rows < 0:
        parser.error("--expected-rows must be nonnegative")
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    server = args.server or "http://{}:{}".format(
        os.environ.get("TRINO_HOST", "localhost"), os.environ.get("TRINO_PORT", "8080")
    )
    command = [args.cli]
    if args.compose:
        root = Path(__file__).resolve().parents[1]
        command = ["docker", "compose", "--project-directory", str(root),
                   "-f", str(root / "docker-compose.yml"), "exec", "-T", "trino", "trino"]
        server = args.server or "http://localhost:8080"
    command += ["--server", server, "--output-format", "CSV"]
    table = ".".join(f'"{part}"' for part in args.table.split("."))
    stage = "count"
    try:
        if args.check_stack:
            stage = "engine"
            if query(command, "SELECT 1", args.timeout) != [["1"]]:
                raise ValueError("SELECT 1 did not return 1")
            print("PASS engine: SELECT 1")
            stage = "catalog"
            catalog, schema, _ = args.table.split(".")
            namespaces = query(command, f'SHOW SCHEMAS FROM "{catalog}"', args.timeout)
            if [schema] not in namespaces:
                raise ValueError(f"namespace {schema} is not visible in {catalog}")
            print(f"PASS catalog: {catalog}.{schema}")
        stage = "count"
        rows = query(command, f"SELECT count(*) FROM {table}", args.timeout)
        if len(rows) != 1 or len(rows[0]) != 1 or not re.fullmatch(r"[0-9]+", rows[0][0]):
            raise ValueError("expected a single nonnegative integer count")
        actual = int(rows[0][0])
        if actual != args.expected_rows:
            raise ValueError(f"expected {args.expected_rows} rows, got {actual}")
        print(f"PASS count {args.table}: {actual} rows")
        if args.check_stack:
            stage = "data"
            sample = query(command, f"SELECT * FROM {table} LIMIT 1", args.timeout)
            if len(sample) != min(actual, 1) or (sample and not sample[0]):
                raise ValueError("data sample does not match the observed count; use a stable fixture")
            print(f"PASS data: {len(sample)} sample row(s) read")
    except (OSError, subprocess.TimeoutExpired, ValueError, csv.Error) as error:
        print(f"FAIL {stage} {args.table}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
