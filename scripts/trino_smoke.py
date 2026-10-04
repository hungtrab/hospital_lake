"""Verify an existing Spark-created Iceberg table through the local Trino CLI."""

import argparse
import csv
import io
import os
import re
import subprocess
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="catalog.schema.table created by Spark")
    parser.add_argument("--expected-rows", required=True, type=int)
    parser.add_argument("--server", default=None, help="Trino URL; defaults to TRINO_HOST/TRINO_PORT")
    parser.add_argument("--cli", default="trino", help="Trino CLI executable path")
    parser.add_argument("--timeout", type=int, default=60, help="overall timeout in seconds")
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
    table = ".".join(f'"{part}"' for part in args.table.split("."))
    command = [args.cli, "--server", server, "--output-format", "CSV",
               "--execute", f"SELECT count(*) FROM {table}"]
    try:
        result = subprocess.run(command, capture_output=True, text=True,
                                encoding="utf-8", timeout=args.timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"FAIL {args.table}: {error}", file=sys.stderr)
        return 1
    if result.returncode:
        print(f"FAIL {args.table}: Trino exited {result.returncode}: {result.stderr.strip()}",
              file=sys.stderr)
        return 1
    try:
        rows = list(csv.reader(io.StringIO(result.stdout), strict=True))
        if len(rows) != 1 or len(rows[0]) != 1 or not re.fullmatch(r"[0-9]+", rows[0][0]):
            raise ValueError("expected a single nonnegative integer count")
        actual = int(rows[0][0])
    except (ValueError, csv.Error) as error:
        print(f"FAIL {args.table}: invalid Trino result: {error}", file=sys.stderr)
        return 1
    if actual != args.expected_rows:
        print(f"FAIL {args.table}: expected {args.expected_rows} rows, got {actual}", file=sys.stderr)
        return 1
    print(f"PASS {args.table}: {actual} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
