"""Run the source-controlled historical queries through Trino and fail loudly.

Usage:
    python -m scripts.run_queries --compose            # make query
    python -m scripts.run_queries --server http://localhost:8080

Every statement in ``sql/trino/queries.sql`` must start with a ``-- N. title``
comment. A statement that errors or returns no rows fails the run, so an empty
or missing Silver/Gold table cannot pass silently.
"""

import argparse
import csv
import io
import subprocess
import sys
import time
from pathlib import Path

from .trino_smoke import cli_command

SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "trino" / "queries.sql"


def load_queries(text):
    """Split on ';' and return (title, sql) pairs; every statement needs a title comment."""
    queries = []
    for statement in text.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        first = statement.splitlines()[0]
        if not first.startswith("--"):
            raise ValueError(f"query without a '-- title' comment: {statement[:60]!r}")
        queries.append((first.lstrip("- ").strip(), statement))
    if not queries:
        raise ValueError("no queries found")
    return queries


def run_query(command, sql, timeout):
    """Return (header, rows) for one statement; raise on CLI errors."""
    result = subprocess.run([*command, "--execute", sql], capture_output=True, text=True,
                            encoding="utf-8", timeout=timeout, check=False)
    if result.returncode:
        raise ValueError(f"Trino exited {result.returncode}: {result.stderr.strip()}")
    rows = list(csv.reader(io.StringIO(result.stdout), strict=True))
    if not rows:
        raise ValueError("Trino returned no header")
    return rows[0], rows[1:]


def format_table(header, rows):
    widths = [max(len(str(value)) for value in column) for column in zip(header, *rows)]
    lines = [" | ".join(name.ljust(width) for name, width in zip(header, widths)),
             "-+-".join("-" * width for width in widths)]
    lines += [" | ".join(value.ljust(width) for value, width in zip(row, widths)) for row in rows]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sql", type=Path, default=SQL_PATH)
    parser.add_argument("--compose", action="store_true", help="run the Trino CLI inside the Compose service")
    parser.add_argument("--cli", default="trino")
    parser.add_argument("--server")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--max-rows", type=int, default=10, help="rows printed per query")
    args = parser.parse_args(argv)

    command = cli_command(args.cli, args.compose, args.server)
    command[-1] = "CSV_HEADER"  # cli_command ends with --output-format CSV
    failures = 0
    queries = load_queries(args.sql.read_text(encoding="utf-8"))
    for title, sql in queries:
        started = time.monotonic()
        try:
            header, rows = run_query(command, sql, args.timeout)
            if not rows:
                raise ValueError("query returned no rows")
        except (OSError, subprocess.TimeoutExpired, ValueError, csv.Error) as error:
            failures += 1
            print(f"\nFAIL {title}\n  {error}", flush=True)
            continue
        shown = rows[:args.max_rows]
        more = f", showing {len(shown)}" if len(rows) > len(shown) else ""
        print(f"\n== {title} ({len(rows)} rows{more}, {time.monotonic() - started:.1f}s)")
        print(format_table(header, shown), flush=True)
    print(f"\n{len(queries) - failures}/{len(queries)} queries returned rows")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
