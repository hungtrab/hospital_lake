"""Execute the portable weekly comparison SQL against a local CSV fixture."""
from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
import json
from pathlib import Path
import re
import sqlite3
import tempfile

SQL_PATH = Path(__file__).resolve().parents[1] / "sql/trino/department_week_comparison.sql"


def iso_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("date must be YYYY-MM-DD")
    return date.fromisoformat(value)


def build_report(input_path: Path, as_of: str) -> list[dict]:
    end = iso_date(as_of)
    try:
        start, previous = end - timedelta(days=7), end - timedelta(days=14)
    except OverflowError as exc:
        raise ValueError("as-of must allow a 14-day lookback") from exc
    rows, seen = [], set()
    with Path(input_path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        if reader.fieldnames is not None and reader.fieldnames != ["date", "department", "admissions"]:
            raise ValueError("CSV header must be date,department,admissions")
        for line, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"line {line}: expected exactly three columns")
            iso_date(row["date"])
            department = row["department"]
            if not re.fullmatch(r"[A-Z][A-Z0-9_]*", department):
                raise ValueError(f"line {line}: department must be a canonical uppercase name")
            if not re.fullmatch(r"[0-9]+", row["admissions"]):
                raise ValueError(f"line {line}: admissions must be a nonnegative integer")
            admissions = int(row["admissions"])
            if admissions > 2**63 - 1:
                raise ValueError(f"line {line}: admissions exceeds SQL BIGINT")
            key = row["date"], department
            if key in seen:
                raise ValueError(f"line {line}: duplicate date/department: {key}")
            seen.add(key)
            rows.append((*key, admissions))
    connection = sqlite3.connect(":memory:")
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("CREATE TABLE department_daily_metrics (date TEXT, department TEXT, admissions INTEGER)")
        connection.execute("CREATE TABLE report_period (previous_start TEXT, current_start TEXT, current_end TEXT)")
        connection.executemany("INSERT INTO department_daily_metrics VALUES (?, ?, ?)", rows)
        connection.execute("INSERT INTO report_period VALUES (?, ?, ?)",
                           (previous.isoformat(), start.isoformat(), end.isoformat()))
        return [dict(row) for row in connection.execute(SQL_PATH.read_text(encoding="utf-8"))]
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    temporary = None
    try:
        result = build_report(args.input, args.as_of)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.output.parent,
                                         prefix=".week-report-", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(args.output)
    except (OSError, ValueError, csv.Error, sqlite3.Error) as exc:
        parser.error(str(exc))
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
