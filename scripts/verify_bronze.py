"""Compare generated CSV/Parquet IDs with one Iceberg Bronze batch via Trino."""

import argparse
import csv
import hashlib
import re
import sys
from pathlib import Path

from .trino_smoke import cli_command, query


ENTITY_IDS = {
    "patients": "patient_id",
    "encounters": "encounter_id",
    "lab_results": "lab_result_id",
    "medications": "medication_id",
    "vitals": "measurement_id",
    "bed_events": "event_id",
}
SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "trino" / "verify_bronze.sql"


def source_ids(directory, entity, id_column):
    """Read stable IDs from exactly one source file; reject missing or null keys."""
    csv_path = directory / f"{entity}.csv"
    parquet_path = directory / f"{entity}.parquet"
    present = [path for path in (csv_path, parquet_path) if path.exists()]
    if len(present) != 1:
        raise ValueError(f"expected exactly one CSV or Parquet file for {entity} in {directory}")
    ids = []
    if present[0] == csv_path:
        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or id_column not in reader.fieldnames:
                raise ValueError(f"{csv_path}: missing {id_column}")
            for row in reader:
                ids.append(row[id_column])
    else:
        try:
            import pyarrow.parquet as parquet
        except ImportError as error:
            raise ValueError("Parquet verification requires pyarrow") from error
        file = parquet.ParquetFile(parquet_path)
        if id_column not in file.schema_arrow.names:
            raise ValueError(f"{parquet_path}: missing {id_column}")
        for batch in file.iter_batches(columns=[id_column]):
            ids.extend(str(value) if value is not None else None
                       for value in batch.column(0).to_pylist())
    if any(value is None or value == "" for value in ids):
        raise ValueError(f"{present[0]}: null or empty {id_column}")
    return ids


def checksum(ids):
    """SHA-256 of sorted, length-framed UTF-8 IDs; duplicates remain visible."""
    digest = hashlib.sha256()
    for value in sorted(ids):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def verify_entity(directory, entity, catalog, batch_id, command, timeout, queries):
    id_column = ENTITY_IDS[entity]
    expected = source_ids(directory, entity, id_column)
    table = f'"{catalog}"."bronze"."{entity}_raw"'
    where = "_batch_id = '" + batch_id.replace("'", "''") + "'"
    count_sql, ids_sql = (statement.strip().format(table=table,
                                                     id=f'"{id_column}"', where=where)
                          for statement in queries)
    count_rows = query(command, count_sql, timeout)
    if len(count_rows) != 1 or len(count_rows[0]) != 1 or not re.fullmatch(r"[0-9]+", count_rows[0][0]):
        raise ValueError("Trino count query did not return one nonnegative integer")
    actual_count = int(count_rows[0][0])
    if actual_count != len(expected):
        raise ValueError(f"row count: source={len(expected)}, bronze={actual_count}")
    bronze_rows = query(command, ids_sql, timeout)
    if len(bronze_rows) != actual_count or any(len(row) != 1 or not row[0] for row in bronze_rows):
        raise ValueError("Trino ID result has missing, null, or malformed rows")
    source_digest = checksum(expected)
    bronze_digest = checksum(row[0] for row in bronze_rows)
    if source_digest != bronze_digest:
        raise ValueError(f"ID checksum mismatch: source={source_digest}, bronze={bronze_digest}")
    print(f"PASS {entity}: rows={actual_count}, id_sha256={bronze_digest}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--batch-id", required=True, help="Bronze _batch_id written by ingestion")
    parser.add_argument("--entities", nargs="+", choices=ENTITY_IDS, default=list(ENTITY_IDS))
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--cli", default="trino")
    parser.add_argument("--compose", action="store_true")
    parser.add_argument("--server")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog):
        parser.error("--catalog must be a simple SQL identifier")
    if not args.batch_id:
        parser.error("--batch-id cannot be empty")
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    statements = [statement.strip() for statement in SQL_PATH.read_text(encoding="utf-8").split(";")
                  if statement.strip()]
    if len(statements) != 2:
        raise ValueError("verification SQL must contain exactly two queries")
    command = cli_command(args.cli, args.compose, args.server)
    failed = False
    for entity in dict.fromkeys(args.entities):
        try:
            verify_entity(args.input_dir, entity, args.catalog, args.batch_id,
                          command, args.timeout, statements)
        except (OSError, ValueError, csv.Error) as error:
            print(f"FAIL {entity}: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
