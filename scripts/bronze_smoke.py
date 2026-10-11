"""Live TV2 table-contract checks; uses only isolated temporary data fixtures."""

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import quote, urlsplit
import uuid

from scripts.lakehouse_smoke import ROOT, compose_command, run
from scripts.trino_smoke import query
from scripts.verify_bronze import ENTITY_IDS, checksum, source_ids
from spark.common.bronze import load_contracts


def metadata_in_minio(compose, namespace, table):
    response = json.loads(run([*compose, "exec", "-T", "iceberg-rest", "curl", "--fail", "--silent",
                               f"http://localhost:8181/v1/namespaces/{namespace}/tables/{table}"], capture=True))
    location = urlsplit(response["metadata-location"])
    if location.scheme != "s3" or not location.netloc or not location.path.endswith(".metadata.json"):
        raise ValueError(f"{namespace}.{table}: invalid S3 metadata location")
    url = "http://localhost:9000/" + quote(location.netloc + location.path, safe="/")
    stored = json.loads(run([*compose, "exec", "-T", "minio", "sh", "-c",
                             'exec curl --fail --silent --show-error --max-time 20 '
                             '--aws-sigv4 "aws:amz:${MINIO_REGION_NAME:-us-east-1}:s3" '
                             '--user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" "$1"',
                             "metadata-check", url], capture=True))
    if stored["table-uuid"] != response["metadata"]["table-uuid"] or stored["format-version"] != 2:
        raise ValueError(f"{namespace}.{table}: metadata identity/version mismatch")
    print(f"PASS: {namespace}.{table} metadata exists in MinIO", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, help="Existing six-entity Parquet fixture inside repository")
    args = parser.parse_args(argv)
    compose = compose_command()
    contracts = load_contracts()
    namespace = "bronze_smoke_" + uuid.uuid4().hex
    temporary = None
    cleanup = False
    stage = "canonical tables"
    try:
        trino = [*compose, "exec", "-T", "trino", "trino", "--server", "http://localhost:8080", "--output-format", "CSV"]
        visible = query(trino, "SHOW TABLES FROM iceberg.bronze", 60)
        for contract in contracts:
            if [contract.name] not in visible:
                raise ValueError(f"Missing canonical table {contract.name}; run make bootstrap-bronze")
            metadata_in_minio(compose, "bronze", contract.name)
            query(trino, f"SELECT * FROM iceberg.bronze.{contract.name} LIMIT 1", 60)
        stage = "source fixtures"
        if args.input_dir:
            input_dir = args.input_dir.resolve()
        else:
            generated = ROOT / "data/generated"
            generated.mkdir(parents=True, exist_ok=True)
            temporary = tempfile.TemporaryDirectory(prefix="bronze-contract-", dir=generated)
            input_dir = Path(temporary.name)
            run([sys.executable, "-m", "generators.batch_generator.generate", "--patients", "32",
                 "--seed", "42", "--format", "parquet", "--output-dir", str(input_dir)])
        relative = input_dir.relative_to(ROOT)
        expected_ids = {}
        for entity, key in ENTITY_IDS.items():
            if not (input_dir / f"{entity}.parquet").is_file():
                raise ValueError(f"Missing Parquet fixture for {entity}")
            ids = source_ids(input_dir, entity, key)
            if not ids:
                raise ValueError(f"Fixture {entity} must be non-empty")
            expected_ids[entity] = [*ids, min(ids)] + ([min(ids)] if entity == "vitals" else [])
        stage = "Parquet writes and bootstrap rerun"
        cleanup = True
        run([*compose, "run", "--rm", "--no-deps", "bronze-smoke", "--namespace", namespace,
             "--input-dir", str(Path("/opt/hospital-lake") / relative)], timeout=900)
        stage = "Trino read and S3 metadata"
        for contract in contracts:
            entity = contract.name[:-4]
            name = f"iceberg.{namespace}.{contract.name}"
            ids = query(trino, f"SELECT {ENTITY_IDS[entity]} FROM {name}", 90)
            if len(ids) != len(expected_ids[entity]) or checksum(row[0] for row in ids) != checksum(expected_ids[entity]):
                raise ValueError(f"{name}: source IDs/duplicates were changed")
            metadata_in_minio(compose, namespace, contract.name)
            if query(trino, f"SELECT count(*) FROM {name} WHERE _batch_id = '{namespace}' AND _schema_version = 1", 60) != [[str(len(ids))]]:
                raise ValueError(f"{name}: ingestion metadata mismatch")
            files = query(trino, f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM iceberg.{namespace}."{contract.name}$files"', 60)[0]
            partitions = query(trino, f'SELECT count(*) FROM iceberg.{namespace}."{contract.name}$partitions"', 60)[0][0]
            print(json.dumps({"table": contract.name, "fixture_rows": len(ids), "files": int(files[0]),
                              "average_file_bytes": round(int(files[1]) / max(int(files[0]), 1), 1),
                              "partitions": int(partitions)}), flush=True)
        if query(trino, f"SELECT count(*) FROM iceberg.{namespace}.vitals_raw WHERE heart_rate = -1", 60) != [["1"]]:
            raise ValueError("Invalid raw vital was dropped or normalized")
        stage = "schema, partition and property drift rejection"
        run([*compose, "run", "--rm", "--no-deps", "bronze-smoke", "--namespace", namespace,
             "--action", "drift"], timeout=300)
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError, csv.Error, KeyError) as error:
        print(f"FAIL {stage}: {error}", file=sys.stderr)
        return 1
    finally:
        failed_cleanup = False
        if cleanup:
            try:
                run([*compose, "run", "--rm", "--no-deps", "bronze-smoke", "--namespace", namespace,
                     "--action", "cleanup"], timeout=300)
            except (OSError, subprocess.TimeoutExpired, RuntimeError) as error:
                print(f"FAIL cleanup: {error}", file=sys.stderr)
                failed_cleanup = True
        if temporary:
            temporary.cleanup()
        if failed_cleanup:
            return 1
    print("PASS: TV2 Week 2 Bronze table contracts, reruns, MinIO and Trino", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
