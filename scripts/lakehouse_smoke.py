"""Verify Week 1 services, rerunnable bucket bootstrap, and Spark/Trino persistence."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import uuid

from scripts.trino_smoke import query


ROOT = Path(__file__).resolve().parents[1]


def compose_command():
    if os.environ.get("COMPOSE"):
        return shlex.split(os.environ["COMPOSE"])
    probe = subprocess.run(["docker", "compose", "version"], capture_output=True)
    return ["docker", "compose"] if probe.returncode == 0 else ["docker-compose"]


def run(command, *, capture=False, timeout=180):
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=capture, timeout=timeout)
    if result.returncode:
        details = result.stderr.strip() if capture else "see service/job output above"
        raise RuntimeError(f"Command failed ({result.returncode}): {details}")
    return result.stdout


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart", action="store_true", help="recreate MinIO/catalog with fixture active")
    args = parser.parse_args(argv)
    compose = compose_command()
    stage = "services"
    table = "w1_smoke_" + uuid.uuid4().hex
    cleanup = False
    try:
        run([*compose, "exec", "-T", "minio", "curl", "--fail", "--silent",
             "http://localhost:9000/minio/health/ready"])
        config = json.loads(run([*compose, "exec", "-T", "iceberg-rest", "curl", "--fail", "--silent",
                                 "http://localhost:8181/v1/config"], capture=True))
        if not isinstance(config.get("defaults"), dict) or not isinstance(config.get("overrides"), dict):
            raise ValueError("Invalid REST config response")
        print("PASS: MinIO and REST catalog readiness", flush=True)
        stage = "bucket bootstrap"
        for _ in range(2):
            run([*compose, "run", "--rm", "--no-deps", "minio-init"])
        stage = "Spark write/read"
        cleanup = True
        run([*compose, "run", "--rm", "--no-deps", "spark-smoke", "--table", table], timeout=900)
        # Re-running bootstrap must also preserve already-written Iceberg files.
        run([*compose, "run", "--rm", "--no-deps", "minio-init"])
        trino = [*compose, "exec", "-T", "trino", "trino", "--server", "http://localhost:8080",
                 "--output-format", "CSV"]

        def verify_trino():
            if query(trino, f"SELECT id, label FROM iceberg.bronze.{table} ORDER BY id", 90) != [
                ["1", "synthetic-1"], ["2", "synthetic-2"], ["3", "synthetic-3"]
            ]:
                raise ValueError("Trino rows do not match Spark fixture")
            for namespace in ("bronze", "silver", "gold"):
                if [namespace] not in query(trino, "SHOW SCHEMAS FROM iceberg", 90):
                    raise ValueError(f"Namespace {namespace} not visible through Trino")
            print("PASS: Trino reads exact Spark rows and all three namespaces", flush=True)

        stage = "Trino read"
        verify_trino()
        if args.restart:
            stage = "storage/catalog recovery"
            run([*compose, "up", "-d", "--no-deps", "--no-build", "--pull", "never",
                 "--force-recreate", "--wait", "--wait-timeout", "120", "minio", "iceberg-rest"])
            run([*compose, "run", "--rm", "--no-deps", "spark-smoke", "--table", table,
                 "--action", "read"], timeout=300)
            verify_trino()
            print("PASS: fixture survives MinIO and catalog container recreation", flush=True)
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError) as error:
        print(f"FAIL {stage}: {error}", file=sys.stderr)
        return 1
    finally:
        if cleanup:
            try:
                run([*compose, "run", "--rm", "--no-deps", "spark-smoke", "--table", table,
                     "--action", "drop"], timeout=300)
            except (OSError, subprocess.TimeoutExpired, RuntimeError) as error:
                print(f"FAIL fixture cleanup: {error}", file=sys.stderr)
                return 1
    print("PASS: Week 1 Spark → Iceberg on MinIO → Trino gate", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
