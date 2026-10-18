"""Bucket bootstrap must preserve existing buckets and fail on access errors."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


BOOTSTRAP = Path(__file__).resolve().parents[2] / "infra/docker/minio/bootstrap.sh"


class BootstrapTests(unittest.TestCase):
    def run_bootstrap(self, head="200", put="200", warehouse="s3://warehouse/"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            curl = root / "curl"
            curl.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$CALL_LOG"
case "$*" in
  *--head*) status=$HEAD_STATUS ;;
  *) status=$PUT_STATUS ;;
esac
case "$*" in
  *--fail*) test "$status" = 200 || exit 22 ;;
  *) printf '%s' "$status" ;;
esac
''')
            curl.chmod(0o755)
            log = root / "calls"
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       MINIO_ENDPOINT="http://minio:9000", MINIO_ACCESS_KEY="test-key",
                       MINIO_SECRET_KEY="test-secret", ICEBERG_WAREHOUSE=warehouse,
                       CALL_LOG=str(log), HEAD_STATUS=head, PUT_STATUS=put)
            result = subprocess.run(["sh", str(BOOTSTRAP)], env=env, capture_output=True, text=True)
            return result, log.read_text() if log.exists() else ""

    def test_existing_bucket_is_not_modified(self):
        result, calls = self.run_bootstrap()
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("--request PUT", calls)
        self.assertNotIn("test-secret", result.stdout + result.stderr)

    def test_missing_bucket_is_created(self):
        result, calls = self.run_bootstrap(head="404")
        self.assertEqual(result.returncode, 0)
        self.assertIn("--request PUT", calls)
        self.assertIn("http://minio:9000/warehouse", calls)

    def test_access_and_creation_errors_fail(self):
        result, calls = self.run_bootstrap(head="403")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("--request PUT", calls)
        for put in ("403", "500", "409"):
            with self.subTest(status=put):
                result, _ = self.run_bootstrap(head="404", put=put)
                self.assertNotEqual(result.returncode, 0)

    def test_invalid_warehouse_fails_before_network(self):
        for value in ("file:///tmp/data", "s3:///", "s3://bad$name/"):
            with self.subTest(value=value):
                result, calls = self.run_bootstrap(warehouse=value)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, "")
