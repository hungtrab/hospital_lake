"""Live S3 round-trip test. Requires MinIO; recreates only its container."""

import os
from pathlib import Path
import shlex
import subprocess
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[2]


class MinioIntegrationTests(unittest.TestCase):
    def setUp(self):
        command = os.environ.get("COMPOSE")
        if command:
            self.compose = shlex.split(command)
        else:
            probe = subprocess.run(["docker", "compose", "version"], capture_output=True)
            self.compose = ["docker", "compose"] if probe.returncode == 0 else ["docker-compose"]

    def run_compose(self, *args, timeout=45):
        return subprocess.run(
            [*self.compose, *args], cwd=ROOT, check=True,
            capture_output=True, text=True, timeout=timeout,
        ).stdout

    def s3(self, method, path, *args):
        # Credentials stay in the container environment and are never printed.
        return self.run_compose(
            "exec", "-T", "minio", "sh", "-c",
            'exec curl --fail --silent --show-error --max-time 15 '
            '--aws-sigv4 aws:amz:us-east-1:s3 '
            '--user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" "$@"',
            "s3-test", "-X", method, f"http://localhost:9000/{path}", *args,
        )

    def test_s3_object_survives_container_recreation(self):
        bucket = "smoke-" + uuid.uuid4().hex
        key = f"{bucket}/probe.txt"
        payload = "hospital-lake-persistence-check"
        self.s3("PUT", bucket)
        self.addCleanup(self.s3, "DELETE", bucket)
        self.addCleanup(self.s3, "DELETE", key)
        self.s3("PUT", key, "--data-binary", payload)
        self.assertEqual(self.s3("GET", key), payload)
        self.run_compose(
            "up", "-d", "--no-deps", "--no-build", "--pull", "never",
            "--force-recreate", "--wait", "--wait-timeout", "90", "minio",
            timeout=120,
        )
        self.assertEqual(self.s3("GET", key), payload)


if __name__ == "__main__":
    unittest.main()
