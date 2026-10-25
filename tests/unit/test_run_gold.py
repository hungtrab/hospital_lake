"""Gold submission must use shared service endpoints without putting secrets in arguments."""

import unittest

import os
from pathlib import Path

from scripts.run_gold import command, submit_env
from spark.common.config import SparkConfig


class GoldSubmitTests(unittest.TestCase):
    def test_command_uses_shared_catalog_and_storage(self):
        config = SparkConfig.from_env({
            "MINIO_ACCESS_KEY": "local-user",
            "MINIO_SECRET_KEY": "local-password",
            "MINIO_ENDPOINT": "http://127.0.0.1:9000",
            "ICEBERG_CATALOG_URI": "http://127.0.0.1:8181",
            "ICEBERG_WAREHOUSE": "s3://warehouse/",
        })
        args = command(config)
        rendered = " ".join(args)
        self.assertIn("spark.sql.catalog.iceberg.uri=http://127.0.0.1:8181", rendered)
        self.assertIn("spark.sql.catalog.iceberg.s3.endpoint=http://127.0.0.1:9000", rendered)
        self.assertIn("silver_to_gold.py", rendered)
        self.assertNotIn("local-password", rendered)
        self.assertNotIn("local-user", rendered)

    def test_submit_env_puts_repo_root_on_pythonpath(self):
        config = SparkConfig.from_env({"MINIO_ACCESS_KEY": "u", "MINIO_SECRET_KEY": "p"})
        root = str(Path(__file__).resolve().parents[2])
        env = submit_env(config, {"PYTHONPATH": "/extra"})
        self.assertEqual(env["PYTHONPATH"].split(os.pathsep), [root, "/extra"])
        self.assertEqual((env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]), ("u", "p"))
        self.assertEqual(submit_env(config, {})["PYTHONPATH"], root)


if __name__ == "__main__":
    unittest.main()
