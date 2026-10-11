"""Gold submission must use shared service endpoints without putting secrets in arguments."""

import unittest

from scripts.run_gold import command
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


if __name__ == "__main__":
    unittest.main()
