import os
import unittest
from unittest.mock import patch

from spark.common.config import SparkConfig


class SparkConfigTests(unittest.TestCase):
    def setUp(self):
        self.env = {
            "MINIO_ACCESS_KEY": "fixture-access",
            "MINIO_SECRET_KEY": "fixture-secret",
        }

    def test_defaults_match_shared_local_contract(self):
        config = SparkConfig.from_env(self.env)
        self.assertEqual(config.master, "local[2]")
        self.assertEqual(config.catalog_uri, "http://iceberg-rest:8181")
        self.assertEqual(config.warehouse, "s3://warehouse/")
        self.assertEqual(config.minio_endpoint, "http://minio:9000")
        self.assertEqual(config.minio_region, "us-east-1")

    def test_custom_settings_and_credentials_are_preserved(self):
        self.env.update({
            "SPARK_MASTER": " spark://spark:7077 ",
            "ICEBERG_CATALOG_URI": "https://catalog.example.test/api",
            "ICEBERG_WAREHOUSE": "s3://demo/lakehouse/",
            "MINIO_ENDPOINT": "https://storage.example.test",
            "MINIO_REGION": "ap-southeast-1",
            "MINIO_SECRET_KEY": " secret with spaces ",
        })
        original = self.env.copy()
        config = SparkConfig.from_env(self.env)
        self.assertEqual(config.master, "spark://spark:7077")
        self.assertEqual(config.catalog_uri, self.env["ICEBERG_CATALOG_URI"])
        self.assertEqual(config.warehouse, self.env["ICEBERG_WAREHOUSE"])
        self.assertEqual(config.minio_endpoint, self.env["MINIO_ENDPOINT"])
        self.assertEqual(config.minio_region, self.env["MINIO_REGION"])
        self.assertEqual(config.minio_access_key, "fixture-access")
        self.assertEqual(config.minio_secret_key, " secret with spaces ")
        self.assertEqual(self.env, original)

    def test_reads_exported_environment(self):
        with patch.dict(os.environ, self.env, clear=True):
            self.assertEqual(SparkConfig.from_env(), SparkConfig.from_env(self.env))

    def test_missing_or_blank_credentials_fail_with_variable_name(self):
        for name in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
            for value in (None, "", "   "):
                with self.subTest(name=name, value=value):
                    env = self.env.copy()
                    if value is None:
                        del env[name]
                    else:
                        env[name] = value
                    with self.assertRaisesRegex(ValueError, name):
                        SparkConfig.from_env(env)

    def test_explicit_blank_settings_do_not_fall_back_to_defaults(self):
        for name in ("SPARK_MASTER", "ICEBERG_CATALOG_URI", "ICEBERG_WAREHOUSE",
                     "MINIO_ENDPOINT", "MINIO_REGION"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, name):
                SparkConfig.from_env({**self.env, name: "   "})

    def test_invalid_endpoints_and_warehouse_fail(self):
        invalid = {
            "MINIO_ENDPOINT": ("minio:9000", "http://", "s3://warehouse/"),
            "ICEBERG_CATALOG_URI": ("iceberg-rest:8181", "https://", "file:///tmp"),
            "ICEBERG_WAREHOUSE": ("/tmp/warehouse", "s3:///", "https://bucket"),
        }
        for name, values in invalid.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    with self.assertRaisesRegex(ValueError, name):
                        SparkConfig.from_env({**self.env, name: value})

    def test_credentials_are_hidden_in_repr_and_validation_errors(self):
        config = SparkConfig.from_env(self.env)
        for secret in self.env.values():
            self.assertNotIn(secret, repr(config))
        with self.assertRaises(ValueError) as error:
            SparkConfig.from_env({**self.env, "ICEBERG_WAREHOUSE": "invalid"})
        for secret in self.env.values():
            self.assertNotIn(secret, str(error.exception))


if __name__ == "__main__":
    unittest.main()
