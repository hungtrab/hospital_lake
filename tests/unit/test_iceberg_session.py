"""Keep shared session callers and existing Spark 3.5 submit packages compatible."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from spark.common.config import SparkConfig
from spark.common.iceberg import ICEBERG_PACKAGES, create_spark_session


class IcebergSessionTests(unittest.TestCase):
    def build(self, version, supplied_packages="", extra_packages=()):
        config = SparkConfig.from_env({"MINIO_ACCESS_KEY": "fixture-access",
                                       "MINIO_SECRET_KEY": "fixture-secret"})
        builder = MagicMock()
        builder.master.return_value = builder
        builder.appName.return_value = builder
        builder.config.return_value = builder
        conf = MagicMock()
        conf.get.return_value = supplied_packages
        modules = {
            "pyspark": SimpleNamespace(__version__=version, SparkConf=lambda: conf),
            "pyspark.sql": SimpleNamespace(SparkSession=SimpleNamespace(builder=builder)),
        }
        with patch.dict(sys.modules, modules):
            create_spark_session("existing-smoke-caller", config,
                                 extra_packages=extra_packages)
        builder.appName.assert_called_once_with("existing-smoke-caller")
        return {call.args[0]: call.args[1] for call in builder.config.call_args_list}

    def test_host_spark4_gets_matching_packages_and_shared_endpoints(self):
        settings = self.build("4.0.2")
        self.assertEqual(settings["spark.jars.packages"], ICEBERG_PACKAGES)
        self.assertEqual(settings["spark.sql.catalog.iceberg.uri"], "http://iceberg-rest:8181")
        self.assertEqual(settings["spark.sql.session.timeZone"], "UTC")

    def test_existing_spark35_compose_packages_are_preserved(self):
        supplied = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0"
        self.assertEqual(self.build("3.5.5", supplied)["spark.jars.packages"], supplied)

    def test_spark35_host_uses_spark35_runtime(self):
        self.assertIn("iceberg-spark-runtime-3.5_2.12:1.9.0", self.build("3.5.5")["spark.jars.packages"])

    def test_extra_packages_append_without_replacing_existing_packages(self):
        iceberg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        aws = "org.apache.iceberg:iceberg-aws-bundle:1.9.0"
        kafka = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
        supplied = f"{iceberg},{aws}"
        packages = self.build("3.5.5", supplied, (kafka, aws))["spark.jars.packages"]
        self.assertEqual(packages, f"{supplied},{kafka}")


if __name__ == "__main__":
    unittest.main()
