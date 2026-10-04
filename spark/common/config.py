"""Environment configuration shared by future batch and streaming Spark jobs."""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True)
class SparkConfig:
    master: str
    catalog_uri: str
    warehouse: str
    minio_endpoint: str
    minio_region: str
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SparkConfig":
        """Read exported variables; fail on blank settings without exposing secrets.

        This does not load .env files or connect to external services.
        An explicit mapping lets tests run independently of the host environment.
        """
        env = os.environ if environ is None else environ

        def setting(name: str, default: str = "", *, secret: bool = False) -> str:
            value = env.get(name, default)
            if not value.strip():
                raise ValueError(f"{name} must be set and non-empty")
            return value if secret else value.strip()

        config = cls(
            master=setting("SPARK_MASTER", "local[2]"),
            catalog_uri=setting("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
            warehouse=setting("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            minio_endpoint=setting("MINIO_ENDPOINT", "http://minio:9000"),
            minio_region=setting("MINIO_REGION", "us-east-1"),
            minio_access_key=setting("MINIO_ACCESS_KEY", secret=True),
            minio_secret_key=setting("MINIO_SECRET_KEY", secret=True),
        )
        for name, value in (
            ("ICEBERG_CATALOG_URI", config.catalog_uri),
            ("MINIO_ENDPOINT", config.minio_endpoint),
        ):
            uri = urlsplit(value)
            if uri.scheme not in ("http", "https") or not uri.hostname:
                raise ValueError(f"{name} must be an HTTP(S) URL with a host")
        warehouse = urlsplit(config.warehouse)
        if warehouse.scheme != "s3" or not warehouse.netloc:
            raise ValueError("ICEBERG_WAREHOUSE must be an s3:// URL with a bucket")
        return config
