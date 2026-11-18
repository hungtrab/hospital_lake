"""Apply the TV4 Kubernetes serving and observability resources."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Mapping

import yaml


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "hospital-lake"
CREDENTIAL_REVISION_ANNOTATION = "hospital-lake/credentials-resource-version"


def _configmap(name: str, files: Mapping[str, Path]) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "data": {key: path.read_text(encoding="utf-8") for key, path in files.items()},
    }


def build_configmaps(root: Path = ROOT) -> list[dict]:
    grafana = root / "dashboard" / "grafana"
    dashboards = {
        path.name: path
        for path in sorted((grafana / "dashboards").glob("*.json"))
    }
    if not dashboards:
        raise ValueError("No generated Grafana dashboard JSON files were found")

    return [
        _configmap(
            "hospital-grafana-dashboard-provider",
            {"hospital.yaml": grafana / "provisioning/dashboards/hospital.yaml"},
        ),
        _configmap(
            "hospital-grafana-datasources",
            {
                "hospital.yaml": grafana / "provisioning/datasources/hospital.yaml",
                "prometheus.yaml": root
                / "infra/k8s/observability/grafana-prometheus-datasource.yaml",
            },
        ),
        _configmap("hospital-grafana-dashboards", dashboards),
    ]


def build_credentials_secret(environment: Mapping[str, str]) -> dict:
    keys = ("CLICKHOUSE_GRAFANA_PASSWORD", "GRAFANA_ADMIN_PASSWORD")
    missing = [key for key in keys if not environment.get(key)]
    if missing:
        raise ValueError(f"Required local environment variables are empty: {', '.join(missing)}")
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "hospital-serving-credentials",
            "namespace": NAMESPACE,
        },
        "type": "Opaque",
        "stringData": {key: environment[key] for key in keys},
    }


def _apply_manifest(document: dict) -> None:
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        cwd=ROOT,
        input=yaml.safe_dump(document, sort_keys=False),
        text=True,
        check=True,
    )


def stamp_credentials_revision(document: dict, resource_version: str) -> None:
    name = document.get("metadata", {}).get("name")
    consumes_credentials = (document.get("kind"), name) in {
        ("StatefulSet", "clickhouse"),
        ("Deployment", "grafana"),
    }
    if not consumes_credentials:
        return

    pod_metadata = document["spec"]["template"].setdefault("metadata", {})
    annotations = pod_metadata.setdefault("annotations", {})
    annotations[CREDENTIAL_REVISION_ANNOTATION] = resource_version


def _apply_resource_file(path: Path, credential_resource_version: str) -> None:
    for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if not document:
            continue
        stamp_credentials_revision(document, credential_resource_version)
        _apply_manifest(document)


def _run_kubectl(*arguments: str) -> None:
    subprocess.run(["kubectl", *arguments], cwd=ROOT, check=True)


def _credential_resource_version() -> str:
    result = subprocess.run(
        [
            "kubectl",
            "get",
            "secret",
            "hospital-serving-credentials",
            "--namespace",
            NAMESPACE,
            "-o",
            "json",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["metadata"]["resourceVersion"]


def deploy(environment: Mapping[str, str] = os.environ) -> None:
    # kubectl receives credential YAML over stdin; secrets never appear in argv.
    build_credentials_secret(environment)  # Validate before creating any resources.
    _run_kubectl("apply", "-f", "infra/k8s/namespace.yaml")
    _apply_manifest(build_credentials_secret(environment))
    # A Secret resourceVersion change rolls both consumers without exposing values.
    credential_version = _credential_resource_version()
    for configmap in build_configmaps():
        _apply_manifest(configmap)

    manifest_dir = ROOT / "infra" / "k8s" / "observability"
    for filename in ("clickhouse.yaml", "prometheus.yaml", "grafana.yaml"):
        _apply_resource_file(manifest_dir / filename, credential_version)

    for workload, timeout in (
        ("statefulset/clickhouse", "300s"),
        ("deployment/prometheus", "180s"),
        ("deployment/grafana", "300s"),
    ):
        _run_kubectl(
            "rollout",
            "status",
            workload,
            "--namespace",
            NAMESPACE,
            f"--timeout={timeout}",
        )


def main() -> int:
    try:
        deploy()
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
