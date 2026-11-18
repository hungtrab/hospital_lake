"""Apply the TV4 Kubernetes serving and observability resources."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Mapping

import yaml


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "hospital-lake"


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


def _run_kubectl(*arguments: str) -> None:
    subprocess.run(["kubectl", *arguments], cwd=ROOT, check=True)


def deploy(environment: Mapping[str, str] = os.environ) -> None:
    # kubectl receives credential YAML over stdin; secrets never appear in argv.
    build_credentials_secret(environment)  # Validate before creating any resources.
    _run_kubectl("apply", "-f", "infra/k8s/namespace.yaml")
    _apply_manifest(build_credentials_secret(environment))
    for configmap in build_configmaps():
        _apply_manifest(configmap)

    manifest_dir = ROOT / "infra" / "k8s" / "observability"
    for filename in ("clickhouse.yaml", "prometheus.yaml", "grafana.yaml"):
        _run_kubectl("apply", "-f", str(manifest_dir / filename))

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
