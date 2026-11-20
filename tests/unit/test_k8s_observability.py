from pathlib import Path
from unittest.mock import patch

import yaml
import pytest

from scripts.deploy_observability_k8s import (
    build_configmaps,
    build_credentials_secret,
    deploy,
    stamp_credentials_revision,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
K8S_DIR = REPO_ROOT / "infra" / "k8s" / "observability"


def _documents(path: Path) -> list[dict]:
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def test_clickhouse_has_persistent_stateful_storage_and_health_probes():
    documents = _documents(K8S_DIR / "clickhouse.yaml")
    statefulset = next(doc for doc in documents if doc["kind"] == "StatefulSet")
    service = next(
        doc
        for doc in documents
        if doc["kind"] == "Service" and doc["metadata"]["name"] == "clickhouse"
    )
    headless_service = next(
        doc
        for doc in documents
        if doc["kind"] == "Service"
        and doc["metadata"]["name"] == "clickhouse-headless"
    )
    config = next(doc for doc in documents if doc["kind"] == "ConfigMap")

    assert statefulset["spec"]["serviceName"] == headless_service["metadata"]["name"]
    assert headless_service["spec"]["clusterIP"] == "None"
    assert statefulset["spec"]["selector"]["matchLabels"] == service["spec"]["selector"]
    template = statefulset["spec"]["template"]
    container = template["spec"]["containers"][0]
    assert container["readinessProbe"]
    assert container["livenessProbe"]
    assert statefulset["spec"]["volumeClaimTemplates"]
    assert {port["name"] for port in service["spec"]["ports"]} == {
        "http",
        "native",
        "metrics",
    }
    assert 'from_env="CLICKHOUSE_GRAFANA_PASSWORD"' in config["data"]["grafana-reader.xml"]
    assert "<readonly>1</readonly>" in config["data"]["grafana-reader.xml"]
    assert "<max_execution_time><changeable_in_readonly/></max_execution_time>" in config["data"]["grafana-reader.xml"]
    assert "<port>9363</port>" in config["data"]["metrics.xml"]


def test_grafana_mounts_provisioned_dashboards_and_persistent_data():
    documents = _documents(K8S_DIR / "grafana.yaml")
    deployment = next(doc for doc in documents if doc["kind"] == "Deployment")
    service = next(doc for doc in documents if doc["kind"] == "Service")
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    mounts = {mount["mountPath"]: mount for mount in container["volumeMounts"]}

    assert deployment["spec"]["selector"]["matchLabels"] == service["spec"]["selector"]
    assert "/var/lib/grafana" in mounts
    assert "/etc/grafana/provisioning/dashboards" in mounts
    assert "/etc/grafana/provisioning/datasources" in mounts
    assert "/var/lib/grafana/hospital-dashboards" in mounts
    assert any(volume.get("persistentVolumeClaim") for volume in pod["volumes"])
    assert container["readinessProbe"]
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    secret_refs = {
        env["valueFrom"]["secretKeyRef"]["key"]
        for env in container["env"]
        if "valueFrom" in env
    }
    assert secret_refs == {"GRAFANA_ADMIN_PASSWORD", "CLICKHOUSE_GRAFANA_PASSWORD"}


def test_prometheus_scrapes_clickhouse_and_persists_tsdb():
    documents = _documents(K8S_DIR / "prometheus.yaml")
    config = next(doc for doc in documents if doc["kind"] == "ConfigMap")
    deployment = next(doc for doc in documents if doc["kind"] == "Deployment")
    scrape_config = yaml.safe_load(config["data"]["prometheus.yml"])
    targets = {
        target
        for job in scrape_config["scrape_configs"]
        for static in job.get("static_configs", [])
        for target in static["targets"]
    }

    assert "clickhouse:9363" in targets
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["readinessProbe"]
    assert any(volume.get("persistentVolumeClaim") for volume in pod["volumes"])
    assert deployment["spec"]["strategy"]["type"] == "Recreate"


def test_grafana_configmaps_are_built_from_repository_provisioning():
    grafana = next(
        doc for doc in _documents(K8S_DIR / "grafana.yaml") if doc["kind"] == "Deployment"
    )
    volumes = {
        volume["name"]: volume["configMap"]["name"]
        for volume in grafana["spec"]["template"]["spec"]["volumes"]
        if "configMap" in volume
    }

    assert volumes == {
        "dashboard-provider": "hospital-grafana-dashboard-provider",
        "datasources": "hospital-grafana-datasources",
        "dashboards": "hospital-grafana-dashboards",
    }
    assert list((REPO_ROOT / "dashboard/grafana/dashboards").glob("*.json"))
    assert (REPO_ROOT / "dashboard/grafana/provisioning/dashboards/hospital.yaml").is_file()
    assert (REPO_ROOT / "dashboard/grafana/provisioning/datasources/hospital.yaml").is_file()


def test_generated_grafana_configmaps_use_source_dashboards_and_prometheus_datasource():
    configmaps = {item["metadata"]["name"]: item for item in build_configmaps(REPO_ROOT)}

    assert set(configmaps) == {
        "hospital-grafana-dashboard-provider",
        "hospital-grafana-datasources",
        "hospital-grafana-dashboards",
    }
    dashboards = configmaps["hospital-grafana-dashboards"]["data"]
    assert set(dashboards) == {path.name for path in (REPO_ROOT / "dashboard/grafana/dashboards").glob("*.json")}
    assert {
        "hospital-overview.json",
        "realtime-operations.json",
        "historical-analytics.json",
    }.issubset(dashboards)
    assert sum(len(content.encode("utf-8")) for content in dashboards.values()) < 1024 * 1024
    datasource_data = configmaps["hospital-grafana-datasources"]["data"]
    assert "Hospital Trino" in datasource_data["hospital.yaml"]
    assert "http://prometheus:9090" in datasource_data["prometheus.yaml"]


def test_credentials_secret_is_created_from_runtime_environment_values():
    secret = build_credentials_secret(
        {
            "CLICKHOUSE_GRAFANA_PASSWORD": 'reader "$punctuation&ok',
            "GRAFANA_ADMIN_PASSWORD": "demo-secret",
        }
    )

    assert secret["kind"] == "Secret"
    assert secret["metadata"]["namespace"] == "hospital-lake"
    assert secret["stringData"] == {
        "CLICKHOUSE_GRAFANA_PASSWORD": 'reader "$punctuation&ok',
        "GRAFANA_ADMIN_PASSWORD": "demo-secret",
    }
    assert yaml.safe_load(yaml.safe_dump(secret))["stringData"] == secret["stringData"]


def test_deploy_validates_credentials_before_applying_resources():
    with patch("scripts.deploy_observability_k8s._run_kubectl") as run_kubectl:
        with pytest.raises(ValueError, match="CLICKHOUSE_GRAFANA_PASSWORD"):
            deploy({})
    run_kubectl.assert_not_called()


def test_k8s_readme_documents_grafana_admin_password_rotation():
    readme = " ".join(
        (K8S_DIR / "README.md").read_text(encoding="utf-8").split()
    ).lower()

    assert "initial grafana admin password" in readme
    assert "rotate it in grafana" in readme
    assert "update the local environment variable" in readme


def test_credential_revision_restarts_only_secret_consuming_workloads():
    clickhouse = {
        "kind": "StatefulSet",
        "metadata": {"name": "clickhouse"},
        "spec": {"template": {"metadata": {"labels": {"app": "clickhouse"}}}},
    }
    grafana = {
        "kind": "Deployment",
        "metadata": {"name": "grafana"},
        "spec": {"template": {"metadata": {"labels": {"app": "grafana"}}}},
    }
    prometheus = {
        "kind": "Deployment",
        "metadata": {"name": "prometheus"},
        "spec": {"template": {"metadata": {"labels": {"app": "prometheus"}}}},
    }

    stamp_credentials_revision(clickhouse, "42")
    stamp_credentials_revision(grafana, "42")
    stamp_credentials_revision(prometheus, "42")

    annotation = "hospital-lake/credentials-resource-version"
    assert clickhouse["spec"]["template"]["metadata"]["annotations"][annotation] == "42"
    assert grafana["spec"]["template"]["metadata"]["annotations"][annotation] == "42"
    assert "annotations" not in prometheus["spec"]["template"]["metadata"]

def test_observability_resources_are_namespaced_and_have_services():
    for path in K8S_DIR.glob("*.yaml"):
        for document in _documents(path):
            if document.get("kind") and document["kind"] not in {
                "Namespace",
                "PersistentVolumeClaim",
            }:
                assert document["metadata"].get("namespace") == "hospital-lake"
