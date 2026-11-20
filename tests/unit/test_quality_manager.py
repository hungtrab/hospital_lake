"""HTTP contracts, review persistence and job lifecycle for the local manager."""
import http.client
import json
from pathlib import Path
import threading

import pytest

from dashboard.quality_manager.server import Store, create_server, save_json


def report():
    return {"passed": True, "scenario_count": 1, "spark_version": "test",
            "entities": [{"entity": "patients", "bronze_rows": 1, "output_rows": 0,
                          "quarantine_rows": 1, "exact_duplicate_rows": 0}], "gold": {},
            "scenarios": [{"id": "P_BAD", "entity": "patients", "description": "Synthetic test",
                           "before": [{"patient_id": None}], "after": [], "quarantine": []}]}


@pytest.fixture
def service(tmp_path):
    save_json(tmp_path / "run-complete" / "report.json", report())
    (tmp_path / "run-partial").mkdir()
    save_json(tmp_path / "run-corrupt" / "report.json", {"unexpected": True})
    server = create_server(tmp_path, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.store.close()
    server.server_close()
    thread.join(timeout=5)


def request(server, path, method="GET", data=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    connection.request(method, path, body=json.dumps(data) if data is not None else None, headers=headers or {})
    response = connection.getresponse()
    status, response_headers, body = response.status, dict(response.getheaders()), response.read()
    connection.close()
    return status, response_headers, body


def mutation_headers(server):
    return {"Origin": f"http://127.0.0.1:{server.server_port}", "X-CSRF-Token": server.store.token,
            "Content-Type": "application/json"}


def test_lists_partial_and_corrupt_runs_without_calling_them_passed(service):
    status, _, body = request(service, "/api/state")
    assert status == 200
    runs = {r["id"]: r for r in json.loads(body)["runs"]}
    assert runs["run-partial"]["status"] == "incomplete"
    assert runs["run-corrupt"]["status"] == "invalid"
    assert runs["run-complete"]["totals"]["quarantine_rows"] == 1
    assert request(service, "/api/runs/run-partial")[0] == 404
    assert request(service, "/api/runs/run-corrupt")[0] == 400


def test_review_survives_restart_and_does_not_modify_data(service):
    path = service.store.results / "run-complete" / "report.json"
    before = path.read_bytes()
    value = {"status": "investigate", "note": "Cần đối chiếu <script>alert(1)</script>"}
    status, _, body = request(service, "/api/runs/run-complete/reviews/P_BAD", "POST", value, mutation_headers(service))
    assert status == 200
    assert json.loads(body)["note"] == value["note"]
    restored = Store(service.store.results)
    assert restored.report("run-complete")["reviews"]["P_BAD"]["status"] == "investigate"
    assert path.read_bytes() == before


@pytest.mark.parametrize("value", [{"status": "approved"}, {"status": "reviewed", "note": "x" * 4001}, {"status": "pending", "note": 5}])
def test_invalid_review_rejected(service, value):
    assert request(service, "/api/runs/run-complete/reviews/P_BAD", "POST", value, mutation_headers(service))[0] == 400


def test_unknown_case_and_unsafe_http_requests_rejected(service):
    data = {"status": "reviewed", "note": ""}
    assert request(service, "/api/runs/run-complete/reviews/UNKNOWN", "POST", data, mutation_headers(service))[0] == 400
    assert request(service, "/api/jobs", "POST", {})[0] == 403
    headers = mutation_headers(service)
    headers["Origin"] = "https://another-site.example"
    assert request(service, "/api/jobs", "POST", {}, headers)[0] == 403
    assert request(service, "/api/state", headers={"Host": "another-site.example"})[0] == 403
    for path in ("/.env", "/../../AGENTS.md", "/api/runs/..%2F..%2F.env", "/static/../server.py"):
        assert request(service, path)[0] == 404


def test_download_is_original_report_and_static_assets_are_served(service):
    status, headers, body = request(service, "/api/runs/run-complete/download")
    assert status == 200
    assert json.loads(body) == report()
    assert "attachment" in headers["Content-Disposition"]
    for path, mime in (("/", "text/html"), ("/app.js", "text/javascript"), ("/style.css", "text/css")):
        status, headers, body = request(service, path)
        assert status == 200 and headers["Content-Type"].startswith(mime) and body


def test_symlink_reports_are_not_served(service, tmp_path):
    outside = tmp_path.parent / "outside-quality-report.json"
    save_json(outside, report())
    folder = tmp_path / "run-symlink"
    folder.mkdir()
    (folder / "report.json").symlink_to(outside)
    assert request(service, "/api/runs/run-symlink")[0] == 400


@pytest.mark.parametrize("exit_code, has_report, expected", [(0, True, "passed"), (1, True, "failed"), (0, False, "failed")])
def test_job_requires_exit_success_and_complete_report_and_prevents_overlap(tmp_path, monkeypatch, exit_code, has_report, expected):
    release = threading.Event()
    calls = []

    class Process:
        def __init__(self, argv, **kwargs):
            calls.append((argv, kwargs))
            self.output = Path(argv[-1])

        def wait(self):
            assert release.wait(timeout=5)
            if has_report:
                save_json(self.output / "run-test" / "report.json", report())
            return exit_code

    monkeypatch.setattr("dashboard.quality_manager.server.subprocess.Popen", Process)
    store = Store(tmp_path)
    job = store.start_job()
    try:
        with pytest.raises(RuntimeError):
            store.start_job()
    finally:
        release.set()
        store.worker.join(timeout=5)
    assert store.job_state()["status"] == expected
    assert calls[0][0][1:3] == ["-m", "scripts.data_quality_demo"]
    assert not calls[0][1].get("shell", False)
    assert Store(tmp_path).job_state()["id"] == job["id"]
    assert Store(tmp_path).job_state()["status"] == expected


def test_interrupted_job_remains_visible(tmp_path):
    save_json(tmp_path / "_manager/jobs/job1/job.json", {"id": "job1", "status": "running", "started_at": "2026-01-01T00:00:00Z"})
    assert Store(tmp_path).job_state()["status"] == "interrupted"
