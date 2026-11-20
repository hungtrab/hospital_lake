"""Loopback-only UI for local quality demo reports, reviews and execution.

No third-party web dependency. Hospital data remains in the existing pipeline;
review notes are separate metadata and cannot approve/rewrite Silver records.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import threading
from urllib.parse import urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[2]
STATIC = Path(__file__).parent / "static"
LOG = logging.getLogger(__name__)
RUN_ID = re.compile(r"run-[A-Za-z0-9_-]+\Z")


def now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


class Store:
    def __init__(self, results):
        self.results = Path(results).resolve()
        self.meta = self.results / "_manager"
        self.meta.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.token = secrets.token_urlsafe(32)
        self.process = None
        self.worker = None
        self.job = None
        self.closing = False
        review_path = self.meta / "reviews.json"
        self.reviews = json.loads(review_path.read_text()) if review_path.exists() else {}
        # Finished/failed jobs survive server restart. An interrupted process is
        # never reported as successful merely because it wrote partial output.
        for path in sorted((self.meta / "jobs").glob("*/job.json")):
            job = json.loads(path.read_text())
            if job["status"] == "running":
                job.update(status="interrupted", finished_at=now())
                save_json(path, job)
            if self.job is None or job["started_at"] > self.job["started_at"]:
                self.job = job

    def run_paths(self):
        paths = list(self.results.glob("run-*"))
        paths += list((self.meta / "jobs").glob("*/output/run-*"))
        return {p.name: p for p in paths if p.is_dir() and RUN_ID.fullmatch(p.name)
                and not p.is_symlink() and p.resolve().is_relative_to(self.results)}

    def report_path(self, run_id):
        if not RUN_ID.fullmatch(run_id):
            raise ValueError("Mã lần chạy không hợp lệ.")
        directory = self.run_paths().get(run_id)
        if directory is None:
            raise FileNotFoundError("Không tìm thấy lần chạy.")
        path = directory / "report.json"
        if path.is_symlink():
            raise ValueError("Không đọc liên kết ngoài thư mục kết quả.")
        return path

    def report(self, run_id):
        data = json.loads(self.report_path(run_id).read_text(encoding="utf-8"))
        if (not isinstance(data, dict) or not isinstance(data.get("scenarios"), list)
                or not isinstance(data.get("entities"), list) or not isinstance(data.get("gold"), dict)
                or not isinstance(data.get("scenario_count"), int)):
            raise ValueError("Báo cáo không đúng định dạng.")
        with self.lock:
            return {"id": run_id, "report": data, "reviews": dict(self.reviews.get(run_id, {}))}

    def runs(self):
        result = []
        for run_id, directory in self.run_paths().items():
            report_file = directory / "report.json"
            entry = {"id": run_id, "created_at": datetime.fromtimestamp(
                directory.stat().st_mtime, timezone.utc).isoformat(), "status": "incomplete"}
            if report_file.exists():
                try:
                    report = self.report(run_id)["report"]
                    entry.update(status="passed" if report.get("passed") is True else "failed",
                                 scenario_count=report["scenario_count"], spark_version=report["spark_version"],
                                 totals={key: sum(r[key] for r in report["entities"]) for key in
                                         ("bronze_rows", "output_rows", "quarantine_rows", "exact_duplicate_rows")})
                except (ValueError, KeyError, TypeError, OSError):
                    entry["status"] = "invalid"
            result.append(entry)
        return sorted(result, key=lambda row: (row["created_at"], row["id"]), reverse=True)

    def review(self, run_id, case_id, data):
        if data.get("status") not in ("pending", "reviewed", "investigate"):
            raise ValueError("Trạng thái rà soát không hợp lệ.")
        note = data.get("note", "")
        if not isinstance(note, str) or len(note) > 4000:
            raise ValueError("Ghi chú tối đa 4.000 ký tự.")
        report = self.report(run_id)["report"]
        if case_id not in {case["id"] for case in report["scenarios"]}:
            raise ValueError("Kịch bản không có trong lần chạy này.")
        value = {"status": data["status"], "note": note, "updated_at": now()}
        with self.lock:
            updated = {**self.reviews, run_id: {**self.reviews.get(run_id, {}), case_id: value}}
            save_json(self.meta / "reviews.json", updated)
            self.reviews = updated
        return value

    def job_state(self):
        with self.lock:
            if self.job is None:
                return None
            result = dict(self.job)
            path = self.meta / "jobs" / self.job["id"] / "output.log"
            if path.exists():
                with path.open("rb") as stream:
                    stream.seek(max(0, path.stat().st_size - 16000))
                    result["log"] = stream.read().decode("utf-8", errors="replace")
            return result

    def start_job(self):
        with self.lock:
            if self.closing or (self.job and self.job["status"] == "running"):
                raise RuntimeError("Một lượt kiểm tra đang chạy. Vui lòng đợi hoàn tất.")
            job_id = uuid.uuid4().hex
            folder = self.meta / "jobs" / job_id
            folder.mkdir(parents=True)
            self.job = {"id": job_id, "status": "running", "started_at": now(), "run_id": None}
            save_json(folder / "job.json", self.job)
            self.worker = threading.Thread(target=self._execute, args=(folder,), daemon=True)
            self.worker.start()
            return dict(self.job)

    def _execute(self, folder):
        outcome = {}
        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            with (folder / "output.log").open("w", encoding="utf-8") as log:
                with self.lock:
                    if self.closing:
                        raise RuntimeError("Web đã dừng trước khi bắt đầu xử lý.")
                    # Fixed argv; no command, path or configuration from HTTP.
                    self.process = subprocess.Popen(
                        [sys.executable, "-m", "scripts.data_quality_demo", "--output-dir", str(folder / "output")],
                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                        start_new_session=(os.name == "posix"),
                    )
                code = self.process.wait()
            outputs = list((folder / "output").glob("run-*/report.json"))
            passed = code == 0 and len(outputs) == 1 and json.loads(outputs[0].read_text()).get("passed") is True
            outcome = dict(status="passed" if passed else "failed", exit_code=code,
                           run_id=outputs[0].parent.name if outputs else None, finished_at=now())
        except Exception as error:
            LOG.exception("Quality job failed")
            outcome = dict(status="failed", error=str(error), finished_at=now())
        finally:
            with self.lock:
                self.job.update(outcome)
                self.process = None
                save_json(folder / "job.json", self.job)

    def close(self):
        with self.lock:
            self.closing = True
            process = self.process
        if process and process.poll() is None:
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            else:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait()
        if self.worker:
            self.worker.join(timeout=6)


class Handler(BaseHTTPRequestHandler):
    def send(self, status, data, content_type="application/json; charset=utf-8", download=None):
        body = json.dumps(data, ensure_ascii=False).encode() if content_type.startswith("application/json") else data
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if download:
            self.send_header("Content-Disposition", f'attachment; filename="{download}"')
        self.end_headers()
        self.wfile.write(body)

    def handle_request(self, mutate=False):
        store = self.server.store
        port = self.server.server_port
        hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
        if self.headers.get("Host") not in hosts:
            return self.send(403, {"error": "Web chỉ phục vụ trên localhost."})
        path = urlsplit(self.path).path
        try:
            if mutate:
                if (self.headers.get("Origin") not in {f"http://{host}" for host in hosts}
                        or not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""), store.token)):
                    return self.send(403, {"error": "Phiên web không hợp lệ. Hãy tải lại trang."})
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= 20000:
                    return self.send(413, {"error": "Nội dung quá lớn."})
                data = json.loads(self.rfile.read(length))
                if not isinstance(data, dict):
                    raise ValueError("Nội dung phải là JSON object.")
                if path == "/api/jobs":
                    return self.send(202, store.start_job())
                match = re.fullmatch(r"/api/runs/(run-[A-Za-z0-9_-]+)/reviews/([A-Za-z0-9_-]+)", path)
                if match:
                    return self.send(200, store.review(*match.groups(), data))
            else:
                if path == "/api/state":
                    return self.send(200, {"runs": store.runs(), "job": store.job_state(), "csrf_token": store.token})
                match = re.fullmatch(r"/api/runs/(run-[A-Za-z0-9_-]+)(/download)?", path)
                if match:
                    result = store.report(match[1])
                    if match[2]:
                        return self.send(200, result["report"], download=f"{match[1]}.json")
                    return self.send(200, result)
                assets = {"/": ("index.html", "text/html; charset=utf-8"),
                          "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                          "/style.css": ("style.css", "text/css; charset=utf-8")}
                if path in assets:
                    filename, mime = assets[path]
                    return self.send(200, (STATIC / filename).read_bytes(), mime)
            self.send(404, {"error": "Không tìm thấy nội dung."})
        except FileNotFoundError:
            self.send(404, {"error": "Lần chạy chưa có báo cáo hoàn tất hoặc không còn tồn tại."})
        except (ValueError, KeyError, TypeError) as error:
            self.send(400, {"error": str(error)})
        except RuntimeError as error:
            self.send(409, {"error": str(error)})
        except OSError:
            LOG.exception("Storage failure")
            self.send(500, {"error": "Không đọc/ghi được kết quả. Kiểm tra quyền truy cập thư mục."})

    def do_GET(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request(mutate=True)


def create_server(results, port=8765):
    store = Store(results)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.store = store
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--results", type=Path, default=ROOT / "results/data-quality")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    server = create_server(args.results, args.port)
    print(f"HospitalLake · Chất lượng dữ liệu: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.store.close()
        server.server_close()


if __name__ == "__main__":
    main()
