# next_step.md — Kế hoạch tới Checkpoint 1 (thứ Hai 26/10/2026)

> Thông báo của thầy: *"Next week 26/10 we would have our first checkpoint for your milestone project. Every group have to show some progress: git is ready with executable code, data and system architecture must be ready also."*
>
> File này là kế hoạch sprint ngắn **22/10 → 26/10**. Nó **bổ sung** cho `works.md` (lịch 9 tuần) và **không thay thế** `AGENTS.md`. Mọi agent vẫn phải đọc `plan.md`, `works.md`, `AGENTS.md`, `sitemap.md` trước khi code.

---

## 1. Checkpoint cần show được gì

| Thầy yêu cầu | Nhóm sẽ show | Lệnh / file |
|---|---|---|
| **Git ready with executable code** | `main` sạch, mọi PR đã merge, cả 4 TV đều có commit code thật; clone mới → chạy được | `git log`, `make help`, `make test` |
| **Data ready** | Dữ liệu synthetic 6 entity → Bronze → Silver → Gold trên Iceberg/MinIO, query được bằng Trino | `make batch` (end-to-end), `make query` |
| **System architecture ready** | Sơ đồ kiến trúc Lambda đầy đủ, bảng component, luồng batch/stream, ghi rõ phần nào **đã chạy** / **đang làm** / **kế hoạch** | `docs/architecture.md`, `docs/data-model.md` |
| (điểm cộng) Streaming | Producer → Kafka → Spark Structured Streaming → Bronze Iceberg | `make kafka-up`, `make stream-bronze-docker` |

**Checkpoint gate (bắt buộc pass trước tối CN 25/10):** trên một clone mới

```bash
cp .env.example .env
make up
make batch          # generate → bootstrap Bronze → ingest Bronze → verify → Silver (+ Gold khi TV4 xong T4.1)
make query          # các analytical query trên Trino trả về dữ liệu thật
make test
```

Streaming là điểm cộng: làm song song nhưng **không được làm chậm** batch gate (batch là P0, stream là P1 — `AGENTS.md` §6).

---

## 2. Hiện trạng repo (22/10)

| Tuần / gate | Trạng thái |
|---|---|
| W1 — Spark → Iceberg → Trino | ✅ Pass (`make week1-smoke`, `make week1-recovery`, 12/10) |
| W2 — Parquet → Bronze → verify | ✅ Gate live pass 22/10 (1.000 patients, 6 bảng khớp số dòng + checksum ID) sau khi sửa 2 bug tích hợp — **PR #10 đã merge** |
| W3 — Bronze → Silver → Gold → SQL | ⚠️ **Đang hở**: framework data-quality (TV3) ✅, Gold job (TV4) ✅, Silver patients/encounters (TV1) ✅ **PR #12 merged**, `make batch` tới Silver (TV1, T1.2); Silver clinical (TV2) và `gold-docker` (TV4) chưa có ⇒ Gold chưa chạy được trên dữ liệu thật |
| W4 — Simulator → Kafka → Streaming → Bronze | ⚠️ Kafka reader + dedup (TV3) ✅, stream Bronze sink + DLQ (TV4) ✅, producer (TV1) ở **PR #13**; **thiếu Kafka local stack (TV2)** |

Vấn đề phát hiện khi chạy lại test (TV1, 22/10):

- **`make batch-bronze-docker` hỏng trên `main`** (đã sửa ở PR #10):
  1. Image `apache/spark:3.5.5-python3` chạy **Python 3.8** → annotation `list[dict]` crash lúc import. Mọi module chạy trong container Spark phải có `from __future__ import annotations` (đã có unit test chặn).
  2. Ghi bảng partition theo ngày bằng fanout writer → OOM driver 512 MiB. Ghi Iceberg dùng `.option("fanout-enabled", "false")`.
- `pytest tests/unit` pass 93/93 và `make test` (unit + integration) 106 passed / 10 skipped khi `TZ=UTC`, nhưng `tests/unit/test_gold_metrics.py::test_hourly_and_icu_metrics` **fail trên máy múi giờ +07** (Spark trả timestamp theo giờ local khi `collect()`). → TV4 sửa (T4.1).
- Máy có `JAVA_HOME` trỏ Java 8 hoặc worker Python khác driver sẽ làm test Spark lỗi. Khi chạy local: `JAVA_HOME=<java 17+>`, `PYSPARK_PYTHON=.venv/bin/python`.
- 3 commit của TV3 (`dce4ce9`, `8f1e538`, `46fdacf`) được push **thẳng lên `main`**, không qua PR/audit. Từ giờ **mọi thay đổi đi qua PR** (mục 5).

---

## 3. Giao việc

Thời hạn chung: **PR mở + audit PASS trước 23:59 thứ Bảy 24/10**. TV1 merge **ngay khi** PR có audit PASS và các PR phụ thuộc đã vào `main` (xem mục 4); Chủ nhật 25/10 dành cho những merge cuối + chạy rehearsal. Thứ tự ưu tiên trong mỗi TV: làm từ trên xuống.

### TV1 — hungtrab (Silver patients/encounters + orchestration)

| # | Việc | Branch | Acceptance |
|---|---|---|---|
| T1.1 | `spark/batch/bronze_to_silver.py` (framework entity registry dùng `spark/common/data_quality.py`) + Silver `patients`, `encounters`; `spark/common/normalization.py` (alias khoa → tên chuẩn); `sql/iceberg/create_silver_tables.sql` (patients, encounters, `silver.quarantine`); Compose service + `make batch-silver-docker` | `feat/w3-silver-patients-encounters` | Alias khoa được chuẩn hóa; duplicate y hệt bị gộp, duplicate mâu thuẫn + encounter lỗi (discharge < admission, patient không tồn tại…) vào `silver.quarantine` kèm reason code; chạy lại cho cùng kết quả; unit test Spark + chạy live trên stack |
| T1.2 | `make batch` (generate → bootstrap Bronze → ingest → Silver → Gold) + mục Silver trong `docs/data-model.md` | `feat/w3-batch-pipeline` | Một lệnh chạy hết batch path trên stack local, exit code ≠ 0 khi một bước lỗi |
| T1.3 | (sau batch gate) Stream producer `generators/stream_generator/` (`producer.py --rate`, envelope chuẩn, late/duplicate/invalid events) — nợ Tuần 4 | `feat/w4-stream-producer` | Producer đẩy event vào Kafka của TV2; seed deterministic; unit test |
| — | **Merge owner**: review kết quả audit của mọi PR, merge, chạy rehearsal CN 25/10 | — | Checkpoint gate pass trên clone mới |

### TV2 — Vũ Thường Tín (Silver clinical tables + Kafka local)

| # | Việc | Branch | Acceptance |
|---|---|---|---|
| T2.1 | Silver `lab_results`, `bed_events` (Gold cần 2 bảng này — **làm trước**), rồi `medications`, `vitals`: thêm entity vào registry trong `bronze_to_silver.py` + DDL vào `create_silver_tables.sql`. Bắt đầu **ngay khi T1.1 merge** (dự kiến sáng thứ Sáu 23/10) | `feat/w3-silver-clinical` | Kiểm tra FK tới `silver.encounters` (encounter bị quarantine ⇒ bản ghi con vào quarantine với `UNKNOWN_ENCOUNTER`); tính lại `is_abnormal` từ reference range; vitals ngoài miền hợp lệ (`heart_rate > 0`, `0 ≤ spo2 ≤ 100`, nhiệt độ hợp lý) → quarantine; event bed hợp lệ (`assigned/transferred/released`) |
| T2.2 | Kafka local trong `docker-compose.yml` (single broker KRaft) + tạo topic tự động `hospital.events`, `hospital.dlq` (khớp `KAFKA_TOPICS` hiện có) + `make kafka-up` + smoke produce/consume | `infra/w4-kafka-local` | `make kafka-up` từ clone mới; topic tự tạo; smoke test pass; partition key = `patient_id` được ghi trong doc |

### TV3 — Duck (Architecture + streaming chạy trong Compose)

| # | Việc | Branch | Acceptance |
|---|---|---|---|
| T3.1 | Tài liệu đi kèm (**deliverable code chính của TV3 là T3.2**; commit chung branch `feat/` của T3.2): `docs/architecture.md` viết lại cho **toàn hệ thống** (hiện chỉ mô tả hạ tầng Week 1): sơ đồ Lambda (batch / speed / serving), bảng component (công nghệ, vai trò, layer, **trạng thái: chạy được / đang làm / kế hoạch**), luồng batch và stream qua từng bảng/topic, mapping thư mục code, kế hoạch K8s (Tuần 6). Chỉ ghi “chạy được” cho thứ đã có lệnh tái lập (`AGENTS.md` §38) | `feat/w4-stream-bronze-compose` (cùng T3.2) | TV1 đọc và đối chiếu được từng box trong sơ đồ với file code / lệnh `make` |
| T3.2 | Chạy `spark.streaming.kafka_to_bronze` trong Compose: service `stream-bronze` (Spark image có sẵn + Kafka connector), checkpoint trên volume, `make stream-bronze-docker`. Phụ thuộc T2.2; nếu T2.2 chưa xong thì phát triển/test với một container Kafka tạm (KRaft single node) rồi chuyển sang service của TV2 khi merge | `feat/w4-stream-bronze-compose` | Event từ Kafka của TV2 vào `bronze` stream tables; restart service không tạo duplicate (checkpoint) |
| T3.3 | PR audit bù cho 3 commit đã push thẳng `main` (`dce4ce9..46fdacf`): mở PR “audit-only” hoặc ghi kết quả audit vào PR T3.2 | — | Có bản audit độc lập (model/effort, SHA, verdict) |

### TV4 — frichickens (Gold chạy thật + query + demo)

| # | Việc | Branch | Acceptance |
|---|---|---|---|
| T4.1 | Sửa `test_gold_metrics` để không phụ thuộc múi giờ máy; Gold chạy trong Compose: service `gold` (giống `bronze-ingest`), `make gold-docker`, giá trị mặc định `GOLD_AS_OF` / `GOLD_BED_CAPACITIES` trong `.env.example` (đủ 5 khoa chuẩn); thêm bước `$(MAKE) gold-docker` vào cuối target `batch` | `feat/w3-gold-compose` | `make test` pass ở `TZ=Asia/Ho_Chi_Minh`; `make gold-docker` ghi 4 bảng Gold từ Silver thật (sau T1.1 + T2.1 lab/bed) |
| T4.2 | `make query`: chạy `sql/trino/queries.sql` qua Trino, fail nếu query trả rỗng/lỗi; bổ sung query trên Silver + Gold (admissions/day, avg LOS theo khoa, abnormal lab rate, ICU utilization, patient history) | `feat/w3-trino-queries` | 5+ query chạy trên dữ liệu thật, in kết quả rõ ràng |
| T4.3 | `docs/demo-script.md` + `scripts/checkpoint_demo.sh`: kịch bản trình bày checkpoint (lệnh chính xác, output mong đợi) | đi kèm T4.2 | TV1 chạy theo script trên clone mới không phải sửa tay |

---

## 4. Lịch & phụ thuộc

```text
Thu 22/10   TV1: PR #10 (fix Bronze) ✅ merged; T1.1 Silver framework + patients/encounters (PR #12) → audit → merge (tối)
            TV2: T2.2 Kafka local     TV3: T3.1 architecture draft     TV4: T4.1 fix TZ test + gold service
Fri 23/10   TV2: T2.1 (dựa trên T1.1)   TV4: T4.2 queries   TV3: T3.2 (khi T2.2 xong)   TV1: T1.2 make batch
Sat 24/10   23:59 — mọi PR mở + audit PASS.   TV1: T1.3 producer nếu batch đã ổn
Sun 25/10   TV1 merge theo thứ tự phụ thuộc → rehearsal trên clone mới → sửa integration bug (ai cũng tham gia)
Mon 26/10   Checkpoint
```

Phụ thuộc chính (thứ tự merge):

```text
PR #10 fix Bronze (✅) ──► T1.1 (PR #12) ──► …
T1.1 Silver patients/encounters ──► T2.1 Silver clinical ──► T4.1 Gold trên dữ liệu thật ──► T4.2 make query
                                  └──────────────────────────► T1.2 make batch ◄── T4.1 gold-docker
T2.2 Kafka local ──► T3.2 stream-bronze-docker ──► T1.3 producer (live test)
```

Week 4 gate đầy đủ (1000 events/s chạy 10 phút) **hoãn sang sau checkpoint**; checkpoint chỉ cần luồng stream chạy được (điểm cộng).

Nếu T2.1 trễ: T4 vẫn test Gold bằng fixture; TV1 sẽ nhận phần `bed_events` để không chặn Gold (`AGENTS.md` §7 — task bị block).

---

## 5. Luật làm việc cho sprint này

1. **Không push thẳng `main`.** Mỗi task = 1 branch `feat|infra|fix|bench/w<tuần>-<tên>` (`works.md` §6; riêng PR chỉ có tài liệu điều phối dùng `docs/…`) + 1 PR theo template `AGENTS.md` §32.
2. **Audit độc lập bắt buộc** (`AGENTS.md` §33): agent của mỗi TV spawn auditor riêng, ghi model/effort, base/head SHA, verdict, các check đã chạy vào PR.
3. **Quyết định merge cuối cùng thuộc TV1 (hungtrab) trong sprint checkpoint.** Audit PASS ⇒ PR sẵn sàng merge, nhưng **không tự merge**; TV1 merge theo thứ tự phụ thuộc ở mục 4 sau khi chạy lại test trên `main` mới nhất. (Ngoại lệ tạm thời cho bước 5 của `AGENTS.md` §33 và mục Review của `works.md` §6 — quyết định của thành viên TV1 cho sprint này, chỉ áp dụng đến hết 26/10; không tạo quan hệ owner lâu dài. Thành viên nào phản đối thì nêu trong PR này.)
4. Commit đứng tên **người thật** trong `git config` — không AI identity, không `Co-Authored-By` AI (`AGENTS.md` §31).
5. Chạy test local với `TZ=UTC` *và* một lần với `TZ=Asia/Ho_Chi_Minh`; `JAVA_HOME` Java 17+; `PYSPARK_PYTHON=.venv/bin/python`.
6. Port mặc định (9000/9001/8080) trùng thì đổi trong `.env` (`MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `TRINO_PORT`) — không sửa `docker-compose.yml`.
7. Shared contract (tên bảng, cột, topic, biến môi trường) đổi thì phải grep toàn repo và sửa consumer trong cùng PR (`AGENTS.md` §34). Cập nhật `sitemap.md` khi thêm file/thư mục/lệnh mới.

---

## 6. Contract Silver (để TV2/TV4 làm song song với TV1)

Silver = **tính lại toàn bộ từ Bronze** mỗi lần chạy (overwrite) ⇒ chạy lại an toàn.

| Bảng | Khóa | Cột chính (ngoài lineage) |
|---|---|---|
| `silver.patients` | `patient_id` | `gender` (`M/F/O`), `birth_year`, `city`, `created_at` |
| `silver.encounters` | `encounter_id` | `patient_id`, `encounter_type` (`emergency/inpatient/outpatient/icu`), `department` (5 tên chuẩn), `admission_time`, `discharge_time` (null = đang nằm viện), `status` (`admitted/discharged`), `length_of_stay_hours` |
| `silver.lab_results`, `silver.medications`, `silver.vitals`, `silver.bed_events` | khóa như Bronze | TV2 định nghĩa theo `plan.md` §5; Gold cần `lab_results(lab_result_id, test_name, is_abnormal, measured_at)`, `bed_events(event_id, event_type, event_time)` |
| `silver.quarantine` | — | `entity`, `record_key`, `reason_codes` (array), `raw_record` (JSON), `_bronze_batch_id`, `_processed_at`; partition theo `entity` |

Mọi bảng Silver có thêm 2 cột lineage: `_bronze_batch_id`, `_processed_at`. Timestamp là `TIMESTAMP_LTZ`, session UTC.

Department không nhận ra được (không thuộc 5 tên chuẩn hoặc alias đã biết) ⇒ quarantine `UNKNOWN_DEPARTMENT`, không bao giờ đi vào Silver/Gold.

Xử lý trùng lặp: bản ghi **giống hệt nhau** (kể cả do ingest lại cùng file) được gộp lại 1 và đếm vào `exact_duplicate_rows`; bản ghi **cùng khóa nhưng khác giá trị** đều vào quarantine `DUPLICATE_KEY` (không tự chọn winner — theo `data_quality.py`).

Chi tiết API registry: xem docstring đầu `spark/batch/bronze_to_silver.py` sau khi T1.1 merge.
