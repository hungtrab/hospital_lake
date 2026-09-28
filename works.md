# HospitalLake — 9-Week Work Allocation

> Kế hoạch chia việc cho **4 thành viên ngang hàng**, không phân biệt lead/phó/member.  
> Mục tiêu: trong **9 tuần**, cả 4 người đều có **code chạy được mỗi tuần**, workload tương đối cân bằng, và cuối mỗi tuần hệ thống phải tiến thêm một mốc tích hợp rõ ràng.

---

# 1. Nguyên tắc chia việc

## 1.1 Không chia theo chức danh

Sử dụng ký hiệu:

- **TV1**
- **TV2**
- **TV3**
- **TV4**

Không có lead/phó/member. Mỗi người đều chịu trách nhiệm implement, test, review và integration.

---

## 1.2 Yêu cầu commit hàng tuần

Mỗi thành viên, mỗi tuần phải có ít nhất:

- **1 PR có code/config executable thực chất** được merge;
- ít nhất **1 test hoặc smoke check** liên quan trực tiếp tới phần mình làm;
- commit phải nằm trong một trong các loại:
  - Python/Scala application code;
  - Spark job;
  - SQL;
  - Kafka producer/consumer;
  - test code;
  - Docker/Kubernetes/Helm configuration;
  - dashboard provisioning JSON;
  - benchmark/load-test script;
  - automation script.

Các thay đổi sau **không được tính là task chính của tuần**:

- chỉ sửa README;
- chỉ update tài liệu;
- chỉ đổi tên file;
- format/lint đơn thuần;
- commit placeholder/TODO;
- chỉ review code người khác.

Documentation vẫn có thể cập nhật khi cần, nhưng không được dùng thay cho nhiệm vụ kỹ thuật.

---

## 1.3 Definition of Done cho một task

Một task chỉ được coi là hoàn thành khi:

1. code chạy được;
2. có cách reproduce bằng command/script;
3. có test hoặc validation;
4. không làm hỏng pipeline hiện tại;
5. được merge vào nhánh chính;
6. output của task dùng được bởi ít nhất một block tiếp theo trong pipeline.

---

# 2. Cách cân bằng workload

Kế hoạch cố tình không để một người làm mãi một mảng.

Trong 9 tuần, mỗi người đều phải chạm vào nhiều lớp:

- data generation;
- Spark batch;
- Lakehouse/Iceberg;
- Kafka/streaming;
- serving/query;
- Kubernetes;
- testing/benchmark;
- integration/hardening.

Mỗi tuần workload mục tiêu khoảng **6–8 story points/người**.

Quy ước gần đúng:

| Loại task | Story point |
|---|---:|
| Feature nhỏ + test | 2 |
| Component hoàn chỉnh | 3 |
| Integration giữa 2 service | 3 |
| Distributed/K8s component | 3–4 |
| Benchmark/fault-recovery | 3–4 |

Không cần chấm point tuyệt đối; mục đích là tránh tình trạng một người làm một service lớn còn người khác chỉ sửa dashboard/docs.

---

# 3. Kế hoạch 9 tuần

---

# Tuần 1 — Bootstrap + Data Model + Local Infrastructure

## Mục tiêu cuối tuần

Có thể chạy local:

```text
Synthetic schema
      +
MinIO
      +
Iceberg Catalog
      +
Trino
      +
Spark connectivity
```

và repo có cấu trúc đủ để 4 người làm song song.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Implement core Python data models/schema cho `patient`, `encounter`, `lab_result`; tạo generator bản nhỏ | Generate được dataset 1,000 patients; FK patient/encounter hợp lệ; có unit tests |
| **TV2** | Dựng `docker-compose` cho MinIO + Iceberg REST Catalog + network/volumes; bootstrap bucket | `docker compose up` chạy ổn; bucket warehouse tự tạo; health checks pass |
| **TV3** | Tạo Spark common module: config, SparkSession, Iceberg catalog config, S3/MinIO connector | Một Spark smoke job tạo/đọc được Iceberg test table |
| **TV4** | Dựng Trino + Iceberg catalog config; viết integration smoke query và script kiểm tra toàn stack | Trino query được Iceberg table do TV3 tạo; smoke test trả exit code đúng |

### Integration cuối tuần

```text
Spark
  ↓
Iceberg table on MinIO
  ↓
Trino SELECT
```

---

# Tuần 2 — Synthetic Big Dataset + Bronze Layer

## Mục tiêu cuối tuần

Có pipeline:

```text
Synthetic Parquet
      ↓
Spark
      ↓
Bronze Iceberg
      ↓
Trino
```

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Mở rộng generator cho `medication`, `vital`, `bed_event`; thêm configurable scale + deterministic seed | Generate được 10k/100k patients từ config; Parquet output; chạy lại cùng seed cho kết quả deterministic |
| **TV2** | Implement Iceberg Bronze schemas/tables + partition strategy + table bootstrap script | Tạo được toàn bộ `bronze.*`; table metadata lưu trên MinIO; Trino nhìn thấy |
| **TV3** | Implement Spark `ingest_bronze.py`: raw Parquet → Bronze; ingestion metadata; idempotent batch ID | Chạy lại cùng batch không duplicate ngoài thiết kế; log input/output rows |
| **TV4** | Implement batch validation harness + Trino verification queries + row-count/checksum integration test | Tự động kiểm tra source count vs Bronze count; phát hiện ingestion lỗi |

### Integration cuối tuần

```bash
make generate-data
make batch-bronze
make verify-bronze
```

phải chạy end-to-end.

---

# Tuần 3 — Silver + Gold Batch Pipeline

## Mục tiêu cuối tuần

Hoàn thành:

```text
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Trino analytics
```

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Implement Silver transformations cho `patients` + `encounters`: normalize department, timestamps, dedup | Test được duplicate, invalid timestamp, department aliases |
| **TV2** | Implement Silver transformations cho `labs`, `medications`, `vitals`, `bed_events` | Dữ liệu typed/clean; abnormal lab flag đúng; invalid vital bị quarantine |
| **TV3** | Implement shared data-quality + quarantine framework cho Spark batch | Invalid rows có reason code; metrics `valid/invalid/duplicate` được emit |
| **TV4** | Implement Gold batch jobs: `hospital_hourly_metrics`, `department_daily_metrics`, `lab_daily_metrics`, `icu_occupancy` | Trino query được Gold; sample expected-value tests pass |

### Integration cuối tuần

Một command phải tạo được toàn bộ:

```text
raw
→ bronze
→ silver
→ gold
```

và chạy ít nhất 5 analytical SQL queries.

---

# Tuần 4 — Kafka + Event Simulator + Streaming Skeleton

## Mục tiêu cuối tuần

Live event đi được:

```text
Simulator
   ↓
Kafka
   ↓
Spark Structured Streaming
```

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Implement streaming event models + hospital event producer (`ADMISSION`, `DISCHARGE`, `LAB_RESULT`, `VITAL`,...) | Producer hỗ trợ `--rate`; event envelope đúng schema; deterministic seed |
| **TV2** | Dựng Kafka local stack + topic bootstrap + partition strategy + health/test scripts | Topics tự tạo; produce/consume smoke test pass; partition key theo patient |
| **TV3** | Implement Spark Structured Streaming Kafka reader: deserialize, schema validate, watermark, dedup | Consume live events; duplicate `event_id` bị xử lý; malformed event không crash job |
| **TV4** | Implement streaming Bronze sink + DLQ path + streaming integration test | Raw event ghi được xuống Iceberg; bad event vào DLQ/quarantine; test chạy liên tục 5 phút |

### Integration cuối tuần

```text
1000 events/s
→ Kafka
→ Spark
→ Bronze Iceberg
```

chạy ổn tối thiểu 10 phút.

---

# Tuần 5 — Realtime Analytics + ClickHouse + Grafana

## Mục tiêu cuối tuần

Có dashboard realtime thay đổi khi event chạy.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Implement realtime hospital-state aggregation: current patients, admission/discharge windows | Metrics thay đổi đúng theo known test event sequence |
| **TV2** | Implement realtime ICU/department/lab aggregations bằng Structured Streaming | Có `icu_occupancy`, `department metrics`, `abnormal labs last 15m`; event-time window hoạt động |
| **TV3** | Dựng ClickHouse schema + Spark/stream sink + retry/idempotency logic | Realtime rows query được trong vài giây; restart stream không tạo duplicate nghiêm trọng |
| **TV4** | Implement Grafana provisioning + datasource + 3 dashboard JSON: Overview, Realtime, Historical | `docker compose up` tự load dashboard; không cần click-config bằng tay |

### Integration cuối tuần

Demo:

```text
ADMISSION event
      ↓
Kafka
      ↓
Streaming
      ↓
ClickHouse
      ↓
Grafana

Current patients: N → N+1
```

---

# Tuần 6 — Kubernetes Migration

## Mục tiêu cuối tuần

Toàn bộ core platform chạy trên Kubernetes.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Kubernetes hóa MinIO + Iceberg Catalog + Trino: manifests/Helm values/PVC/config | Các service ready; Trino query Iceberg trên cluster |
| **TV2** | Dựng Strimzi Kafka trên K8s + KafkaTopic resources + producer deployment | Producer trong cluster push được events; broker restart vẫn recover |
| **TV3** | Dựng Spark Operator + `SparkApplication` cho batch và streaming jobs | Submit được batch SparkApplication và long-running streaming SparkApplication |
| **TV4** | Kubernetes hóa ClickHouse + Grafana + Prometheus + persistent volume/service config | Grafana và Prometheus truy cập được; ClickHouse giữ data qua pod restart |

### Integration cuối tuần

```bash
kubectl get pods -n hospital-lake
```

tất cả core service phải healthy và một flow:

```text
producer → Kafka → Spark → ClickHouse → Grafana
```

chạy trên K8s.

---

# Tuần 7 — Observability + Scale + Recovery

## Mục tiêu cuối tuần

Chứng minh K8s/distributed processing có ý nghĩa thật.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Instrument Spark batch + streaming custom metrics; expose throughput/latency metrics | Prometheus scrape được `rows/sec`, `events/sec`, batch duration |
| **TV2** | Implement Kafka lag monitoring + scalable load generator/load profiles | Có command tạo 100/1k/5k/10k events/s; Kafka lag hiển thị trên Grafana |
| **TV3** | Implement Spark scaling scripts/config; test 2→4 executors và collect throughput | Có script reproducible; đo được lag/throughput trước và sau scale |
| **TV4** | Implement failure-injection + recovery test harness cho Spark executor/service pods | Script tự kill pod, chờ recover, kiểm tra stream tiếp tục xử lý |

### Integration cuối tuần

Phải demo được:

```text
load ↑
→ lag ↑
→ executors ↑
→ throughput ↑
→ lag ↓
```

và:

```text
kill Spark executor
→ K8s restart/recover
→ stream tiếp tục
```

---

# Tuần 8 — Benchmark + End-to-End Reliability

## Mục tiêu cuối tuần

Có benchmark thật, script chạy lại được, không chỉ screenshot.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Implement batch benchmark runner cho 100k/500k/1M patients và 1/2/4 executors | Xuất machine-readable CSV/JSON: runtime, rows/s, CPU/memory nếu lấy được |
| **TV2** | Implement streaming benchmark runner cho 100/1k/5k/10k events/s | Ghi throughput, consumer lag, end-to-end latency theo profile |
| **TV3** | Implement Iceberg maintenance/performance jobs: compaction/small-file handling + benchmark before/after | Có measurable table/file-count/query improvement hoặc kết luận rõ nếu không cải thiện |
| **TV4** | Implement automated E2E regression suite: generator → batch → Kafka → stream → ClickHouse/Trino assertions | Một command chạy smoke/E2E và fail rõ block nào lỗi |

### Integration cuối tuần

Chạy:

```bash
make benchmark
make e2e
```

và thu được kết quả tái lập được.

---

# Tuần 9 — Hardening + Final Integrated Build

## Mục tiêu cuối tuần

Không thêm feature lớn mới. Tập trung biến toàn bộ thành một system ổn định để demo/chấm.

| Thành viên | Nhiệm vụ code | Acceptance criteria |
|---|---|---|
| **TV1** | Hardening data generation + batch pipeline: large-run bugs, memory issues, idempotency, deterministic demo dataset | Full batch pipeline chạy clean ít nhất 2 lần liên tiếp |
| **TV2** | Hardening Lakehouse/query path: Iceberg partitions, Trino query performance, MinIO persistence, failure cases | Restart storage/query pods không làm mất warehouse; query set pass |
| **TV3** | Hardening streaming path: restart Kafka/Spark, checkpoint recovery, DLQ, late/duplicate event tests | 30-minute stress run không crash; restart job resume được |
| **TV4** | Hardening K8s + serving layer: one-command deploy/reset/demo, dashboard provisioning, health gates | Fresh cluster → deploy → demo được bằng scripted commands, không config tay |

### Cross-integration bắt buộc

Trong tuần 9, mỗi thành viên phải sửa ít nhất **1 integration bug nằm ngoài module chính của mình**.

Ví dụ:

- TV1 sửa bug Spark ↔ Iceberg;
- TV2 sửa bug Kafka ↔ streaming;
- TV3 sửa bug ClickHouse/Grafana;
- TV4 sửa bug batch/Trino.

Mục đích: đảm bảo không ai chỉ hiểu một silo duy nhất.

---

# 4. Ma trận trách nhiệm tổng thể

| Mảng | TV1 | TV2 | TV3 | TV4 |
|---|---:|---:|---:|---:|
| Data generation | ●●● | ● | ● | ● |
| Spark batch | ●● | ●● | ●●● | ●● |
| Iceberg/Lakehouse | ● | ●●● | ●● | ●● |
| Kafka | ●● | ●●● | ●● | ●● |
| Structured Streaming | ●●● | ●● | ●●● | ●● |
| ClickHouse | ● | ● | ●●● | ●● |
| Grafana | ● | ● | ● | ●●● |
| Kubernetes | ●● | ●●● | ●●● | ●●● |
| Monitoring | ●● | ●●● | ●● | ●●● |
| Benchmark/Test | ●●● | ●●● | ●●● | ●●● |

`●●●` không có nghĩa là "owner cao hơn", chỉ có nghĩa người đó có nhiều task trực tiếp hơn trong mảng đó. Tổng effort 9 tuần phải tương đương nhau.

---

# 5. Khối lượng dự kiến theo tuần

Để tránh lệch workload:

| Tuần | TV1 | TV2 | TV3 | TV4 |
|---|---:|---:|---:|---:|
| 1 | 7 SP | 7 SP | 7 SP | 7 SP |
| 2 | 7 SP | 7 SP | 8 SP | 6–7 SP |
| 3 | 7 SP | 7 SP | 7 SP | 8 SP |
| 4 | 7 SP | 7 SP | 8 SP | 7 SP |
| 5 | 7 SP | 7 SP | 8 SP | 7 SP |
| 6 | 8 SP | 8 SP | 8 SP | 8 SP |
| 7 | 7 SP | 7 SP | 8 SP | 8 SP |
| 8 | 8 SP | 8 SP | 8 SP | 8 SP |
| 9 | 7 SP | 7 SP | 7 SP | 7 SP |

Sai lệch nhỏ là bình thường; nếu một task phát sinh lớn hơn dự kiến thì chuyển một subtask cho người có workload thấp hơn ngay trong tuần.

---

# 6. Quy tắc Git/PR

## Branch

Mỗi task dùng branch:

```text
feat/<week>-<short-name>
fix/<week>-<short-name>
bench/<week>-<short-name>
infra/<week>-<short-name>
```

Ví dụ:

```text
feat/w3-silver-encounters
infra/w6-strimzi-kafka
bench/w8-streaming-load
```

---

## PR

Mỗi PR phải ghi:

```text
What:
- Component được implement

How to run:
- command cụ thể

How tested:
- test/smoke command

Output:
- expected result
```

Không merge PR nếu không có cách chạy/test rõ ràng.

---

## Review

Mỗi tuần rotate reviewer:

```text
TV1 reviews TV2
TV2 reviews TV3
TV3 reviews TV4
TV4 reviews TV1
```

Tuần tiếp theo rotate thêm một bước để không hình thành cặp cố định.

Review không thay thế task code cá nhân.

---

# 7. Weekly Integration Gate

Không được coi một tuần là hoàn thành chỉ vì 4 branch đều có code.

Cuối mỗi tuần phải chạy một integration gate.

| Tuần | Gate |
|---|---|
| 1 | Spark → Iceberg → Trino |
| 2 | Generated Parquet → Bronze → verify |
| 3 | Bronze → Silver → Gold → analytical SQL |
| 4 | Simulator → Kafka → Streaming → Bronze |
| 5 | Event → Kafka → Spark → ClickHouse → Grafana |
| 6 | Full core flow chạy trên Kubernetes |
| 7 | Scale + pod failure recovery |
| 8 | Benchmark + automated E2E |
| 9 | Fresh deploy → full demo without manual patching |

Nếu gate fail thì ưu tiên sửa integration trước khi mở feature mới.

---

# 8. Phân bổ file/module chính

Đây chỉ là phân bổ ban đầu để tránh conflict. Không phải quyền sở hữu độc quyền.

## TV1

Chạm nhiều vào:

```text
generators/
spark/batch/
spark/streaming/realtime state
scripts/benchmark batch
```

## TV2

Chạm nhiều vào:

```text
infra/docker/
sql/iceberg/
infra/k8s/kafka/
load generator
```

## TV3

Chạm nhiều vào:

```text
spark/common/
spark/streaming/
infra/k8s/spark/
iceberg maintenance
```

## TV4

Chạm nhiều vào:

```text
sql/trino/
sql/clickhouse/
dashboard/grafana/
infra/k8s/observability/
tests/e2e/
```

Đến tuần 7–9 phải cross-module để tránh silo.

---

# 9. Deliverables sau 9 tuần

Sau tuần 9, cả nhóm phải có code cho toàn bộ:

```text
Synthetic Hospital Dataset
          |
          +------------------------+
          |                        |
          v                        v
      Spark Batch                Kafka
          |                        |
          v                        v
       Bronze              Structured Streaming
          |                        |
          v                        |
        Silver <-------------------+
          |
          v
         Gold
        /    \
       /      \
    Trino   ClickHouse
       \      /
        \    /
        Grafana
```

và:

```text
Docker/local
     +
Kubernetes
     +
Prometheus
     +
Benchmark
     +
Failure Recovery
     +
Automated E2E Tests
```

---

# 10. Tiêu chí công bằng cuối kỳ

Trước khi kết thúc project, kiểm tra lịch sử Git.

Mỗi thành viên phải có:

- code commit trong **cả 9 tuần**;
- feature code ở ít nhất **4 lớp khác nhau** của hệ thống;
- ít nhất **1 task Spark/data processing**;
- ít nhất **1 task streaming hoặc Kafka**;
- ít nhất **1 task K8s/infra executable**;
- ít nhất **1 task test/benchmark/integration**;
- ít nhất **1 integration bug fix ngoài module chính**.

Không để tình trạng:

```text
Một người: Spark + Kafka + K8s toàn bộ
Một người: dashboard
Một người: docs
Một người: slides
```

Kế hoạch này cố tình buộc cả 4 người đều có contribution kỹ thuật đủ rõ để nhìn vào Git history cũng thấy workload tương đối cân bằng.

---

# 11. Tóm tắt phân chia theo phase

```text
Weeks 1–3
=========
Build the Lakehouse
Data → Bronze → Silver → Gold → Trino

Weeks 4–5
=========
Build realtime path
Events → Kafka → Spark Streaming → ClickHouse → Grafana

Week 6
======
Move core system to Kubernetes

Week 7
======
Observability + scaling + failure recovery

Week 8
======
Benchmark + automated E2E

Week 9
======
Hardening + full integrated demo
```

Mọi tuần, cả **TV1–TV4 đều phải merge executable code**.
