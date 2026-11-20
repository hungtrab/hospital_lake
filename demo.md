# Hướng dẫn chạy HospitalLake

Mọi lệnh chạy từ **thư mục gốc repo** bằng `make`. Máy cần **Docker Compose v2**, **Python 3** và
**Java 17** (Java chỉ cần cho test Spark trên máy). Các job Spark chạy trong container, nên không phải cài Spark.
Kịch bản trình bày checkpoint kèm output mong đợi nằm ở [`docs/demo-script.md`](docs/demo-script.md).

## 1. Chuẩn bị (một lần)

```bash
git clone https://github.com/hungtrab/hospital_lake.git && cd hospital_lake
python3 -m venv .venv
make setup PYTHON=.venv/bin/python                          # tạo .env từ .env.example + cài thư viện
.venv/bin/python -m pip install -r requirements-spark.txt   # chỉ cần nếu chạy test Spark
```

Nếu port 9000/9001/8080/9092 đã bị chiếm, sửa trong `.env` (`MINIO_API_PORT`, `MINIO_CONSOLE_PORT`,
`TRINO_PORT`, `KAFKA_PORT`). **Không** sửa `docker-compose.yml`.

## 2. Bật server

```bash
make up          # MinIO + Iceberg REST catalog + Trino, đợi tới khi healthy
make kafka-up    # (tuỳ chọn) Kafka + tạo topic hospital.events, hospital.dlq
docker compose ps
```

- MinIO console: `http://localhost:<MINIO_CONSOLE_PORT>`, tài khoản và mật khẩu lấy trong `.env`.
- Tắt: `make down`. Dữ liệu vẫn giữ vì volume còn. Xoá sạch dữ liệu: `docker compose down -v`.

## 3. Chạy pipeline batch

```bash
make batch PYTHON=.venv/bin/python
```

Lệnh này lần lượt chạy:
1. Sinh dữ liệu: 1000 bệnh nhân, seed 42.
2. Tạo bảng Bronze.
3. Nạp dữ liệu vào Bronze.
4. Đối chiếu Bronze với file nguồn (số dòng + checksum ID).
5. Silver.
6. Gold.

Lần đầu Spark phải tải jar Iceberg, mạng chậm thì sẽ lâu. Nên chạy thử một lần trước khi demo.

## 4. Query

**Bộ 11 query có sẵn** (`sql/trino/queries.sql`). Query nào lỗi hoặc trả về rỗng thì lệnh báo lỗi:

```bash
make query PYTHON=.venv/bin/python
```

**Gõ SQL tự do** bằng Trino CLI:

```bash
docker compose exec trino trino
```

```sql
SHOW TABLES FROM iceberg.silver;
SELECT department, count(*) FROM iceberg.silver.encounters GROUP BY 1;
SELECT * FROM iceberg.gold.icu_occupancy ORDER BY "timestamp" DESC LIMIT 10;
SELECT entity, reason_codes, raw_record FROM iceberg.silver.quarantine LIMIT 5;
```

Chạy một câu rồi thoát:
`docker compose exec -T trino trino --execute "SELECT count(*) FROM iceberg.gold.lab_daily_metrics"`.

Có thể dùng DBeaver hoặc DataGrip kết nối Trino tại `localhost:<TRINO_PORT>`, catalog `iceberg`.

| Namespace | Bảng |
|---|---|
| `iceberg.bronze` | `patients_raw`, `encounters_raw`, `lab_results_raw`, `medications_raw`, `vitals_raw`, `bed_events_raw`, `stream_events_raw`, `stream_events_quarantine` |
| `iceberg.silver` | `patients`, `encounters`, `lab_results`, `medications`, `vitals`, `bed_events`, `quarantine` |
| `iceberg.gold` | `hospital_hourly_metrics`, `department_daily_metrics`, `lab_daily_metrics`, `icu_occupancy` |

## 5. Nhập dữ liệu mới

### a) Dữ liệu tổng hợp với cỡ hoặc seed khác

```bash
make batch PATIENTS=10000 SEED=7 PYTHON=.venv/bin/python
```

- Mã batch được tự đặt là `synthetic-p10000-s7`, và dữ liệu được **nạp thêm** vào Bronze.
- Chạy lại đúng cỡ và seed cũ thì bước Bronze được **bỏ qua**, nên không ghi trùng.
- Silver và Gold luôn được **tính lại** từ toàn bộ Bronze.

### b) File của bạn (Parquet hoặc CSV, đúng schema trong `docs/data-model.md`)

```bash
make bootstrap-bronze
make batch-bronze-docker BATCH_ID=import-2610 INPUT_DIR=/duong/dan/thu_muc
make verify-bronze       BATCH_ID=import-2610 INPUT_DIR=/duong/dan/thu_muc
make batch-silver-docker
make gold-docker
```

Mỗi lần nạp cần một `BATCH_ID` mới. Thư mục nguồn có một file cho mỗi bảng, ví dụ `patients.parquet`, `encounters.parquet`.

### c) Dữ liệu real-time (sự kiện trực tiếp)

```bash
make stream-bronze-docker                                                   # terminal 1: Kafka → Bronze
make produce KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RATE=100 DURATION=60    # terminal 2
```

- Sự kiện hợp lệ vào `iceberg.bronze.stream_events_raw`, sự kiện lỗi vào `stream_events_quarantine`.
- Hiện sự kiện real-time **chưa** được đưa lên Silver và Gold (việc của tuần 5).
- Kiểm tra tải: `make stream-gate` chạy 1000 event/s trong 10 phút, mất khoảng 12 phút, kết quả ghi ra `results/stream-gate.json`.

### Lưu ý cho Gold

- Gold chỉ tính tới mốc `GOLD_AS_OF` trong `.env` (mặc định `2026-09-01T00:00:00Z`, là ngày cuối của generator). Dữ liệu sau mốc này cần sửa lại `GOLD_AS_OF`.
- Có khoa ngoài 5 khoa chuẩn (`ICU`, `EMERGENCY`, `CARDIOLOGY`, `INTERNAL_MEDICINE`, `SURGERY`) thì phải thêm vào `GOLD_BED_CAPACITIES`, nếu không job Gold sẽ báo lỗi thiếu capacity.

## 6. Chạy cả kịch bản demo bằng một lệnh

```bash
PYTHON=.venv/bin/python scripts/checkpoint_demo.sh            # up → batch → query → test
STREAM=1 PYTHON=.venv/bin/python scripts/checkpoint_demo.sh   # thêm Kafka + producer
```

## 7. Test và slide

```bash
TZ=UTC JAVA_HOME=<java17> PYSPARK_PYTHON=.venv/bin/python make test PYTHON=.venv/bin/python
make slides      # results/slides/checkpoint-1.pdf (cần TeX Live có xelatex)
```
