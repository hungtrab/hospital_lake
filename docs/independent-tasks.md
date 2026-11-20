# Ba công cụ độc lập — đã triển khai local ngày 19/11/2026

Theo yêu cầu mới, Hùng/Codex đã tự hoàn thành cả ba công cụ; **không còn giao
các task này cho Tín, Đức, Bảo**. Các mục dưới là hợp đồng input/output và cách
sử dụng, không phải phân công chờ bàn giao.

Chỉ cần Python 3.11+ và thư viện chuẩn; không cần Docker/Kafka/Spark/Trino/VPS.
Đã chạy 19 unit tests và CLI thật cho lịch khám, báo cáo tuần và đối chiếu
hành trình (PASS, FAIL, lỗi schema/JSON). Bằng chứng:
`results/independent-tasks-20261119/summary.json` và các file `.log` cùng thư mục.
Các CLI được kiểm tra với `python3 -S` để loại phụ thuộc thư viện cài thêm.

Phương pháp áp dụng: `/home/hungchan/shared/laura_methodology.md` — giữ hợp
đồng đầu ra, kiểm chứng cả trường hợp sai, lưu kết quả thực tế. Test/CLI kiểm
tra fixture; chưa chứng nhận Kafka/Spark/Trino hoặc deployment thật.

Hùng tiếp tục nhận đầu ra để nối scheduler, producer có lưu tiến độ, web/API và
VPS/Cloudflare. SQLite chỉ là công cụ kiểm tra SQL cục bộ, không thay Trino/Iceberg.

## 1. sinh lịch đến khám một ngày

### File bàn giao

- `generators/stream_generator/daily_schedule.py`
- `tests/unit/test_daily_schedule.py`

Module độc lập, chỉ import thư viện chuẩn. API bắt buộc:

```python
def build_schedule(config: dict) -> list[dict]: ...
```

CLI (chạy từ gốc repo):

```bash
python generators/stream_generator/daily_schedule.py --config input.json --output schedule.json
python -m unittest discover -s tests/unit -p 'test_daily_schedule.py' -v
```

### Input và expected output chính xác

`input.json`:

```json
{
  "date": "2026-11-23",
  "timezone": "Asia/Ho_Chi_Minh",
  "seed": 42,
  "visits": 2,
  "slots": [{"start": "08:00", "end": "08:01", "weight": 1}]
}
```

`schedule.json` phải chứa đúng các giá trị sau; không bắt buộc thứ tự key/indent:

```json
[
  {
    "visit_id": "VISIT-20261123-000001",
    "arrival_time": "2026-11-23T01:00:00Z"
  },
  {
    "visit_id": "VISIT-20261123-000002",
    "arrival_time": "2026-11-23T01:00:00Z"
  }
]
```

Quy tắc:

- `date` là ngày tại Việt Nam; phạm vi phiên bản này chỉ nhận
  `Asia/Ho_Chi_Minh` (UTC+07:00), dùng offset cố định, không cần gói timezone ngoài.
- Slot là khoảng `[start, end)`, độ phân giải một phút, giây bằng 00.
  `end` có thể là `24:00`; không nhận slot qua ngày hoặc chồng lấn nhau.
- Với mỗi lượt: chọn slot theo weight, chọn đều một phút trong slot bằng
  `random.Random(seed)`. Không dùng đồng hồ hiện tại hay random toàn cục.
- Sort thời điểm tăng dần, sau đó gán ID theo ngày và số thứ tự từ 1 như mẫu.
  `visit_id` là ID của lịch, chưa phải `patient_id` hay Kafka `event_id`.
  Hùng ánh xạ nó vào hành trình và giữ mapping khi retry; không sửa lịch đã phát.
- Nhiều lượt đến cùng phút là hợp lệ. Cùng toàn bộ input phải ra cùng output.
- `visits` là số nguyên không âm (không nhận boolean); 0 lượt trả `[]`.
  `seed` là số nguyên; weights hữu hạn, dương; danh sách slots không rỗng.
- Sai ngày, giờ, timezone, số lượt hoặc weight: API raise `ValueError`;
  CLI exit 2, ghi giải thích ra stderr, không để lại output dở dang.

### Tests bắt buộc

1. Fixture trên ra đúng JSON, không chỉ kiểm tra số dòng.
2. 100 lượt ra đúng 100 ID riêng biệt, được sort, nằm trong slot/ngày đã chọn.
3. Chạy hai lần cùng input ra cùng kết quả; không sửa config đầu vào.
4. Đổi ngày không trùng ID; 0 lượt ra `[]`.
5. Slot `00:00–00:01` ngày 23/11 ra `2026-11-22T17:00:00Z`.
6. Các input không hợp lệ ở trên phải fail rõ. Không test random bằng giả định
   “seed khác luôn phải ra output khác” hoặc ngưỡng xác suất dễ chập chờn.

Hùng phụ thuộc output này để phát sự kiện đúng giờ. Công cụ này không làm Kafka,
patient lifecycle, scheduler, lưu checkpoint hoặc deploy.

## 2. bộ kịch bản qua đêm và công cụ đối chiếu

### File bàn giao

- `data/samples/overnight_journey.json`: fixture với event envelope hiện có.
- `scripts/verify_patient_journey.py`: đọc expected/actual JSON và so sánh.
- `tests/unit/test_verify_patient_journey.py`: unittest không dùng Spark.

Công cụ không viết lại stream processor; nó nhận kết quả do hệ thống xuất ra.
Adapter phát fixture và thu kết quả thật vẫn là phần tích hợp tiếp theo. Unit
tests dùng actual đúng/sai cố định trong thư mục tạm, độc lập với adapter đó.

### Fixture và timeline bắt buộc

Patient `P-DEMO-001`, encounter `E-DEMO-001`; mọi event có `source` là
`hospital-simulator`, `version` là 1. Ví dụ event đầu tiên:

```json
{
  "event_id": "overnight-a1",
  "event_type": "ADMISSION",
  "event_time": "2026-11-23T16:55:00Z",
  "source": "hospital-simulator",
  "version": 1,
  "payload": {
    "patient_id": "P-DEMO-001",
    "encounter_id": "E-DEMO-001",
    "department": "ICU",
    "encounter_type": "icu"
  }
}
```

Fixture có `scenario_id: "overnight-v1"` và `steps` theo đúng thứ tự dưới đây.
Mỗi step có `step_id`, `as_of` UTC, `events` (envelopes) và `expected`.
Tên step/giá trị phải giống bảng; step không phát event dùng `events: []`.

| step_id | as_of (UTC) | events đưa vào | current_patients | ICU | CARDIOLOGY |
|---|---|---|---:|---:|---:|
| admission | 2026-11-23T16:55:00Z | Event nhập viện mẫu ở trên | 1 | 1 | 0 |
| midnight | 2026-11-23T17:05:00Z | Không có; lúc này là 00:05 ngày 24/11 tại Việt Nam | 1 | 1 | 0 |
| transfer | 2026-11-23T17:10:00Z | `overnight-t1`, TRANSFER, event_time bằng as_of | 1 | 0 | 1 |
| late_replay | 2026-11-23T17:15:00Z | Gửi lại nguyên event `overnight-a1`, giữ ID/time/payload cũ | 1 | 0 | 1 |
| discharge | 2026-11-24T02:00:00Z | `overnight-d1`, DISCHARGE, event_time bằng as_of | 0 | 0 | 0 |
| discharge_replay | 2026-11-24T02:01:00Z | Gửi lại nguyên `overnight-d1` | 0 | 0 | 0 |

Payload transfer giữ patient/encounter, `department: "CARDIOLOGY"`,
`from_bed_id: "ICU-DEMO-1"`, `to_bed_id: "CARDIOLOGY-DEMO-1"`.
Payload discharge giữ patient/encounter và `department: "CARDIOLOGY"`.

`expected` của bước admission:

```json
{"current_patients": 1, "by_department": {"ICU": 1, "CARDIOLOGY": 0}}
```

### Input công cụ so sánh

`actual.json` do hệ thống/fixture test cung cấp:

```json
{
  "scenario_id": "overnight-v1",
  "steps": [
    {"step_id": "admission", "current_patients": 1, "by_department": {"ICU": 1, "CARDIOLOGY": 0}},
    {"step_id": "midnight", "current_patients": 1, "by_department": {"ICU": 1, "CARDIOLOGY": 0}},
    {"step_id": "transfer", "current_patients": 1, "by_department": {"ICU": 0, "CARDIOLOGY": 1}},
    {"step_id": "late_replay", "current_patients": 1, "by_department": {"ICU": 0, "CARDIOLOGY": 1}},
    {"step_id": "discharge", "current_patients": 0, "by_department": {"ICU": 0, "CARDIOLOGY": 0}},
    {"step_id": "discharge_replay", "current_patients": 0, "by_department": {"ICU": 0, "CARDIOLOGY": 0}}
  ]
}
```

Đây là số liệu của fixture chạy từ baseline rỗng trong môi trường riêng, không
phải so tổng bệnh viện đang có người bệnh khác với số 1. Adapter của Hùng phải
isolate dữ liệu; không reset dữ liệu vận hành để test.

### Lệnh và expected output

```bash
python scripts/verify_patient_journey.py --expected data/samples/overnight_journey.json --actual actual.json
python -m unittest discover -s tests/unit -p 'test_verify_patient_journey.py' -v
```

- File đúng: exit 0; stdout JSON `{"status":"PASS","checked_steps":6}`.
- Đổi `midnight.current_patients` thành 0: exit 1; stdout JSON có
  `status: "FAIL"`, `mismatches` chứa
  `{"step_id":"midnight","field":"current_patients","expected":1,"actual":0}`.
- Sai JSON/schema, thiếu/thừa/trùng step, sai scenario_id, count âm hoặc count
  không phải integer (bao gồm boolean): exit 2, giải thích ra stderr.
- Match step bằng ID, không theo thứ tự array. Mỗi step phải có đúng hai khoa
  trong contract; không tự coi trường bị thiếu là 0. Không tự sinh actual từ
  expected trong chế độ CLI. Fixture expected là giá trị cố định đã duyệt ở đây.
- Unit tests phải có PASS, count sai, thiếu step, trùng step, JSON hỏng,
  reordered steps và count sai kiểu. Kiểm tra actual sai thực sự trả exit khác 0.

PASS của công cụ chỉ chứng nhận file actual khớp expected; chỉ khi Hùng nối
actual từ pipeline thật thì mới là kết quả kiểm thử tích hợp.

## 3. SQL so sánh hai tuần, chạy test không cần Trino

### File bàn giao

- `sql/trino/department_week_comparison.sql`
- `scripts/department_week_report.py`: chạy cùng file SQL bằng SQLite in-memory.
- `tests/unit/test_department_week_report.py`

Chỉ dùng `sqlite3`, `csv`, `json`, `datetime`, `unittest`, v.v. của Python.
SQL dùng phần chung: SELECT/CTE, SUM, CASE, CAST, ROUND, GROUP BY, ORDER BY;
không dùng hàm ngày riêng của SQLite, PRAGMA trong SQL hoặc SQL giả lập bằng Python.

### Hai relation đầu vào cố định

`department_daily_metrics(date, department, admissions)`:
`date` là chuỗi `YYYY-MM-DD`, department là tên chuẩn, admissions là integer
không âm. Mỗi cặp date/department chỉ có một dòng; runner phải từ chối duplicate.
Tên/ý nghĩa cột lấy từ Gold hiện có, không đổi production schema.

`report_period(previous_start, current_start, current_end)` có đúng một dòng.
Runner nhận `--as-of YYYY-MM-DD`, tính bằng Python:

- `current_end = as_of`, exclusive (không lấy ngày đang dở).
- `current_start = as_of - 7 ngày`.
- `previous_start = as_of - 14 ngày`.

Ngày là ngày của Gold đầu vào; không tự chuyển UTC thành ngày Việt Nam trong
query. Hùng chịu trách nhiệm chuẩn hóa/ghi rõ ngày nghiệp vụ khi nối pipeline.

### Fixture CSV và expected output

`input.csv`:

```csv
date,department,admissions
2026-11-08,ICU,999
2026-11-09,ICU,2
2026-11-15,ICU,2
2026-11-16,ICU,3
2026-11-22,ICU,3
2026-11-16,CARDIOLOGY,3
2026-11-10,EMERGENCY,2
2026-11-20,SURGERY,0
2026-11-23,ICU,999
```

```bash
python scripts/department_week_report.py --input input.csv --as-of 2026-11-23 --output report.json
python -m unittest discover -s tests/unit -p 'test_department_week_report.py' -v
```

`report.json` (so giá trị, không so whitespace; sort department tăng dần):

```json
[
  {"department":"CARDIOLOGY","previous_admissions":0,"current_admissions":3,"delta":3,"change_pct":null},
  {"department":"EMERGENCY","previous_admissions":2,"current_admissions":0,"delta":-2,"change_pct":-100.0},
  {"department":"ICU","previous_admissions":4,"current_admissions":6,"delta":2,"change_pct":50.0},
  {"department":"SURGERY","previous_admissions":0,"current_admissions":0,"delta":0,"change_pct":null}
]
```

Quy tắc: lấy khoa có ít nhất một dòng trong `[previous_start, current_end)`;
ngày thiếu tính 0, khoa chỉ xuất hiện ngoài khoảng không được đưa vào kết quả.
`delta = current - previous`; `change_pct = 100.0 * delta / previous`, làm tròn
2 chữ số; previous bằng 0 thì null. Không dùng COUNT(*) thay SUM(admissions).

Tests bắt buộc: fixture trên; input rỗng → `[]`; hai đầu kỳ; khoa chỉ có kỳ cũ;
mẫu số 0; 3→4 ra 33.33%; duplicate key/ngày sai/count âm phải bị từ chối.
CLI thành công exit 0; input lỗi exit 2 và stderr có lý do; không để output dở dang.
Runner phải nạp input vào SQLite rồi thực thi **đúng file SQL bàn giao**.
Không maintain hai thuật toán riêng cho test và production.

Hùng chịu trách nhiệm adapter Trino: cung cấp hai relation cùng contract bằng
view/CTE (cast Gold date thành ISO string), giữ logic query và kiểm tra lại trên
Trino thật. SQLite PASS chưa chứng nhận cú pháp/kết quả Trino.

## Kiểm chứng và phần còn lại

Chạy ba lệnh unittest đã ghi ở từng mục. Các test CLI còn chạy từ thư mục tạm
ngoài repo, tắt site-packages và kiểm tra exit code/output. Khi input hỏng,
các CLI ghi file giữ nguyên output cũ, không để lại file tạm dở dang.

Bằng chứng local ngày 19/11:

- Lịch mẫu: 2 lượt, cùng 08:00 giờ Việt Nam = 01:00 UTC, ID đúng mẫu.
- Báo cáo mẫu: ICU 4 → 6, delta 2, change_pct 50.0; khoa kỳ trước bằng 0 ra null.
- Hành trình mẫu: 6 bước khớp → exit 0; cố ý sửa số người qua đêm thành 0 →
  exit 1 với trường sai; JSON hỏng → exit 2. Actual ở đây là fixture thử công cụ,
  **không phải quan sát từ bệnh viện giả lập đang chạy**.
- Unit tests: lịch 7, đối chiếu 6, báo cáo 6, tổng 19 test đạt.
- Audit độc lập `gpt-6.1-sol / high`: PASS cho snapshot 8 file code/test/fixture;
  hash và kiểm tra bổ sung ở `results/independent-tasks-20261119/audit.md`.
  Đã sửa phép chia DOUBLE cho SQL và kiểm tra timestamp UTC sai định dạng.
  Đây là audit local chưa commit, không phải chứng nhận merge hoặc gate toàn hệ thống.

Còn lại: adapter từ schedule sang sự kiện thật, xuất actual từ pipeline,
chạy SQL trên Trino thật, web/VPS và các gate toàn hệ thống. Mục tiêu vận hành
23/11 vẫn phụ thuộc các gate đó. Chưa commit/push, deploy hay bật lịch tự động.
