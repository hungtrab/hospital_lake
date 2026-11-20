# Làm sạch dữ liệu HospitalLake

Mục tiêu: có dữ liệu bẩn cụ thể, giải thích được vì sao xử lý như vậy, chạy lại
được và chứng minh báo cáo cuối không đếm nhầm. Phạm vi lần này là **batch**:
Bronze → Silver → Gold. Dữ liệu hoàn toàn giả lập.

## 1. Ba quyết định khi gặp một bản ghi

- **Sửa chắc chắn:** ` ER ` → `EMERGENCY`, ` glucose ` → `Glucose`,
  `mg / dl` → `mg/dL`. Chỉ sửa cách viết đã có trong từ điển.
- **Giữ nguyên thông tin hợp lệ:** Glucose bằng 200 vẫn được giữ và đánh dấu
  bất thường. Chưa xuất viện thì `discharge_time = null` là đúng.
- **Cách ly để kiểm tra:** thiếu mã, hai bản ghi cùng mã nhưng mâu thuẫn,
  số đo không hợp lệ, sai liên kết hoặc đơn vị chưa hỗ trợ. Bảng
  `silver.quarantine` lưu bản ghi gốc và danh sách mã lỗi.

Bronze giữ dữ liệu nguồn. Silver không tự bịa thông tin còn thiếu. Gold chỉ
đọc các bản ghi Silver được chấp nhận. Một bản ghi bị cách ly chưa có nghĩa là
nó sai ngoài thực tế: có thể danh mục hoặc dữ liệu cha chưa được nạp đủ.

## 2. Các phương pháp đã xem xét

| Vấn đề | Các cách có thể làm | Quyết định trong dự án và lý do |
|---|---|---|
| Khác cách viết | Từ điển chính xác; so khớp gần đúng | Dùng từ điển. Không đoán tên khoa từ chuỗi gần giống vì có thể đổi sai ý nghĩa. |
| Thiếu dữ liệu | Bỏ dòng; điền mặc định/trung bình; giữ null; cách ly | Trường không bắt buộc giữ null; thiếu mã hoặc số đo bắt buộc thì cách ly. Điền trung bình sẽ tạo số đo không có thật. |
| Trùng dữ liệu | Gộp bản sao; chọn bản mới nhất; cách ly xung đột | Gộp khi mọi trường nguồn giống nhau. Nếu cùng mã nhưng khác thông tin thì cách ly cả nhóm; chưa có phiên bản nghiệp vụ để chọn bản đúng. |
| Sai đơn vị | Chỉ đổi tên; chuyển đổi cả giá trị và khoảng tham chiếu; cách ly | Chỉ chuẩn hóa cách viết tương đương. Không đổi `mmol/L` thành `mg/dL` bằng cách đổi nhãn. Chưa triển khai chuyển đổi số học. |
| Giá trị cực đoan | Cắt về ngưỡng; loại theo thống kê; kiểm tra tính hợp lệ | Giữ kết quả bất thường nhưng hợp lệ. NaN/vô cực và các giá trị vi phạm hợp đồng nguồn thì cách ly. |
| Sai quan hệ | Ghép gần đúng; tra mã chính xác | Join mã chính xác vào Silver cha; không ghép sang bệnh nhân khác. |
| Sai thời gian | Đảo hai mốc; tự điền giờ; kiểm tra thứ tự | Kiểm tra thứ tự và trạng thái, không đoán thời gian đúng. |

Các lựa chọn trên là chính sách của HospitalLake. Phần thực thi dùng API Spark
sẵn có, không thêm framework chất lượng dữ liệu:

- [Spark NULL semantics](https://spark.apache.org/docs/3.5.6/sql-ref-null-semantics.html):
  phép so sánh với null có thể cho null. Quy tắc kiểm tra phải xử lý trường hợp
  này rõ ràng; framework hiện có coi điều kiện null là không đạt.
- [Spark dropDuplicates](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html):
  có thể xét trùng theo một tập cột. HospitalLake không chỉ gộp theo ID vì như
  vậy không giải quyết được hai bản ghi có cùng ID nhưng thông tin khác nhau.
  Hàm hiện có nhóm theo toàn bộ cột nguồn trước, sau đó kiểm tra xung đột ID.
- [Spark data types](https://spark.apache.org/docs/3.5.6/sql-ref-datatypes.html):
  NaN/vô cực cần được xét riêng; timestamp có các ngữ nghĩa múi giờ khác nhau.
  Demo dùng UTC và số hữu hạn khi kiểm tra số đo.

## 3. Bộ kịch bản chạy được

Nguồn: [`data/samples/dirty_scenarios.json`](../data/samples/dirty_scenarios.json).
36 kịch bản, 39 dòng ở sáu bảng. Mỗi kịch bản có dữ liệu đầu vào và kết quả
mong đợi tách riêng. Nhãn kịch bản và kết quả mong đợi không đi vào pipeline.

| Nhóm | Ví dụ | Kết quả |
|---|---|---|
| Chữ hoa/thường và khoảng trắng | ` m `, ` Hanoi ` | `M`, `Hanoi` |
| Thiếu trường tùy chọn | Thành phố rỗng; đang nằm viện chưa xuất viện | Giữ null, vẫn nhận bản ghi |
| Thiếu trường bắt buộc | Thiếu `patient_id`, `admission_time` | `MISSING_*` |
| Sai miền giá trị | Năm sinh 2030 nhưng đăng ký 2026 | `IMPLAUSIBLE_BIRTH_YEAR` |
| Bản sao hoàn toàn | E1 được nhập hai lần từ hai batch | Silver chỉ có một E1; Bronze giữ cả hai |
| Cùng ID nhưng mâu thuẫn | Hai năm sinh; một kết quả bình thường và một NaN | Cách ly cả nhóm, `DUPLICATE_KEY` |
| Tên khoa hợp lệ | `ER`, `intensive-care_unit` | `EMERGENCY`, `ICU` |
| Tên khoa không rõ | `ER123`, `E/R` | `UNKNOWN_DEPARTMENT` |
| Sai liên kết | P404 không tồn tại; E1 thuộc P1 nhưng xét nghiệm ghi P2 | `UNKNOWN_PATIENT`, `ENCOUNTER_PATIENT_MISMATCH` |
| Lỗi lan từ bảng cha | Bệnh nhân/lượt khám cha đã bị cách ly | Con chưa được đưa vào Silver |
| Sai thời gian/trạng thái | Xuất viện trước nhập viện; ghi đã xuất viện nhưng không có ngày | Mã lỗi thời gian/trạng thái |
| Chuẩn hóa xét nghiệm | ` glu `, ` glucose `, ` mg / dl ` | `GLU`, `Glucose`, `mg/dL`; số đo giữ nguyên |
| Mã/tên xét nghiệm mâu thuẫn | Mã GLU, tên Sodium | `LAB_TEST_NAME_MISMATCH` |
| Đơn vị chưa hỗ trợ | Glucose `mmol/L`, hoặc `MG/dL` | `UNSUPPORTED_LAB_UNIT` |
| Cờ bất thường nhập sai | 90 trong khoảng 70–99 nhưng cờ true | Tính lại thành false |
| Bất thường hợp lệ | Glucose 200, khoảng 70–99 | Giữ 200, cờ true |
| Đúng biên | Glucose 99, khoảng 70–99 | Hợp lệ, cờ false |
| Số không hợp lệ | NaN, vô cực, liều âm, SpO2 150%, thiếu nhiệt độ | Cách ly; không ép về ngưỡng hoặc điền trung bình |
| Sự kiện giường sai | `occupied` không thuộc danh sách nguồn | `INVALID_BED_EVENT_TYPE` |

Các ngưỡng số đo là quy tắc kiểm tra dữ liệu giả lập đã có của dự án, không
phải quy tắc chẩn đoán. Xét nghiệm chỉ kiểm tra số hữu hạn và khoảng tham chiếu
hợp lệ, không áp một ngưỡng thống kê để xóa các kết quả cao/thấp.

## 4. Chạy và trình bày cho thầy

Cần môi trường Python của dự án có PySpark và Java phù hợp. Với môi trường
host PySpark 4.0.2 hiện có, dùng Java 17 trở lên. Trên máy Linux hiện tại:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  make quality-demo PYTHON=.venv/bin/python
```

Nếu `JAVA_HOME` đã đúng thì chỉ cần `make quality-demo PYTHON=.venv/bin/python`.
Runner tự dùng Python đang chạy cho Spark worker, tránh lẫn phiên bản Python.
Không cần bật Docker cho demo này.

Có thể dùng [web quản lý local](../dashboard/quality_manager/README.md) để xem
báo cáo, lọc lỗi, đối chiếu trước/sau, ghi chú rà soát và chạy lại bộ kiểm tra:
`make quality-web PYTHON=.venv/bin/python` (cùng yêu cầu Java khi chạy Spark).

Mỗi lần chạy tạo thư mục mới `results/data-quality/run-*/`, in đường dẫn cuối
lệnh, gồm:

- `report.md`: giải thích từng trường hợp bằng tiếng Việt, dữ liệu trước/sau.
- `report.json`: kết quả thực tế, số dòng, mã lỗi, Spark version và Gold.
- `bronze/`, `silver/`, `quarantine/`, `gold/`: Parquet để kiểm tra trực tiếp.

Demo gọi **chính** `transform_entity()` và `build_gold()` của pipeline. Nó
không viết vào các bảng Iceberg đang dùng và không thay dữ liệu `data/generated`.
Đây là kiểm tra tích hợp xử lý batch trên Parquet local, chưa phải bài kiểm tra
Kafka, MinIO, Trino hoặc toàn hệ thống.

Kết quả mong đợi, được runner kiểm tra và dừng với lỗi nếu không khớp:

| Bảng | Đầu vào | Silver | Cách ly | Bản sao gộp |
|---|---:|---:|---:|---:|
| patients | 6 | 2 | 4 | 0 |
| encounters | 10 | 2 | 7 | 1 |
| lab_results | 15 | 4 | 11 | 0 |
| medications | 3 | 1 | 2 | 0 |
| vitals | 3 | 1 | 2 | 0 |
| bed_events | 2 | 1 | 1 | 0 |
| Tổng | 39 | 11 | 27 | 1 |

**39 = 11 + 27 + 1**: không có dòng nào biến mất không rõ lý do. Một dòng có
thể có nhiều mã lỗi nên tổng số lần xuất hiện mã lỗi không bằng số dòng cách ly.
Demo còn kiểm tra schema Silver, dữ liệu nguồn không đổi và chạy lại với phân
vùng khác vẫn cho cùng kết quả.

Gold phải cho **2 lượt nhập viện**, **1 bệnh nhân còn nằm viện** ở thời điểm chốt;
Glucose có **4 kết quả hợp lệ**, **1 bất thường = 25%**. Bản sao E1 không tăng
số lượt nhập viện; `glucose` và `Glucose` không bị tách thành hai nhóm báo cáo.

Khi trình bày, mở `report.md`, chọn E1 (trùng), E_TYPO (không được đoán tên),
L_ALIAS (sửa chắc chắn), L_ABNORMAL (giữ bất thường) và L_CONFLICT (cách ly
mâu thuẫn), rồi cho xem tổng Gold. Có thể nói:

> Nhóm giữ nguyên dữ liệu gốc, chỉ sửa những lỗi có quy tắc chắc chắn. Những
> dòng chưa xác định được đúng sai được giữ riêng cùng lý do. Nhóm đã xây dựng
> 36 tình huống để kiểm tra việc làm sạch và tác động lên báo cáo cuối.

## 5. Thay đổi trong pipeline và giới hạn

Đã có trước lần này: bỏ bản sao hoàn toàn, cách ly xung đột ID, kiểm tra thiếu
trường, FK, thứ tự thời gian, số hữu hạn, ngưỡng vital và tính lại cờ xét nghiệm.

Bổ sung lần này:

1. Chuẩn hóa khoa chỉ đổi chữ hoa/thường và dấu phân cách cho phép. Không còn
   xóa mọi ký tự ngoài A–Z khiến `ER123` bị nhận nhầm là `ER`.
2. Kiểm tra và chuẩn hóa danh mục **7 xét nghiệm giả lập hiện có**. Mã mới,
   tên mâu thuẫn hoặc đơn vị ngoài hợp đồng sẽ bị cách ly. Danh mục trong
   `spark/common/normalization.py` có test đối chiếu với generator.
3. JSON của bản ghi cách ly giữ cả các trường null để dễ đối chiếu.
4. Bộ fixture, lệnh demo và kiểm thử tích hợp trước/sau/Gold.

Không đổi tên bảng hay schema Silver. Sau khi áp dụng code, chạy lại toàn bộ
Silver rồi Gold để báo cáo dùng cùng quy tắc:

```bash
make batch-silver-docker
make gold-docker
make query PYTHON=.venv/bin/python
```

Các lệnh trên cần stack local đã chạy và Bronze đã có dữ liệu. Số dòng Silver
có thể giảm nếu dữ liệu cũ chứa mã/đơn vị chưa hỗ trợ; bản ghi nguồn vẫn ở Bronze.
Không tự coi kết quả checkpoint cũ là kết quả mới sau khi đổi quy tắc.

Chưa triển khai: suy đoán/sửa lỗi chính tả gần đúng, chuyển đổi đơn vị số học,
điền thiếu số đo, phân giải cập nhật theo phiên bản nghiệp vụ, kiểm tra đầy đủ
chuỗi chuyển giường hoặc các kịch bản streaming. Timestamp nguồn hiện là
Parquet có kiểu sẵn; demo không tuyên bố sửa được chuỗi ngày tháng sai định dạng.

Kiểm thử:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest tests/unit/test_bronze_to_silver.py \
  tests/unit/test_silver_clinical.py tests/unit/test_data_quality.py \
  tests/unit/test_gold_metrics.py tests/integration/test_data_quality_demo.py -q
```

Đã kiểm tra ngày 15/11/2026: demo đạt 36/36 kịch bản, Gold và chạy lại trên
cả PySpark 4.0.2 (host, Java 17) và Spark 3.5.5 (image Docker của dự án).
Toàn bộ suite local đạt **165 passed, 10 skipped, 85 subtests passed**;
10 test tích hợp tùy chọn không chạy trong lượt này. Kết quả không chứng nhận
gate streaming hoặc các dịch vụ đang chạy thật.

## Kiểm tra trên bản đồ đang chạy

Trên luồng Kafka, bản tin hỏng cấu trúc đi vào `bronze.stream_events_quarantine` kèm mã lỗi. Luồng kiểm tra ID, khoa, giá trị xét nghiệm và sinh hiệu trước khi nhập sổ sự kiện Silver. Bản đồ đọc vị trí từ Silver và đối chiếu bản tin Bronze theo ID sự kiện, giờ, loại sự kiện, mã người bệnh và mã lượt khám. Kafka gửi lại cùng bản tin chỉ tạo một người trong phòng. Nếu các bản sao bất đồng về mã giường, bản đồ xếp người nằm viện vào khu chưa xác định. Bản ghi chuyển vị trí đến sau khi xuất viện không đưa người bệnh trở lại giường. Kiểm tra những trường hợp này bằng `python -m tests.integration.room_map_contract` trong container API có biến `TRINO_URL`.
