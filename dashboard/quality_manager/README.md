# HospitalLake — Web quản lý chất lượng dữ liệu

Web tiếng Việt để xem kết quả bộ kiểm tra batch, tìm bản ghi lỗi, đối chiếu
trước/sau và lưu ghi chú rà soát. Đọc báo cáo thật từ `results/data-quality/`;
không dùng số liệu mẫu viết sẵn trong giao diện.

## Chạy

Từ thư mục gốc repo, với môi trường `.venv` đã có PySpark:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  make quality-web PYTHON=.venv/bin/python
```

Mở **http://127.0.0.1:8765**. Nếu `JAVA_HOME` đã trỏ tới Java phù hợp thì bỏ
phần đặt biến đó. Xem báo cáo đã có chỉ cần Python; nút chạy kiểm tra cần Java
và PySpark trong cùng môi trường Python khởi động web.

Đổi cổng hoặc nơi lưu kết quả:

```bash
make quality-web PYTHON=.venv/bin/python QUALITY_WEB_PORT=8766 \
  QUALITY_OUTPUT_DIR=results/data-quality
```

Không cần npm, frontend build, Docker hoặc thêm thư viện web. `Ctrl+C` dừng
server và tiến trình kiểm tra do server đang chạy. Web chỉ lắng nghe localhost.

## Có thể quản lý gì?

- **Tổng quan:** số dòng nguồn, Silver, cách ly và bản sao gộp; phân bố theo
  sáu bảng, lý do cách ly, kết quả Gold, số kịch bản đạt kỳ vọng.
- **Kịch bản & rà soát:** tìm bằng mã/mô tả/lý do lỗi; lọc theo bảng, kết quả,
  trạng thái rà soát. Xem các trường thay đổi và toàn bộ dữ liệu gốc.
- **Ghi chú:** chưa rà soát, đã rà soát hoặc cần kiểm tra thêm. Lưu qua tải lại
  trang và khởi động lại server. Ghi chú không sửa dữ liệu nguồn, không duyệt
  bản ghi cách ly vào Silver.
- **Lịch sử:** mở báo cáo cũ; lần chạy dở hoặc báo cáo hỏng không được gắn nhãn
  đạt; tải JSON của báo cáo đang chọn.
- **Chạy bộ kiểm tra:** thực thi `scripts.data_quality_demo` bằng Python đang
  chạy web. Tại một thời điểm chỉ có một job do tiến trình server quản lý.
  Xem nhật ký và tự mở kết quả mới sau khi job kết thúc thành công.

36/36 kịch bản đạt nghĩa là pipeline xử lý đúng kết quả mong đợi, bao gồm
những lỗi được cố ý cài vào dữ liệu. Nó không có nghĩa 100% dữ liệu đầu vào
là hợp lệ.

## Phạm vi và dữ liệu lưu

Đây là công cụ local dành cho demo chất lượng batch. Chưa quản lý dịch vụ
Kafka/Kubernetes, chưa thay Grafana, chưa chỉnh trực tiếp dữ liệu bệnh viện
hay tự giải quyết các vấn đề cập nhật bản ghi/hành trình bệnh nhân đã đề xuất.

| Đường dẫn | Nội dung |
|---|---|
| `results/data-quality/run-*/report.json` | Báo cáo tạo bằng CLI hiện có |
| `results/data-quality/_manager/reviews.json` | Ghi chú và trạng thái rà soát theo lần chạy/kịch bản |
| `results/data-quality/_manager/jobs/<id>/job.json` | Trạng thái job do web khởi chạy |
| `results/data-quality/_manager/jobs/<id>/output.log` | Nhật ký thực thi |
| `results/data-quality/_manager/jobs/<id>/output/run-*` | Báo cáo và Parquet của lần chạy từ web |

Các file này được Git bỏ qua qua quy tắc `results/`. Source Bronze/Silver/Gold
thật trên MinIO/Iceberg không bị sửa bởi web. Chạy một server cho một thư mục
kết quả; chưa hỗ trợ nhiều người sửa ghi chú đồng thời từ nhiều server.

Server dùng thư viện chuẩn Python, giới hạn truy cập localhost, kiểm tra
Host/Origin và token của phiên trước thao tác ghi/chạy job. Phù hợp cho quản
lý local; [Python không khuyến nghị `http.server` cho triển khai production](https://docs.python.org/3.12/library/http.server.html).

## Kiểm tra

```bash
.venv/bin/python -m pytest tests/unit/test_quality_manager.py -q
node --check dashboard/quality_manager/static/app.js
```

Kiểm thử trình duyệt tùy chọn dùng Playwright (đã chạy với 1.63.0). Cần server
đang mở và package `playwright` có trong môi trường Node kiểm thử:

```bash
node tests/e2e/quality_manager.browser.cjs
# Chạy cả Spark thật từ nút trên web:
RUN_QUALITY_JOB=1 node tests/e2e/quality_manager.browser.cjs
```

`QUALITY_WEB_URL` đổi URL kiểm thử; `BROWSER_EXECUTABLE` chỉ tới Chrome có sẵn
nếu chưa cài browser của Playwright. Screenshot nằm trong
`results/quality-web-qa/`. Kiểm tra bao gồm số liệu thật, bộ lọc, lưu/đọc lại ghi
chú, tải báo cáo, màn hình trống/lỗi và giao diện 360–1440 px. Lượt kiểm thử
ghi chú rồi khôi phục nội dung/trạng thái cũ; thời điểm lưu vẫn được cập nhật.

Đã kiểm tra ngày 15/11/2026: 12 test HTTP/lưu ghi chú/vòng đời job đạt;
kiểm tra trình duyệt đạt ở các kích thước trên, bao gồm trạng thái trống và lỗi.
Nút chạy trên web đã thực thi Spark thật, exit 0, đủ 36/36 kịch bản và Gold.
