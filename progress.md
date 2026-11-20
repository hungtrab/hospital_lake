# HospitalLake: tiến độ dự án

## 20/11 — biểu đồ theo giờ, thời gian chờ và nhóm người bệnh

Dashboard có bốn biểu đồ cho ngày hiện tại theo giờ Hà Nội. Trino đếm sự kiện Silver, lấy tuổi, nhóm bệnh và thời gian chờ từ bản tin Bronze tương ứng. Phát lại bản tin giữ nguyên số đếm. Thiếu trường hoặc có nhiều giá trị mâu thuẫn được hiển thị riêng. Cùng yêu cầu đọc tổng quan của Dashboard lấy thêm bản số liệu biểu đồ được lưu đệm 90 giây.

Lúc 10:13 giờ Hà Nội ghi nhận 3.311 lượt đăng ký, 354 lượt nhập viện, 2.855 lượt đã bắt đầu khám và 263 lượt chờ từ 60 phút. Các số này thuộc mô phỏng trên VPS. Kiểm tra trên Trino gồm ranh giới ngày, bản sao, trường thiếu và biên nhóm tuổi/thời gian. Trình duyệt đã đối chiếu các tổng và kiểm tra giao diện trên Cloudflare ở 390 và 1.440 px. Bằng chứng ở `results/visit-charts/`. [Đối chiếu với quy trình bệnh viện](docs/hospital-profile.md#hospitallake-khác-hệ-thống-bệnh-viện-thật-ở-đâu)

## 19/11 — Dashboard vận hành và theo dõi lượt khám

Tab **Dashboard** đọc lượt đến, nhập/xuất viện, số người nằm viện và trạng thái xử lý qua các API hiện có. Biểu đồ xem 7 hoặc 14 ngày theo giờ Hà Nội. Có thể mở một lượt từ danh sách cập nhật gần đây hoặc từ hộp hành trình ở bản đồ, màn hình nhập tay và trang người bệnh. Dashboard lưu mã lượt trên trình duyệt và đọc sự kiện mới mỗi 30 giây khi tab hiển thị.

Đã kiểm tra bản Cloudflare bằng trình duyệt ở chiều rộng 1.440 và 390 px, đối chiếu các chỉ số với API, mở và theo dõi lượt E-BM-20261119-W4975 sau khi tải lại. Tab chỉ gửi GET. Dịch vụ mô phỏng trên VPS tiếp tục chạy trong lúc phát hành. Bằng chứng ở `results/dashboard/`.

## 19/11 — nhập viện bằng tay và kịch bản cuối tuần đông

Trên website của nhóm, **Nhập tay** tiếp nhận ca giả lập, chuyển khoa và trả giường khi xuất viện. Qua trình duyệt, ca 68 tuổi thuộc nhóm tim mạch xuất hiện tại giường EMERGENCY-B260, chuyển sang CARDIOLOGY-B620 rồi rời bản đồ. Hành trình Silver có đủ sáu sự kiện. Sau cập nhật đã đối chiếu 201 bản tin có từ trước với bản sao trên VPS, giữ nguyên nội dung và các xác nhận Kafka đã có.

Hồ sơ Bạch Mai hiện mô phỏng cuối tuần đông theo yêu cầu demo, giờ đến theo luồng cấp cứu/ngoại trú, tuổi, nhóm bệnh và một số mã ICD-10 theo độ tuổi. Diễn tập ngoại tuyến 28 ngày sinh 148.585 lượt, gồm 20.081 lượt nhập nội trú. Trung bình 4.882 lượt đến mỗi ngày thường và 6.367 lượt mỗi ngày cuối tuần. Trên website đã đọc được lượt cấp cứu tự phát E-BM-20261119-W4971 lúc 22:42:56 giờ Việt Nam, 38 tuổi, nhóm thần kinh. Chạy `python scripts/profile_live_visits.py` để tái hiện. Bằng chứng vận hành và kiểm thử ở `results/manual/`. [Số liệu tham khảo và giả định](docs/hospital-profile.md)

## 19/11 — tìm kiếm và lọc trên website

Danh sách người bệnh tìm trên toàn bộ lịch sử theo mã, khoa, trạng thái, kết quả xét nghiệm và ngày cập nhật theo giờ Việt Nam. Bản đồ tìm không dấu theo phòng, giường, người bệnh hoặc bác sĩ và kết hợp với khu, tình trạng phòng. Đã kiểm tra chuyển trang cùng snapshot Iceberg, tìm một lượt cũ trong 130 lượt thử, mở phòng từ mã lượt khám và kết quả xét nghiệm bất thường trên website. Bằng chứng ở `results/search/`.

## 19/11 — Bạch Mai và bản đồ phòng đang hoạt động

VPS đã chuyển sang `bach-mai-v1`, dùng mốc 5.315 lượt/ngày và giữ các sự kiện của các lượt khám đang dở. Bản đồ Cloudflare chia phòng theo khu và tầng. Số người được truy vấn qua Trino từ sổ sự kiện và dữ liệu Bronze. Đối chiếu trên VPS cho kết quả 11 người nội trú trên bản đồ và 11 người trong snapshot ClickHouse. Các ca cũ thiếu mã phòng được liệt kê riêng. Trên website công khai, đã bấm vào phòng ED-6 và mở được hành trình đang khám của lượt E-BM-20261119-5986. Bộ kiểm tra Trino xác nhận trường hợp nhận/trả giường, chuyển phòng, hết giờ khám, xuất viện và phát lại bản tin. Browser công khai đạt ở desktop và điện thoại.

Diễn tập 31 ngày với 161.978 lượt và 22.028 ca nội trú, không có tài nguyên trùng lịch. Spark chạy lại đạt 36/36 kịch bản bẩn, gồm 39 dòng nguồn, 11 dòng Silver, 27 dòng cách ly và một bản sao gộp. Chạy lại Gold cho cùng kết quả. Đã kiểm tra bảng cách ly Bronze trên VPS có một bản tin sai JSON. Bằng chứng ở `results/bach-mai/`.

## 19/11 — chuyển mô phỏng sang Bệnh viện Đa khoa Đông Anh, Hà Nội

VPS đang dùng `dong-anh-v1`, trung bình 724 lượt/ngày theo số liệu năm 2024. Bản sao trước khi chuyển có 77 sự kiện, trong đó 53 sự kiện đã được Kafka xác nhận. Đã đối chiếu lại nội dung, ID và toàn bộ xác nhận đã có. Các lượt mới có mã DA, lịch ngày 20/11 có 834 lượt. Nguồn và giả định ở [hồ sơ mô phỏng](docs/hospital-profile.md).

Lượt Đông Anh đầu tiên đăng ký lúc 20:17:44 ngày 19/11. Đã đối chiếu partition và offset từ Kafka với Bronze, tìm thấy đăng ký và đo sinh hiệu trên API hành trình. Timestamp Kafka cách giờ đến 924 mili giây.

Diễn tập 31 ngày với 21.857 lượt, 2.563 ca nội trú. Thời gian nằm viện trung bình 6,01 ngày. Không trùng lịch phòng, nhân viên hay giường. Dashboard Cloudflare đã kiểm tra trên máy tính, máy tính bảng và điện thoại. Bằng chứng ở `results/dong-anh/`.

## 19/11 buổi tối — mô phỏng theo Bệnh viện Đa khoa tỉnh Lai Châu

VPS `homecoming` đã bật hồ sơ `lai-chau-v1`. Mô hình dùng Poisson theo giờ Việt Nam và lấy mốc 366 lượt/ngày. Lịch ngày 20/11 có 391 lượt, giờ đông nhất có 50 lượt. Lịch cũ đã được chuyển đổi sau khi sao lưu. Đã đối chiếu sự kiện, nội dung và xác nhận Kafka của toàn bộ hành trình đã bắt đầu với bản sao. Website Cloudflare phiên bản `d5db812e-2469-429a-a023-1f57f6b99b02` đã qua kiểm thử đăng nhập, API, hành trình thật và giao diện ở ba kích thước màn hình.

Hai lượt Poisson đầu trên VPS được ghi trong Bronze tại Kafka offset tương ứng. Lượt 19:27:20 và 19:30:10 có chênh lệch giữa giờ đến và timestamp Kafka là 0,248 giây và 0,404 giây. Lượt đầu vào phòng ED-1 lúc 19:29:38, sau 138 giây chờ. API truy vấn Silver và Bronze trả đúng mã điều dưỡng và bác sĩ ca trực. Bằng chứng ở `results/poisson/live-verification.json`.

Diễn tập 31 ngày bằng đồng hồ mô phỏng sinh 11.128 lượt, 2.486 ca nội trú, 235–429 lượt/ngày. Thời gian nằm viện trung bình 5,33 ngày. Thời gian chờ khám trung vị 4,75 phút, phân vị 95 là 52,89 phút. Không có phòng, bác sĩ, điều dưỡng hoặc giường nào phục vụ hai người trùng giờ. Đây là số đo từ kịch bản giả lập. Nguồn hiệu chỉnh và giả định được ghi trong [hồ sơ bệnh viện](docs/hospital-profile.md). Bằng chứng vận hành và kiểm thử ở `results/poisson/`.

## 19/11 lúc 18:12 (giờ Việt Nam) — chạy thử trực tiếp trên VPS

Mô phỏng đang bật. Lịch chạy thử đã lưu vào outbox trên VPS: 12 lượt tối
19/11, 100 lượt ngày 20/11. Lịch chính 100 lượt/ngày từ 21/11 tiếp tục
tự động. Chỉ ca PILOT-20261119-001 được rút ngắn còn 95 giây để kiểm tra
đến bước xuất viện ngay trong buổi chạy thử.

Hai ca đầu đã nhập viện. Ca rút ngắn đã xuất viện, ca còn lại đang điều trị.
10 sự kiện được Kafka xác nhận đã khớp từng offset và nội dung Bronze, đủ
10 ID ở Silver. Trino và ClickHouse cùng đếm 1 người đang nằm viện. Gold cả
ngày có 3 nhập/2 xuất, gồm một lượt kiểm thử trước đó. Đã kiểm tra dashboard
có đăng nhập trên desktop/mobile và cập nhật nhãn chạy thử. Bằng chứng ở
`results/pilot-20261119/`. Các mốc “tạm dừng” bên dưới là trạng thái trước đó.


## Phát hành dashboard có đăng nhập sau khi kiểm chứng Kubernetes (19/11/2026)

Website Cloudflare yêu cầu tài khoản admin để xem dashboard và dùng API.
Đã phát hành phiên bản `84649fea-f0c2-4036-8402-a21bfbb9861a` sau các kiểm tra bên dưới.
Kiểm tra trên URL thật: thiếu/sai mật khẩu trả HTTP 401, đúng tài khoản xem
được báo cáo và điều khiển mô phỏng. Browser desktop/mobile đạt. Lịch trên
VPS vẫn tạm dừng, 100 lượt/ngày từ 21/11. Bằng chứng ở
`results/final-gate/account-check.json` và `results/final-gate/public/website-check.json`.

Cụm kind mới `hospital-lake-final` dùng volume trống. Spark Operator và Strimzi
được cài lại từ Helm. Batch 100 bệnh nhân, 3.315 dòng, hoàn tất trong 140,33
giây. Trino đọc 100 bệnh nhân Bronze, 100 Silver và 686 nhóm khoa/ngày Gold.
Kịch bản nhập viện → xuất viện đã qua Kafka, Spark, ClickHouse và Grafana. Bốn
nguồn Prometheus trả UP. Bằng chứng nằm ở `results/final-gate/`.

Với 2 executor, tăng tải từ 3 lên 30 sự kiện/giây. Giữ tải đó khi chuyển sang
4 executor (mất 16,38 giây để operator báo sẵn sàng). Trong ba mẫu tải cao,
tốc độ xử lý dao động 6,6–186,9 dòng/giây với 2 executor và 7,8–154,8 với 4.
Thử nghiệm một máy cho thấy khả năng đổi số executor và bắt kịp backlog. Các
mẫu ngắn và khoảng dao động chồng nhau nên chưa kết luận có tăng tốc ổn định.

Đã ngắt một executor khi Kafka tiếp tục nhận sự kiện. Executor thay thế chạy,
Bronze/quarantine chứa đủ 7.179 offset mới, mỗi offset đúng một lần. Checkpoint
serving bắt kịp cả ba partition. Trino tính lại 417 người đang nằm viện, 37 ICU,
khớp ClickHouse. Xem `recovery.json`, `census.json`, `fresh-smoke.log` và
`fresh-history.json`. Kết quả thuộc cụm thử nghiệm một node. Dashboard VPS
dùng hệ thống Compose đã kiểm chứng riêng.


## Cập nhật 19/11/2026 — VPS và website đã chạy thật

Website: https://hospital-lake-group22.x7f3k9m2.workers.dev
Backend chạy trên VPS `homecoming`, project Compose `hospital-live`.
Cloudflare Worker gọi API qua VPC service/Tunnel riêng; database chỉ mở loopback.

- Đã tích hợp helper clinical metrics/ClickHouse của TV2–TV3 và manifest quan sát
  của TV4; luồng live giữ một writer `realtime_aggregates.py`, không chạy hai sink.
- Mô phỏng 100 lượt/ngày từ **21/11**, hiện **tạm dừng**. Đăng ký → nhập viện →
  xét nghiệm/chỉ số → chuyển khoa → xuất viện; có nằm qua đêm. Outbox và trạng
  thái bật/tắt được lưu bền vững; ID ổn định để xử lý gửi lại.
- Gate VPS thật **PASS trong 89,09 giây**, có restart Spark: số đếm 0→1→0,
  Grafana đọc đúng, Bronze giữ nguyên bytes/offset, Silver loại trùng/trễ và tính
  lại xét nghiệm bất thường; dữ liệu JSON lỗi vào quarantine.
- Spark Gold chạy lần đầu và chạy lại đều thành công: 1 dòng ICU, 1 nhập/1 xuất;
  Trino xác nhận không nhân đôi. Job tự tính lại mỗi giờ.
- Browser kiểm đủ 5 API + timeline, khóa thao tác, desktop/tablet/mobile: PASS.
  Bật thử trước ngày bắt đầu rồi restart API: trạng thái được giữ, 0 sự kiện được
  gửi sớm; đã tắt lại. Các dữ liệu ngày 19/11 trên web là lượt kiểm thử.
- 47 tests +72 subtests cho phần tích hợp, 20 tests +6 subtests pipeline, 5 Worker
  tests đạt. Sau sửa biên lịch và API, 7 tests liên quan chạy lại đều đạt.
- Evidence: `results/live-deploy/` (`live-gate.json`, `website-check.json`,
  `control-check.json`, `gold-check.json` và log). Hướng dẫn: `docs/live-deployment.md`.

Audit độc lập `gpt-6.1-sol/high`: **PASS** trong phạm vi VPS/Cloudflare;
report và hash ở `results/live-deploy/audit.md`.

**Còn thiếu cho toàn bộ tiêu chí môn học:** gate full Kubernetes, scale executor
và phục hồi có tải trên Kubernetes. Kết quả Compose/VPS không thay thế các gate
đó. Mô phỏng mới được kiểm chứng bằng lượt thử; chưa có số liệu chạy liên tục
nhiều ngày. Không commit/push; các phần việc dưới đây là lịch sử trước tích hợp.

## Cập nhật 19/11/2026 — ba công cụ độc lập

Theo yêu cầu mới, Hùng/Codex tự làm ba task nhỏ, không giao tiếp cho TV2–TV4.
Đã có lịch đến khám theo ngày/seed, fixture và comparator hành trình qua đêm,
SQL báo cáo hai tuần cùng runner SQLite cục bộ. **19 unit tests đạt**; CLI thật
kiểm tra output mẫu, mismatch và input lỗi, chỉ cần thư viện chuẩn Python.
Chi tiết/lệnh: [docs/independent-tasks.md](docs/independent-tasks.md).
Evidence: `results/independent-tasks-20261119/summary.json` và log/output.
Audit độc lập `gpt-6.1-sol/high`: PASS cho snapshot local 8 file, hash/report
ở `results/independent-tasks-20261119/audit.md`; chưa chứng nhận gate tích hợp.

Đây là kiểm chứng công cụ độc lập: chưa nối lịch vào Kafka, chưa thu actual từ
Spark hay chạy query trên Trino, chưa deploy/bật scheduler. Còn cần hòa giải
code mới ở `origin/main` với local trước tích hợp. Không commit/push trong phiên.


## Cập nhật 16/11/2026

**Project chưa hoàn tất toàn bộ:** batch và realtime local đã chạy thật;
Kubernetes đầy đủ, scale executor và demo triển khai mới vẫn còn phải kiểm chứng.

HospitalLake dùng dữ liệu bệnh viện **tổng hợp**: sáu thực thể batch dạng Parquet
(patients, encounters, lab_results, medications, vitals, bed_events) và các sự
kiện JSON đi qua Kafka. Kiến trúc giữ đúng hai đường:

- Batch: Parquet → Spark → Bronze/Silver/Gold trên MinIO + Iceberg → Trino.
- Realtime: Kafka → Spark Structured Streaming → Iceberg + aggregate ClickHouse
  → Grafana. Prometheus theo dõi tiến độ và backlog thật.

### Đã kiểm chứng

- Image Spark chạy đủ batch với **1.000 bệnh nhân / 36.281 dòng**; cả 11 query
  lịch sử có dữ liệu. Chạy lại cùng batch không ghi trùng Bronze; Silver/Gold
  tính lại thành công. Log: `results/completion/batch-image-*.log`.
- Gate realtime: nhập viện tăng N→N+1, xét nghiệm bất thường, dữ liệu trùng/trễ/sai,
  tám panel Grafana, restart rồi xuất viện về N. Có đối chiếu raw bytes/offset.
- **100 sự kiện/giây trong 30 phút: PASS**, đủ **180.000 message** vào Bronze một
  lần, cả Bronze và serving đọc tới offset cuối. Restart giữa bài phục hồi;
  không OOM/restart ngoài kế hoạch. Evidence: `results/completion/stress-light-probes-100/`.
- Gom file trên bảng stream thật: **1.821 → 1 file**, giữ **452.410 dòng** và
  checksum; Trino kiểm lại đủ **455.829 offset** tích lũy tại thời điểm đó.
  Report: `results/completion/compaction-live.json`, `compaction-offsets.json`.
- Full suite: **226 passed, 12 skipped, 88 subtests**. Các bản sửa probe/đo thời
  gian sau đó có regression riêng. Test opt-in chưa chạy không được tính PASS.
- Audit độc lập bằng **gpt-6.1-sol / high** đã duyệt các phần code local/P2 trong
  phạm vi ghi ở `results/completion/audit-*.md`; không thay thế gate K8s chưa chạy.

Các lần tải trước bị lỗi đều giữ nguyên FAIL. Chi tiết lỗi, số đo và giới hạn
của phép đo nằm trong [docs/benchmark.md](docs/benchmark.md), tránh nhầm số đo cũ
với kết quả cuối. Web quality manager, các kịch bản làm sạch, slide Group 22 và
script thuyết trình được giữ nguyên.

### Còn thiếu / bị chặn

- Có manifest, image, lệnh triển khai và audit code readiness cho Kubernetes;
  13 manifest qua server-side dry-run. Operators, MinIO và REST đã chạy thử.
  **Chưa chạy đủ core, SparkApplication và E2E trên K8s.**
- Cluster `hospital-lake-lab` hiện dừng để nhường RAM; kubeconfig riêng
  `results/k8s/kubeconfig`, dữ liệu/PVC vẫn giữ. Đây là profile một node dùng RWO.
- Ổ đĩa còn khoảng **5 GiB**, chưa đủ mức dự phòng 10 GiB để nạp stack. Đang
  chờ người dùng xác nhận dọn Docker build cache không dùng (cache chung nhiều
  project); chưa xóa cache, image, container hay volume để lấy chỗ.
- Sau khi đủ dung lượng: chạy K8s core → scale/fault recovery có tải → benchmark
  batch theo size/executor → fresh deploy và demo cuối.

Lệnh sử dụng: [realtime](docs/realtime.md), [Kubernetes](docs/kubernetes.md).
Git vẫn ở `main` / `503fbfd2`; **chưa commit/push thêm** theo yêu cầu. Backup đầu
đợt làm việc: `results/completion/before.zip`.

**Phần dưới là báo cáo lịch sử ngày 15/11, có mục đã lỗi thời.** Dùng cập nhật
ngày 16/11 phía trên cho trạng thái Grafana và PR, không lặp lại việc đã merge.

> Cập nhật ngày **15/11/2026**, đối chiếu với `origin/main` = `4b1638b` (merge PR #27) và các PR đang mở.
> Số đo lấy từ những lần chạy thật (chủ yếu ngày 25/10), chi tiết trong `docs/demo-script.md` và `docs/architecture.md`.
> Kế hoạch gốc theo tuần nằm trong `works.md`.

## 1. Tóm tắt

| Tuần | Chủ đề | Gate tích hợp (`works.md`) | Trạng thái |
|---|---|---|---|
| 1 | Lakehouse bootstrap | Spark → Iceberg → Trino | ✅ Xong |
| 2 | Generator + Bronze | 6 bảng Bronze khớp nguồn | ✅ Xong |
| 3 | Silver + Gold | Bronze → Silver → Gold → SQL | ✅ Xong |
| 4 | Kafka + streaming | 1000 event/s, 10 phút → Bronze | ✅ Gate đạt (25/10); ⚠️ reader chưa ổn định khi chạy lâu |
| 5 | Realtime + ClickHouse + Grafana | ADMISSION → Kafka → Streaming → ClickHouse → Grafana, "Current patients: N → N+1" | 🟡 Mới có dashboard Grafana dạng code; **chưa có** ClickHouse, aggregation realtime, service Grafana |
| 6 | Kubernetes | Core platform chạy trên K8s | ⬜ Chưa bắt đầu (`infra/k8s/*` mới có thư mục rỗng) |
| 7 | Observability + scale + recovery | load ↑ → lag ↑ → scale → lag ↓; kill executor → recover | ⬜ Chưa bắt đầu |
| 8 | Benchmark + E2E | | ⬜ Chưa bắt đầu |
| 9 | Freeze + demo cuối | Deploy mới → demo đầy đủ | ⬜ Chưa bắt đầu |

**Kết luận:** P0 (batch Lakehouse) đã hoàn chỉnh và chạy lại được. Speed layer mới tới Bronze. Tuần 5 đang trễ khoảng 2 tuần so với lịch, vì phần ClickHouse và aggregation realtime (TV1–TV3) chưa ai bắt đầu. Toàn bộ P2 (Kubernetes, Prometheus, scale, recovery) chưa có code.

## 2. Những gì đang chạy được (trên `main`)

| Phần | Lệnh | Kết quả đã đo |
|---|---|---|
| Hạ tầng local | `make up` | MinIO, Iceberg REST, Trino đều healthy |
| Dữ liệu tổng hợp | `make generate-data PATIENTS=1000 SEED=42` | 6 thực thể (patients 1000, encounters 3094, lab 12839, medications 6149, vitals 9261, bed_events 3938), có lỗi cài sẵn có chủ đích, tái lập được theo seed; generator đã thử tới 100k bệnh nhân |
| Batch end-to-end | `make batch` | Bronze khớp số dòng + checksum ID; Silver hợp lệ/quarantine: encounters 3058/5, lab 12719/12, medications 6139/10, vitals 9248/13, bed_events 3934/4; Gold 4 bảng (14343 / 2373 / 3258 / 14343 dòng) |
| Phân tích lịch sử | `make query` | 11/11 query Silver/Gold có dữ liệu (ICU nằm viện TB 187,3 h; Glucose bất thường 27,8%; ICU tháng 8 TB 38,3% của 20 giường) |
| Kafka | `make kafka-up`, `make kafka-smoke`, `make kafka-recovery` | KRaft 1 node, topic `hospital.events` + `hospital.dlq`, key theo bệnh nhân; dữ liệu còn nguyên khi tạo lại broker |
| Producer | `make produce …` | Sinh sự kiện có trễ, trùng, sai định dạng có chủ đích; đo được tới khoảng 9.600 event/s với 1 process |
| Stream → Bronze | `make stream-bronze-docker` | Sự kiện vào raw/quarantine; kill reader rồi chạy lại (ở 10–20 event/s) không mất, không trùng offset |
| Gate tuần 4 | `make stream-gate` | PASS: 600.000 message ở 1000 event/s, mỗi message vào Bronze đúng một lần |
| Kịch bản demo | `scripts/checkpoint_demo.sh` | Exit 0 trên clone mới với volume mới (25/10) |
| Test | `make test` | 155 passed, 10 skipped (UTC và UTC+7), đo trên `main` ngày 25/10, trước khi có test Grafana |
| Grafana (code) | `make dashboard-build` | Sinh 3 dashboard JSON (Overview, Realtime, Historical) và provisioning; **chưa nạp vào Grafana thật** |

## 3. Tài liệu đã có cho Checkpoint 1 (26/10)

- Slide Beamer tiếng Việt `docs/slides/checkpoint-1.tex` (`make slides`).
- Báo cáo ngắn tiếng Anh (`make slides-report`).
- Bộ slide giải thích cho người mới (`make slides-explained`).
- Video giới thiệu có thuyết minh (`make video`, PR #24).
- `docs/architecture.md`, `docs/demo-script.md`, `docs/data-model.md`, `next_step.md`.

## 4. PR và review

| PR | Nội dung | Trạng thái |
|---|---|---|
| #1–#25 | Tuần 1–4, checkpoint | Đã merge; từ #10 trở đi đều có audit độc lập PASS (#16 được audit bù, PASS) |
| #27 | Grafana dashboards + provisioning dạng code (TV4) | Đã merge 08/11 **khi chưa có audit**, do chính TV4 tự merge. Audit sau merge **FAIL**: panel "Patients by department" cộng số patient-day nhưng gắn nhãn là lượt khám |
| #28 | Sửa lỗi audit của #27 (nhãn patient-days, chặt hơn ở bước kiểm tra cột Gold) | **Mở**, chưa audit |
| #26 | Service Grafana trong Compose, `make dashboard`, `make dashboard-smoke` | **Mở**, chưa chạy trên Docker thật vì máy của TV4 không có Docker |

## 5. Đóng góp theo thành viên (commit không tính merge trên `main`)

| Thành viên | Tổng | Từ 26/10 | Ghi chú |
|---|---:|---:|---|
| TV1 hungtrab | 52 | 0 | Generator, Silver, `make batch`, producer, gate stream, tài liệu checkpoint; làm thay phần Gold/query của TV4 cho checkpoint |
| TV2 Nituv05 / Vũ Thường Tín | 9 | 0 | MinIO, Bronze DDL, Silver clinical, Kafka local. Email commit không hợp lệ nên GitHub chưa gắn commit vào tài khoản |
| TV3 Duck | 15 | 0 | Spark config, Bronze ingestion, Kafka reader, stream reader trong Compose, architecture |
| TV4 frichickens | 16 | 4 (+9 trên branch) | Trino verify, Gold job, stream sink; Grafana tuần 5 |

`works.md` yêu cầu mỗi thành viên có code chạy được được merge hằng tuần. Từ sau checkpoint chỉ TV4 có commit mới.

## 6. Rủi ro và nợ kỹ thuật

1. **Stream reader thiếu bộ nhớ.** Khi stream Bronze có khoảng 660k dòng, container (1,25 GiB) bị OOM-kill một lần. Ở 1000 event/s reader chỉ đạt khoảng 950 event/s nên lag tăng dần. Giả thuyết là sink chạy `MERGE` trên toàn bảng mỗi micro-batch, cộng với khoảng 9.700 file nhỏ; chưa profile. Đây là nền cho tuần 5–7, cần sửa trước khi đưa sự kiện lên ClickHouse.
2. **Gate tuần 5 chưa có đầu vào.** Chưa có ClickHouse (`sql/clickhouse/` rỗng), chưa có aggregation realtime, chưa có service Grafana trên `main`. Schema ClickHouse trong `dashboard/grafana/README.md` mới là đề xuất của TV4, cần TV3 chốt lại.
3. **Quy trình review bị phá:** #27 được merge trước khi có audit (trái `AGENTS.md` §33), audit sau merge FAIL, và bản sửa #28 vẫn chưa được duyệt.
4. **Môi trường chưa đồng đều:** máy của TV4 không có Docker; trên Windows có 25 test lỗi sẵn (`pyarrow` thiếu `tzdata`/zoneinfo).
5. **Việc nhỏ còn tồn:**
   - `make gold` trên host chỉ chạy với PySpark 3.5;
   - vài ví dụ `make produce` trong docs thiếu `KAFKA_BOOTSTRAP_SERVERS`;
   - `cli_command` chưa có tham số `output_format`;
   - patch cho cả 3 việc trên đã soạn sẵn cho TV4 nhưng chưa ai làm.
6. **Thời gian:** còn khoảng 4 tuần (6–9) cho K8s, Prometheus, scale, recovery, benchmark và demo cuối. Đây là phần nặng nhất của đề.

## 7. Đề xuất cho 1–2 tuần tới

| Ưu tiên | Việc | Người (theo `works.md`) |
|---|---|---|
| 1 | Audit #28 và merge; cho #26 chạy trên một máy có Docker (`make dashboard-smoke`), audit rồi merge | TV4 + một người có Docker |
| 2 | Sửa sink Bronze (`MERGE` có lọc theo offset, hoặc append + dedup), profile bộ nhớ, chạy lại `make stream-gate` | TV3/TV4 |
| 3 | ClickHouse: service Compose, schema `hospital.*` chốt với TV4, sink idempotent | TV3 |
| 4 | Aggregation realtime: hospital state (TV1); ICU, khoa, xét nghiệm bất thường 15 phút (TV2) | TV1, TV2 |
| 5 | Chạy gate tuần 5 "Current patients: N → N+1" trên Grafana bằng một chuỗi sự kiện đã biết | Cả nhóm |
| 6 | Bắt đầu tuần 6 song song: MinIO/Iceberg/Trino trên K8s (TV1), Strimzi (TV2), Spark Operator (TV3) | TV1–TV3 |

Nếu thời gian không đủ, nên giữ phạm vi theo thứ tự trong `AGENTS.md` §6: P1 (ClickHouse + Grafana chạy thật) trước, sau đó chỉ đưa lên K8s những gì cần để demo scale và recovery.
