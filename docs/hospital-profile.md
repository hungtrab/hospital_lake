# Bạch Mai · 78 Giải Phóng, Hà Nội

HospitalLake mô phỏng dòng người bệnh theo quy mô Bệnh viện Bạch Mai. Báo cáo năm 2025 của bệnh viện công bố gần 1,94 triệu lượt khám, hơn 263.000 lượt điều trị nội trú và thời gian điều trị trung bình 4,89 ngày. Mô hình dùng trung bình 5.315 lượt/ngày, xác suất nhập viện gần đúng 263.000/1.940.000 ≈ 13,6% và thời gian nằm viện theo phân phối tam giác từ 1 đến 9 ngày, trung bình 4,89 ngày. Số liệu tổng hợp năm dùng để hiệu chỉnh các lượt giả lập. [Báo cáo hoạt động năm 2025](https://bachmai.gov.vn/bai-viet/bach-mai-2026-vuon-minh-trong-ky-nguyen-moi-kien-tao-tuong-lai-y-hoc-viet-nam?id=d62b0ce8-04a8-4783-a9bb-71bb19586aa1) · [Địa chỉ cơ sở Hà Nội](https://bachmai.gov.vn/bai-viet/10-su-kien-tieu-bieu-cua-benh-vien-bach-mai-nam-2024-9782?id=dfe49219-9bcc-b809-531a-81c11805bad2).

## Bệnh nhân đến theo giờ Việt Nam

**Cuối tuần đông là kịch bản thử tải của nhóm.** Ngày thường có kỳ vọng 4.890 lượt, thứ Bảy 6.591 và Chủ nhật 6.165. Các hệ số 0,92 / 1,24 / 1,16 giữ trung bình tuần ở mức 5.315 lượt/ngày. Bạch Mai công bố chiều hướng ngược lại tại Khoa Khám bệnh theo yêu cầu, khoảng 2.000 lượt thứ Bảy và 1.600 lượt Chủ nhật. Bài viết cũng ghi nhận 3.500–4.000 lượt đăng ký buổi sáng, 200–300 buổi chiều và dưới 100 mỗi phiên tối. Các số đếm này định hướng hình dạng đường cong ngoại trú. [Thống kê của Bạch Mai, tháng 4/2025](https://bachmai.gov.vn/bai-viet/trai-nghiem-kham-benh-buoi-chieu-hay-la-nguoi-benh-thong-thai?id=7032447a-ffdc-4d82-9a53-bfc498458870)

Mô hình dùng quá trình Poisson theo từng khung giờ. Bảng ghi phần trăm **lượt đến trong từng luồng** do nhóm chọn. Ngoại trú nhận đăng ký sớm và bắt đầu khám từ 06:00. Cấp cứu tiếp nhận 24 giờ. Mỗi mốc giờ là đầu khung, mốc kế tiếp là cuối khung. Tỷ trọng cấp cứu chung xấp xỉ 4,38%, lấy trung điểm của 80.000–90.000 ca/năm tại A9 chia cho quy mô 1,94 triệu lượt khám. Hai con số có phạm vi thống kê khác nhau nên tỷ trọng này là ước lượng. [Trung tâm Cấp cứu A9](https://bachmai.gov.vn/en/department/trung-tam-cap-cuu-a9/60a668af-6bd7-e930-1bf2-59765f13d55f)

| Bắt đầu | 00:00 | 05:00 | 05:30 | 07:00 | 10:00 | 12:00 | 13:00 | 16:30 | 17:00 | 20:00 | 22:00 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Ngoại trú ngày thường | 0 | 3 | 16 | 54 | 18 | 0 | 6 | 1 | 2 | 0 | 0 |
| Ngoại trú cuối tuần | 0 | 0 | 18 | 55 | 18 | 0 | 9 | 0 | 0 | 0 | 0 |
| Cấp cứu mỗi ngày | 9 | 1 | 5 | 15 | 11 | 5 | 19 | 3 | 17 | 9 | 6 |

## Tuổi, nhóm bệnh và thời điểm nhập viện

- **Ngoại trú** dùng mẫu 477 người trưởng thành ở cơ sở Hoàng Mai, Bệnh viện Đại học Y Hà Nội năm 2024. Nhóm 18–59 tuổi chiếm 71,7%. Trong 1.110 mã chẩn đoán, tiêu hóa chiếm 21,1%, nội tiết 13,6% và tim mạch 9,7%. Mỗi lượt giả lập lấy một nhóm từ các tần suất mã này. Với một số lượt, mô hình còn lấy mã cụ thể theo bảng 4 của nghiên cứu, chẳng hạn I10 (tăng huyết áp) chiếm 3,3% số mã ở nhóm 18–59 và 12% ở nhóm cao tuổi. Các lượt ngoài tập mã này chỉ có nhóm bệnh. Nhóm cao tuổi có trọng số tim mạch cao hơn, đồng thời mô hình giữ tỷ trọng chung của các nhóm. [Nghiên cứu và bảng số liệu](https://tapchinghiencuuyhoc.vn/index.php/tcncyh/article/download/3981/2763/54574)
- **Cấp cứu** dùng 22.385 hồ sơ tại Bệnh viện Đại học Y Hà Nội năm 2019. Mô hình lấy trọng số từ số ca theo tuổi (1.896 dưới 18, 12.625 từ 18 đến dưới 50, 7.864 từ 50 trở lên) và theo 12 nhóm bệnh. Tiêu hóa, truyền nhiễm và chấn thương là ba nhóm thường gặp nhất. Tỷ lệ 28,37% chuyển khoa sau cấp cứu làm mốc gần đúng cho quyết định tiếp tục nội trú. [Nghiên cứu cấp cứu, bảng 1–3](https://scholar.dlu.edu.vn/thuvienso/bitstream/DLU123456789/143792/1/CVv251V147S112021317.pdf)
- **Giả định của nhóm** gồm tuổi cụ thể chọn đều trong dải tuổi đến 95, hệ số liên quan giữa tuổi và nhóm bệnh, xác suất phân khoa, thời lượng khám và lịch giường. Trẻ em xuất hiện trong luồng cấp cứu. Nhóm bệnh đi theo bệnh nhân suốt lượt khám. Giờ đăng ký và giờ nhập nội trú là hai mốc riêng. Người bệnh chỉ nhận giường sau khi được tiếp nhận, khám, hoàn tất xét nghiệm nếu có và có giường trống. Trong **Người bệnh → Xem hành trình**, sự kiện đăng ký hiển thị tuổi, nhóm bệnh và mã bệnh (nếu có) cho lượt mới.

Mỗi lượt bắt đầu khi đến giờ. Nếu hệ thống gián đoạn, lượt chưa tiếp nhận quá hạn 3 giây được đánh dấu bỏ lỡ. Lượt đã tiếp nhận và tài nguyên đã giữ tiếp tục dùng cùng mã sau khi khởi động lại.

## Bản đồ và nguồn số người trong phòng

Mặt bằng trên tab **Bản đồ** do nhóm tự thiết kế. Mô hình có 176 phòng ngoại trú thuộc 11 chuyên khoa, 28 phòng cấp cứu, 72 vị trí điều dưỡng tiếp nhận ban ngày và 4.000 giường. Các giường được chia thành hồi sức 160, nội trú cấp cứu 260, tim mạch 620, ngoại khoa 560 và nội tổng hợp 2.400. Mỗi buồng hồi sức có 2 giường, các buồng khác tối đa 4 giường. Mỗi tầng trên sơ đồ có tối đa 12 phòng. Đây là tham số của mô phỏng. Riêng Viện Thần kinh của Bạch Mai công bố 12 phòng khám theo yêu cầu, 5 phòng khám thường và 57 bác sĩ trong 193 viên chức. Mô hình lấy tổng 17 phòng làm mốc cho nhóm Thần kinh. Các khoa còn lại dùng số phòng giả định. Bệnh viện cũng có Trung tâm Thận – Tiết niệu và Lọc máu. [Viện Thần kinh](https://bachmai.gov.vn/don-vi/vien-than-kinh-bach-mai/c95dd917-c4a4-261d-ec5e-4db1677dad22) · [Trung tâm Thận – Tiết niệu và Lọc máu](https://bachmai.gov.vn/don-vi/trung-tam-than-tiet/c485973e-0d4c-2007-a8e1-54f37677acd9).

Bản đồ đọc sổ sự kiện Silver qua Trino, lấy mã phòng và giường từ bản tin Bronze tương ứng. Người đang khám xuất hiện từ lúc bắt đầu khám đến hết thời lượng chiếm phòng. Người nằm viện xuất hiện ở giường được phân cho đến khi chuyển, trả giường hoặc xuất viện. Giường cũ chưa ánh xạ và ca đã nhập viện đang chờ thông tin giường hiện riêng. Khi chưa có dữ liệu mới, trang giữ bản chụp trước và báo lỗi. Các mã bệnh nhân là mã giả lập. Xem [quy tắc xử lý dữ liệu bẩn](data-cleaning.md).

Để đếm lượt đến, ca nhập viện theo giờ, tuổi và mã bệnh của cùng một đoàn hệ 28 ngày, chạy `python scripts/profile_live_visits.py`. Lệnh chạy ngoại tuyến, dùng lịch và bộ sinh hành trình của dịch vụ đang triển khai.

## HospitalLake khác hệ thống bệnh viện thật ở đâu?

| Khi demo | HospitalLake | Tại bệnh viện |
|---|---|---|
| Người bệnh quay lại | Bộ sinh Bạch Mai tạo một mã người bệnh cho mỗi lượt tự động. Nhập tay cho phép dùng lại mã sau xuất viện. | Hồ sơ bệnh án hỗ trợ tra cứu tiền sử và kết quả các lần điều trị. |
| Nhập hoặc xuất viện | Bộ sinh chọn khoa, thời gian nằm và thời điểm rời giường. Thao tác nhập tay tạo sự kiện, cập nhật bản đồ và số liệu. | Nhân viên phụ trách ghi nhận quyết định chuyên môn. Bạch Mai phát hành giấy ra viện và bản tóm tắt bệnh án có chữ ký số. |
| Xét nghiệm, điều trị | Nhóm mô phỏng tuổi, nhóm bệnh, một số mã ICD và kết quả xét nghiệm. | Hồ sơ điện tử Bạch Mai tổng hợp kết quả xét nghiệm, hình ảnh, thuốc và kế hoạch chăm sóc từ hoạt động điều trị. |
| Số liệu vận hành | Số lượt, giờ đến và thời gian chờ được tính trên sự kiện giả lập đã xử lý. Mặt bằng và năng lực từng khoa do nhóm dựng. | Để dùng những chỉ số này cho bố trí nhân sự thực tế, cần đối chiếu dữ liệu tiếp đón, giờ bác sĩ bắt đầu khám, lịch trực và giường thực sự sẵn sàng. |

Khi trình bày, có thể chọn một giờ cao điểm rồi xem phân bố thời gian chờ trong ngày. Hai biểu đồ mô tả hai tập sự kiện khác nhau. Lượt đến tính theo giờ đăng ký; biểu đồ chờ tính những lượt đã bắt đầu khám. Người vẫn đang chờ chưa có thời gian chờ hoàn chỉnh. Phân quyền theo vai trò, lịch sử ai sửa bệnh án và vận hành dự phòng trên nhiều máy là các phần còn cách biệt với môi trường sử dụng hồ sơ người thật. [Bệnh án điện tử Bạch Mai](https://bachmai.gov.vn/bai-viet/benh-vien-bach-mai-chinh-thuc-ap-dung-benh-an-dien-tu?id=7ac2665f-6b4d-990a-0cb1-13318fe0bc1a) · [Quy trình ký số giấy ra viện](https://bachmai.gov.vn/bai-viet/thong-bao-ve-viec-trien-khai-ky-so-dong-dau-so-va-tra-cuu-giay-to-dien-tu-do-benh-vien-bach-mai-phat-hanh?id=7f3ba26c-937d-431c-a387-20331026bd28)
