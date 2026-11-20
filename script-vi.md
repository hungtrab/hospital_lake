# Kịch bản trình bày HospitalLake bằng tiếng Việt

Nhóm 22: Tran Quang Hung, Vu Thuong Tin, Chu Anh Duc, Dinh Quoc Bao.

Kịch bản theo 9 slide của báo cáo hiện tại. Thời lượng dự kiến khoảng 6–7 phút, tùy tốc độ trình bày.

## Slide 1 — HospitalLake

Em chào thầy. Nhóm em là nhóm 22, gồm bốn thành viên: Tran Quang Hung, Vu Thuong Tin, Chu Anh Duc và Dinh Quoc Bao.

Hôm nay, em xin trình bày đề tài HospitalLake và tiến độ mà nhóm đã thực hiện.

Nội dung gồm dữ liệu đầu vào, kiến trúc hệ thống, các kết quả đã chạy được và kế hoạch tiếp theo.

## Slide 2 — HospitalLake connects data to analytics

Trong bệnh viện có nhiều loại ghi chép khác nhau, chẳng hạn thông tin nhập viện, kết quả xét nghiệm, thuốc và giường bệnh.

Muốn biết tình hình chung, ví dụ mỗi khoa tiếp nhận bao nhiêu lượt hoặc thời gian nằm viện trung bình là bao lâu, trước hết cần liên kết và kiểm tra các ghi chép này.

HospitalLake là hệ thống nhóm em xây dựng để gom dữ liệu vào một kho chung, giữ bản gốc, tạo các bảng sạch rồi tính số liệu phục vụ phân tích.

Ở mốc báo cáo này, nhóm đã chạy được luồng xử lý dữ liệu theo lô trên môi trường local. Dữ liệu dùng trong project là dữ liệu giả lập do chương trình tạo ra.

Sau đây, em xin giới thiệu cụ thể bộ dữ liệu.

## Slide 3 — Six entities model hospital activity

Bộ dữ liệu có sáu nhóm: bệnh nhân, lượt khám hoặc nhập viện, kết quả xét nghiệm, y lệnh thuốc, lần đo sinh hiệu và sự kiện giường bệnh.

Trong đó, encounter được hiểu là một lượt khám hoặc nhập viện. Một bệnh nhân có thể quay lại nhiều lần, và trong mỗi lượt có thể phát sinh nhiều xét nghiệm hoặc lần đo sinh hiệu.

Các bảng liên kết với nhau bằng mã bệnh nhân và mã lượt khám.

Bộ dữ liệu mặc định của nhóm có một nghìn bệnh nhân và 3.094 dòng dữ liệu lượt khám hoặc nhập viện. Sáu nhóm được lưu thành sáu file Parquet như trên slide.

Chương trình hỗ trợ sinh lại cùng bộ dữ liệu khi giữ nguyên seed và cấu hình. Điều này giúp các thành viên chạy và đối chiếu kết quả với nhau.

Tiếp theo là cấu trúc của một file cụ thể.

## Slide 4 — Parquet stores typed tables

Parquet là định dạng file lưu dữ liệu dạng bảng, với tên cột và kiểu dữ liệu được xác định. Ta có thể đọc nội dung của nó ra thành hàng và cột, tương tự một bảng tính.

Ví dụ, file encounters có bảy cột. Mỗi dòng mô tả một lượt khám hoặc nhập viện.

Encounter ID là mã của lượt khám. Patient ID là mã bệnh nhân, dùng để liên kết dòng này với bảng bệnh nhân.

Loại lượt khám, khoa và trạng thái được lưu dưới dạng chuỗi ký tự. Thời gian nhập viện và xuất viện được lưu dưới dạng timestamp theo múi giờ UTC.

Nếu bệnh nhân chưa xuất viện thì thời gian xuất viện có thể để null, tức là chưa có giá trị.

Sau đây là một dòng dữ liệu thực tế trong file đã sinh.

## Slide 5 — Example: one actual encounter record

Đây là dòng đầu tiên của file encounters mà chương trình đã tạo. Nhóm hiển thị dòng này theo chiều dọc để dễ đọc từng trường.

Bản ghi mô tả một lượt điều trị nội trú, với mã lượt và mã bệnh nhân được thể hiện trên slide.

Thời gian nhập viện là ngày 8 tháng 12 năm 2025, và thời gian xuất viện là ngày 10 tháng 12. Các thời điểm cụ thể đều theo UTC.

Trong dữ liệu gốc, khoa được viết tắt là SURG. Khi làm sạch, hệ thống sẽ chuẩn hóa giá trị này thành SURGERY. Trạng thái của lượt này là discharged, nghĩa là đã xuất viện.

Ví dụ này cho thấy cấu trúc đầu vào và một giá trị cần được chuẩn hóa. Đây vẫn là dữ liệu giả lập, dù bản ghi được lấy trực tiếp từ file thật mà chương trình đã sinh.

Tiếp theo, em xin trình bày cách các file này đi qua hệ thống.

## Slide 6 — The batch pipeline feeds historical analytics

Ở mốc hiện tại, nhóm tập trung vào nhánh batch, tức là xử lý theo lô đối với các file dữ liệu lịch sử.

Spark đọc các file Parquet, sau đó tạo lần lượt các bảng Bronze, Silver và Gold. Khi đã có các bảng này, Trino cho phép đặt câu hỏi bằng SQL và nhận kết quả phân tích.

Mỗi công nghệ có một nhiệm vụ riêng. MinIO giữ các file dữ liệu. Iceberg quản lý các file đó thành bảng. Spark thực hiện việc xử lý, còn Trino thực hiện các truy vấn.

Thiết kế của nhóm cũng có nhánh streaming để xử lý sự kiện đến liên tục. Tuy nhiên, phần này vẫn đang được hoàn thiện và hiện chưa sẵn sàng để demo.

ClickHouse và Grafana là các thành phần dự kiến bổ sung để phục vụ chỉ số trực tiếp và biểu đồ sau khi nhánh streaming sẵn sàng.

Em xin giải thích rõ hơn ba lớp dữ liệu trong nhánh batch.

## Slide 7 — Batch keeps raw data and isolates errors

Bronze, Silver và Gold là ba mức xử lý dữ liệu.

Bronze giữ các dòng gốc và bổ sung thông tin về lần nhập dữ liệu. Việc này giúp nhóm đối chiếu hoặc xử lý lại khi cần.

Silver chuẩn hóa giá trị, gộp các bản sao giống hệt nhau và kiểm tra tính hợp lệ. Các dòng lỗi được giữ trong một khu riêng, kèm lý do để truy vết.

Gold chứa các bảng tổng hợp phục vụ phân tích, chẳng hạn số lượt nhập viện theo ngày và khoa.

Ví dụ, bảng lượt khám có 3.094 dòng đầu vào. Sau xử lý, còn 3.058 dòng hợp lệ, 31 bản sao được gộp và 5 dòng lỗi được giữ riêng.

Như vậy, nhóm có thể giải thích rõ vì sao số dòng thay đổi và kiểm tra được dữ liệu lỗi, thay vì bỏ chúng đi mà không có dấu vết.

Sau bước chuẩn bị dữ liệu, nhóm sử dụng Trino để chạy các câu hỏi phân tích.

## Slide 8 — Historical queries return useful results

Nhóm đã chạy mười một câu truy vấn phân tích và cả mười một câu đều trả về dữ liệu.

Trên slide là ba kết quả ví dụ.

Thời gian nằm viện trung bình của khoa hồi sức tích cực, hay ICU, là 187,3 giờ.

Tỷ lệ kết quả xét nghiệm Glucose nằm ngoài khoảng tham chiếu được cấu hình là 27,8 phần trăm.

Mức sử dụng ICU trung bình trong tháng 8 là 38,3 phần trăm, với sức chứa được cấu hình là hai mươi giường.

Các kết quả này được tính từ bộ dữ liệu giả lập của nhóm. Chúng cho thấy luồng xử lý đã nối được dữ liệu đầu vào với kết quả phân tích.

Cuối cùng, em xin trình bày kế hoạch tiếp theo.

## Slide 9 — Next: complete streaming and add serving

Ưu tiên tiếp theo của nhóm là hoàn thiện và ổn định nhánh streaming, trong đó cần kiểm tra việc sử dụng bộ nhớ và số lượng file phát sinh, trước khi đưa vào demo trực tiếp.

Trong tuần 5, nhóm cũng dự kiến bổ sung các chỉ số realtime vào ClickHouse và xây dựng biểu đồ trên Grafana.

Trong tuần 6 và tuần 7, nhóm dự kiến triển khai các dịch vụ trên Kubernetes, sau đó thử khả năng mở rộng và phục hồi khi có lỗi.

Các tuần cuối sẽ tập trung vào đo hiệu năng, kiểm thử toàn bộ luồng và chuẩn bị một bản demo có thể chạy lại ổn định.

Ở mốc hiện tại, kết quả chính của nhóm là luồng phân tích dữ liệu lịch sử đã chạy được: từ file đầu vào, qua làm sạch và tổng hợp, đến truy vấn kết quả.

Phần trình bày của nhóm em đến đây là hết. Em cảm ơn thầy và xin nhận các câu hỏi.
