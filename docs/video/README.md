# Video giới thiệu HospitalLake

Video dành cho người mới: bắt đầu từ ghi chép bệnh viện, giới thiệu sáu loại dữ liệu, quan hệ qua mã, hai nhánh xử lý và từng công việc của công nghệ. Ví dụ năm dòng chỉ phục vụ giảng giải; kết quả checkpoint được ghi rõ ngày đo 25/10/2026 và giới hạn hiện tại.

Áp dụng pipeline do người dùng cung cấp trong `~/shared/video_pipeline.md`: Remotion/React, GSAP theo frame, VieNeu-TTS giọng **Hải Đăng miền Bắc**, tốc độ `atempo=.864` (1,2 lần mốc giới thiệu cũ `.72`), căn từ bằng Whisper, một dòng phụ đề 30 px, 1080p/30 fps, FFmpeg chuẩn hóa âm thanh và gắn chương. Đây là công cụ tạo tài liệu tùy chọn, tách khỏi các dependency và dịch vụ của HospitalLake.

## Chuẩn bị

Cần Node.js 22, FFmpeg/ffprobe trên PATH, Python 3.11+ có PyTorch phù hợp máy. Có thể dùng môi trường tiếng nói đã hoạt động theo tài liệu pipeline bằng cách đặt `VIDEO_PYTHON` trỏ tới Python đó. Trên máy mới, tạo môi trường riêng và cài `docs/video/requirements.txt`; không cài model TTS vào môi trường Spark.

```bash
make video-setup
# Ví dụ môi trường riêng sau khi đã cài PyTorch thích hợp:
python3 -m venv .video-venv
.video-venv/bin/python -m pip install -r docs/video/requirements.txt
```

Model và Chrome có thể cần tải lần đầu. Đặt `VIDEO_BROWSER` nếu dùng Chrome/headless shell đã cài; nếu không, Remotion tự chuẩn bị browser. `VIDEO_CONCURRENCY=1` hoặc `2` phù hợp máy ít RAM. Không chạy TTS và render đồng thời.

## Dựng từng bước từ repo root

```bash
# Chạy riêng để giải phóng model TTS trước khi nạp Whisper.
make video-voice VIDEO_PYTHON=.video-venv/bin/python
make video-timeline VIDEO_PYTHON=.video-venv/bin/python
make video-preview
# Xem ảnh trong results/video/previews/ trước khi xuất.
make video VIDEO_CONCURRENCY=2
```

Preview giọng một cảnh: dùng Python đã chọn chạy `docs/video/prepare.py voice --limit 1`. Metadata cache cạnh MP3 xác nhận nhãn preset Bắc. Nếu cần tạo lại một cảnh, chạy `prepare.py voice --regenerate 4 --attempt 1` (ví dụ cảnh 4, seed thay thế), rồi chạy lại bước căn tiếng. `npm run studio` trong `docs/video/` cho phép nghe và tua timeline sau bước căn tiếng.

`make video` kiểm tra TypeScript, dựng MP4 thô, chuẩn hóa tiếng hai lượt, gắn 10 chương và kiểm tra giải mã toàn bộ. Trình xuất từ chối timeline cũ, draft hoặc video thô khác bản đã render. Các nguồn đầu vào được hash trong render manifest. Đây là kiểm tra sản phẩm video, không phải kiểm tra tích hợp batch/stream.

## Sản phẩm

```text
results/video/
├── output/
│   ├── hospitallake-gioi-thieu.mp4
│   ├── loi-doc.txt
│   ├── HUONG-DAN.txt
│   └── verification.json
├── hospitallake-video.zip        # MP4 + nguồn + audio cache + font/license
├── previews/                    # đầu cảnh, mốc lời đọc và cuối cảnh
├── captions.srt                 # tách khỏi MP4
├── chapters.txt
└── render-manifest.json
```

Nguồn được giữ ở thư mục này; audio, timeline, node_modules và sản phẩm đều được bỏ qua trong Git. Gói ZIP có nguồn và audio/timeline đã sinh để dựng lại hình mà không phải tạo giọng từ đầu. Trong gói, chạy `npm ci`, `npm run check`, `npm run render`, `python3 publish.py` từ `project/docs/video/`.

Sửa `content.json` để đổi lời đọc hoặc mốc; chạy lại voice → timeline → preview → video. Nếu chỉ đổi hình trong `src/Video.tsx`, chạy lại preview → video. `prepare.py` mặc định dùng Whisper CPU sau khi tiến trình TTS thoát; đặt `VIDEO_ALIGN_DEVICE=cuda` nếu muốn căn tiếng bằng GPU riêng sau TTS. Whisper nhận dạng độc lập rồi căn với lời gốc, không dùng cả kịch bản dài làm prompt.

## Kiểm tra nội dung và giới hạn

Đối chiếu với `docs/architecture.md`, `docs/demo-script.md`, `docs/data-model.md` và `sql/trino/queries.sql`. Nhánh stream hiện chỉ tới Bronze/quarantine; ClickHouse, Grafana, Kubernetes vẫn là kế hoạch ở checkpoint. Không suy diễn từ video rằng đã có dashboard realtime hoặc đảm bảo ổn định dài hạn.

`verification.json` xác nhận định dạng, thời lượng, chương, nguồn audio, phụ đề nguồn và giải mã. Việc phụ đề không tràn, cách phát âm và các mốc giải thích vẫn cần xem/nghe preview; kiểm tra tự động không thay thế việc đó. Không có nhạc nền hay luồng phụ đề thứ hai.

API chuyển động tham chiếu [Remotion useGsapTimeline](https://www.remotion.dev/docs/gsap/use-gsap-timeline), kết xuất dùng [renderMedia](https://www.remotion.dev/docs/renderer/render-media). Font Inter đi kèm giấy phép trong `public/fonts/LICENSE.txt`.
