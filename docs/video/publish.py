"""Normalize audio, attach chapters, verify the completed MP4 and archive sources."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import zipfile

PROJECT = Path(__file__).resolve().parent
ROOT = PROJECT.parents[1]
BUILD = ROOT / "results" / "video"
OUT = BUILD / "output"


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def probe(path):
    return json.loads(subprocess.check_output([
        "ffprobe", "-v", "error", "-show_streams", "-show_format", "-show_chapters", "-of", "json", str(path)
    ]))


def validate_sources():
    data = json.loads((PROJECT / "src" / "timeline.json").read_text())
    content = json.loads((PROJECT / "content.json").read_text())
    manifest = json.loads((BUILD / "render-manifest.json").read_text())
    if data.get("draft") or data.get("content_digest") != sha(PROJECT / "content.json"):
        raise ValueError("Draft or stale narration timeline; prepare and render again")
    if len(data["scenes"]) != len(content["scenes"]):
        raise ValueError("Scene count differs from the authored script")
    for file, digest in manifest["inputs"].items():
        if sha(PROJECT / file) != digest:
            raise ValueError(f"Render is stale: {file} changed; render the video again")
    if sha(BUILD / "hospitallake-raw.mp4") != manifest["raw_sha256"]:
        raise ValueError("Raw video differs from its successful render manifest")
    if data["voice"] != "Hải Đăng" or data["voice_region"] != "Bắc" or data["speed"] != .864:
        raise ValueError("Voice/region/speed differs from the requested pipeline")
    coverage = []
    expected_from = 0
    cue_count = 0
    for scene in data["scenes"]:
        if scene["from"] != expected_from or scene["duration"] <= 0:
            raise ValueError("Scene timeline contains a gap, overlap or empty scene")
        expected_from += scene["duration"]
        last_end = 0
        for cue in scene["captions"]:
            if not (last_end <= cue["startMs"] < cue["endMs"] <= scene["duration"] / data["fps"] * 1000):
                raise ValueError(f"Caption overlaps or escapes scene {scene['index']}")
            if len(cue["text"]) > 64 or "\n" in cue["text"]:
                raise ValueError("Caption exceeds the single-line source limit")
            last_end = cue["endMs"]
            cue_count += 1
        for part in scene["audio"]:
            meta = json.loads((PROJECT / "public" / part["src"]).with_suffix(".json").read_text())
            if meta["voice"] != "Hải Đăng" or "Bắc" not in meta["voice_label"] or meta["speed"] != .864:
                raise ValueError("Audio preset metadata differs from the video")
            coverage.append(meta["alignment_coverage"])
    if expected_from != data["durationInFrames"] or min(coverage) < .65:
        raise ValueError("Timeline or word-alignment coverage is invalid")
    return data, coverage, cue_count


def main():
    data, coverage, cue_count = validate_sources()
    OUT.mkdir(parents=True, exist_ok=True)
    raw = BUILD / "hospitallake-raw.mp4"
    target = OUT / "hospitallake-gioi-thieu.mp4"
    analysis = subprocess.run([
        "ffmpeg", "-hide_banner", "-nostats", "-i", str(raw), "-vn", "-af",
        "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json", "-f", "null", "-"
    ], capture_output=True, text=True, check=True)
    (BUILD / "loudness-analysis.log").write_text(analysis.stderr)
    measurements = json.loads(re.findall(r"\{[^{}]+\}", analysis.stderr)[-1])
    audio_filter = ("loudnorm=I=-16:TP=-1.5:LRA=11:linear=true:print_format=json"
                    f":measured_I={measurements['input_i']}:measured_TP={measurements['input_tp']}"
                    f":measured_LRA={measurements['input_lra']}:measured_thresh={measurements['input_thresh']}"
                    f":offset={measurements['target_offset']}")
    with (BUILD / "finalize.log").open("w") as log:
        subprocess.run([
            "ffmpeg", "-hide_banner", "-y", "-i", str(raw), "-i", str(BUILD / "chapters.txt"),
            "-map", "0:v:0", "-map", "0:a:0", "-map_metadata", "1", "-map_chapters", "1",
            "-c:v", "copy", "-af", audio_filter, "-c:a", "aac", "-b:a", "160k", "-ar", "48000",
            "-metadata:s:a:0", "language=vie", "-sn", "-movflags", "+faststart", str(target)
        ], stdout=log, stderr=subprocess.STDOUT, check=True)
    metadata = probe(target)
    video = next(stream for stream in metadata["streams"] if stream["codec_type"] == "video")
    audio = next(stream for stream in metadata["streams"] if stream["codec_type"] == "audio")
    seconds = float(metadata["format"]["duration"])
    if (video["width"], video["height"], video["r_frame_rate"], video["codec_name"], video["pix_fmt"]) != (1920, 1080, "30/1", "h264", "yuv420p"):
        raise ValueError("Video resolution/frame rate/format differs from the pipeline")
    if audio["codec_name"] != "aac" or audio["sample_rate"] != "48000" or audio.get("tags", {}).get("language") != "vie":
        raise ValueError("Audio format/language differs from the pipeline")
    if any(stream["codec_type"] == "subtitle" for stream in metadata["streams"]):
        raise ValueError("Unexpected subtitle stream; captions must be burned in exactly once")
    if len(metadata["chapters"]) != len(data["scenes"]) or abs(seconds - data["durationInFrames"] / data["fps"]) >= .15:
        raise ValueError("Chapter count or final duration does not match the authored timeline")
    for chapter, scene in zip(metadata["chapters"], data["scenes"]):
        if abs(float(chapter["start_time"]) - scene["from"] / data["fps"]) > .05:
            raise ValueError("A chapter start does not match its scene")
    decoded = subprocess.run([
        "ffmpeg", "-v", "error", "-threads", "2", "-i", str(target), "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"
    ], capture_output=True, text=True, check=True)
    if decoded.stderr.strip():
        raise RuntimeError(f"Decode errors: {decoded.stderr}")
    report = {
        "status": "PASS", "file": target.name, "duration_seconds": seconds, "size_bytes": target.stat().st_size,
        "resolution": [1920, 1080], "fps": 30, "video_codec": "h264", "audio_codec": "aac",
        "voice": data["voice"], "region": "Bắc", "atempo": .864, "relative_speed": 1.2,
        "speed_reference": "1.2 × Northern introduction reference at atempo .72",
        "caption_layers_configured": 1, "caption_font_size_px": 30, "caption_source_max_chars": 64,
        "caption_cues": cue_count, "subtitle_streams": 0, "chapters": len(data["scenes"]),
        "minimum_alignment_coverage": min(coverage), "decode_errors": 0,
        "audio_normalization": "Two-pass loudnorm I=-16:TP=-1.5:LRA=11",
        "sources": data["sources"], "evidence_date": data["evidence_date"], "sha256": sha(target),
        "limits": "Metadata/decoding checks do not certify pronunciation, visual quality or pipeline performance."
    }
    (OUT / "verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    shutil.copy2(BUILD / "narration.txt", OUT / "loi-doc.txt")
    guide = (f"HospitalLake — video giới thiệu bằng tiếng Việt\n\n"
             f"Mở {target.name}. Thời lượng {int(seconds)//60}:{int(seconds)%60:02}, 10 chương, 1080p/30fps.\n"
             "Giọng Hải Đăng miền Bắc; một dòng phụ đề nhỏ 30px được dựng vào hình.\n"
             "Các ví dụ nhỏ phục vụ giải thích; số đo thật ghi nhận ngày 25/10/2026.\n"
             "Chỉ số trực tiếp, Grafana và Kubernetes vẫn là kế hoạch tại thời điểm checkpoint.\n"
             "Bản lời đọc: loi-doc.txt. Kiểm tra kỹ thuật: verification.json.\n"
             "SRT nằm trong mã nguồn, tách khỏi thư mục MP4 để tránh phụ đề bị nạp hai lần.\n")
    (OUT / "HUONG-DAN.txt").write_text(guide)
    with zipfile.ZipFile(BUILD / "hospitallake-video.zip", "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for file in sorted(OUT.iterdir()):
            if file.is_file():
                archive.write(file, "video/" + file.name)
        for file in sorted(PROJECT.rglob("*")):
            relative = file.relative_to(PROJECT)
            if file.is_file() and not any(part in ("node_modules", "__pycache__") for part in relative.parts):
                archive.write(file, "project/docs/video/" + str(relative))
        for name in ("captions.srt", "chapters.txt", "narration.txt"):
            archive.write(BUILD / name, "project/results/video/" + name)
        for relative in data["sources"]:
            archive.write(ROOT / relative, "project/" + relative)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    print("PACKAGE:", BUILD / "hospitallake-video.zip", flush=True)


if __name__ == "__main__":
    main()
