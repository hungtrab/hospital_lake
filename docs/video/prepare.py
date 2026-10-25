"""Prepare local Northern narration and word-aligned, single-line captions.

Adapted from the workflow documented in ~/shared/video_pipeline.md; self-contained
so this project never imports or modifies the separate mathematics project.
Voice and alignment run as separate processes to avoid keeping two models in RAM.
"""
from __future__ import annotations

import argparse
from difflib import SequenceMatcher
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys

PROJECT = Path(__file__).resolve().parent
BUILD = PROJECT.parents[1] / "results" / "video"
PUBLIC = PROJECT / "public"


def load_content():
    data = json.loads((PROJECT / "content.json").read_text())
    if data["fps"] != 30 or data["speed"] != .864 or data["relative_speed"] != 1.2:
        raise ValueError("The authored video requires 30 fps and the pipeline's .864/1.2 voice speed")
    for scene in data["scenes"]:
        text = " ".join(part["text"] for part in scene["parts"])
        for beat in scene["beats"]:
            if normalized_phrase(beat) not in normalized_phrase(text):
                raise ValueError(f"Beat missing from scene {scene['title']}: {beat}")
    return data


def normalized_phrase(text):
    return " ".join(re.sub(r"[^\w\s]", "", text.casefold()).split())


def digest(data, text):
    settings = {key: data[key] for key in ("model", "voice", "speed")}
    settings.update(text=text, temperature=.7, max_new_frames=600, max_chars=200)
    return hashlib.sha256(json.dumps(settings, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def probe_duration(path):
    return float(subprocess.check_output([
        "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)
    ]))


def paths(index, part):
    audio = PUBLIC / "audio" / f"{index:02}_{part}.mp3"
    return audio, audio.with_suffix(".json")


def read_cache(data, text, audio, metadata):
    if not audio.is_file() or not metadata.is_file():
        return None
    meta = json.loads(metadata.read_text())
    return meta if meta.get("digest") == digest(data, text) else None


def prepare_voice(data, limit=None, regenerate=None, attempt=0):
    pending = []
    for i, scene in enumerate(data["scenes"][:limit]):
        for j, part in enumerate(scene["parts"]):
            audio, metadata = paths(i, j)
            if regenerate is not None and i + 1 != regenerate:
                continue
            if regenerate is not None or not read_cache(data, part["text"], audio, metadata):
                pending.append((i, j, scene, part))
    if not pending:
        print("All requested narration matches cache", flush=True)
        return
    from vieneu import Vieneu
    import numpy as np
    import torch

    torch.set_num_threads(4)
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    tts = Vieneu(backbone_repo=data["model"], max_batch_size=4, max_streams=1)
    matches = [(label, name) for label, name in tts.list_preset_voices() if name == data["voice"]]
    if not matches or "Bắc" not in matches[0][0]:
        raise RuntimeError(f"Expected publisher-labelled Northern preset: {matches}")
    print("VOICE:", matches[0][0], flush=True)
    (PUBLIC / "audio").mkdir(parents=True, exist_ok=True)
    (BUILD / "native").mkdir(parents=True, exist_ok=True)
    for i, j, scene, part in pending:
        audio, metadata = paths(i, j)
        print(f"SPEAK {i + 1}/{len(data['scenes'])}: {scene['title']}", flush=True)
        seed = 9000 + i * 10 + j + attempt
        torch.manual_seed(seed)
        np.random.seed(seed)
        waveform = tts.infer(part["text"], voice=data["voice"], temperature=.7,
                             max_new_frames=600, max_chars=200)
        native = BUILD / "native" / f"{i:02}_{j}.wav"
        tts.save(waveform, str(native))
        temporary = audio.with_suffix(".tmp.mp3")
        subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", str(native), "-af",
                        f"atempo={data['speed']}", "-c:a", "libmp3lame", "-b:a", "160k", str(temporary)], check=True)
        temporary.replace(audio)
        meta = {"digest": digest(data, part["text"]), "voice": data["voice"],
                "voice_label": matches[0][0], "region": "Bắc", "model": data["model"],
                "speed": data["speed"], "duration": probe_duration(audio), "text": part["text"], "generation_seed": seed}
        metadata.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n")
        print(f"AUDIO {i + 1}: {meta['duration']:.2f}s", flush=True)
    tts.close()


def align_words(text, recognized, duration):
    source = text.split()
    matcher = SequenceMatcher(a=[normalized_phrase(token) for token in source],
                              b=[normalized_phrase(word["text"]) for word in recognized], autojunk=False)
    anchors = {}
    for block in matcher.get_matching_blocks():
        for j in range(block.size):
            word = recognized[block.b + j]
            anchors[block.a + j] = (word["start"], word["end"])
    coverage = len(anchors) / max(1, len(source))
    if coverage < .65:
        raise RuntimeError(f"Speech alignment only {coverage:.1%}; inspect audio and regenerate before rendering")
    positions = [anchors.get(i) for i in range(len(source))]
    i = 0
    while i < len(source):
        if positions[i] is not None:
            i += 1
            continue
        first = i
        while i < len(source) and positions[i] is None:
            i += 1
        start = positions[first - 1][1] if first else 0.0
        end = positions[i][0] if i < len(source) else duration
        end = max(start, min(duration, end))
        weight = sum(max(1, len(token)) for token in source[first:i])
        cursor = start
        for index in range(first, i):
            finish = cursor + (end - start) * max(1, len(source[index])) / weight
            positions[index] = (cursor, finish)
            cursor = finish
    result = []
    last = 0.0
    for token, (start, end) in zip(source, positions):
        start = max(last, min(start, duration - .02))
        end = min(duration, max(start + .01, end))
        result.append({"text": token, "start": start, "end": end})
        last = start
    return result, coverage


def make_captions(words, offset, duration, max_chars=64):
    groups, group = [], []
    for word in words:
        if group and len(" ".join(item["text"] for item in group + [word])) > max_chars:
            groups.append(group)
            group = []
        group.append(word)
        if word["text"].endswith((".", "?", "!", ";", ":")):
            groups.append(group)
            group = []
    if group:
        groups.append(group)
    cues = []
    for index, group in enumerate(groups):
        start = offset + group[0]["start"]
        next_start = offset + groups[index + 1][0]["start"] if index + 1 < len(groups) else offset + duration
        end = min(next_start - .02, offset + duration, offset + group[-1]["end"] + .12)
        if end <= start:
            raise ValueError(f"Caption has no positive duration: {group}")
        cues.append({"text": " ".join(word["text"] for word in group),
                     "startMs": round(start * 1000), "endMs": round(end * 1000)})
    return cues


def timestamp(seconds):
    millis = round(seconds * 1000)
    hours, millis = divmod(millis, 3600000)
    minutes, millis = divmod(millis, 60000)
    seconds, millis = divmod(millis, 1000)
    return f"{hours:02}:{minutes:02}:{seconds:02},{millis:03}"


def prepare_alignment(data):
    import numpy as np
    from faster_whisper import WhisperModel

    # Alignment starts only after the separate TTS process exits.
    device = os.environ.get("VIDEO_ALIGN_DEVICE", "cpu")
    if device not in ("cpu", "cuda"):
        raise ValueError("VIDEO_ALIGN_DEVICE must be cpu or cuda")
    recognizer = None
    cursor_frames, scenes, all_cues, transcript = 0, [], [], []
    fps = data["fps"]
    for i, scene in enumerate(data["scenes"]):
        cursor = data["lead_seconds"]
        audio_parts, captions, words = [], [], []
        for j, part in enumerate(scene["parts"]):
            audio, metadata = paths(i, j)
            meta = read_cache(data, part["text"], audio, metadata)
            if meta is None:
                raise RuntimeError(f"Missing/stale narration {audio}; run prepare.py voice first")
            if not meta.get("words"):
                if recognizer is None:
                    recognizer = WhisperModel("small", device=device,
                                              compute_type="int8_float16" if device == "cuda" else "int8", cpu_threads=4)
                pcm = np.frombuffer(subprocess.check_output([
                    "ffmpeg", "-v", "error", "-i", str(audio), "-ac", "1", "-ar", "16000", "-f", "f32le", "pipe:1"
                ]), dtype=np.float32)
                segments, _ = recognizer.transcribe(pcm, language="vi", word_timestamps=True, beam_size=5,
                                                   vad_filter=False,
                                                   condition_on_previous_text=False)
                recognized = []
                for segment in segments:
                    for word in segment.words or []:
                        tokens = word.word.split()
                        for k, token in enumerate(tokens):
                            recognized.append({"text": token,
                                               "start": word.start + (word.end - word.start) * k / len(tokens),
                                               "end": word.start + (word.end - word.start) * (k + 1) / len(tokens)})
                aligned, coverage = align_words(part["text"], recognized, meta["duration"])
                make_captions(aligned, 0, meta["duration"])
                meta.update(words=aligned, alignment_coverage=coverage, alignment_device=device)
                metadata.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n")
            audio_parts.append({"src": str(audio.relative_to(PUBLIC)), "from": round(cursor * fps),
                                "duration": math.ceil(meta["duration"] * fps)})
            captions.extend(make_captions(meta["words"], cursor, meta["duration"]))
            words.extend({"text": word["text"], "time": cursor + word["start"]} for word in meta["words"])
            cursor += meta["duration"] + part.get("pause_after", data["tail_seconds"])
        duration = math.ceil(cursor * fps)
        marks = []
        for phrase in scene["beats"]:
            target = normalized_phrase(phrase).split()
            tokens = [normalized_phrase(word["text"]) for word in words]
            matches = [words[k]["time"] for k in range(len(words) - len(target) + 1)
                       if tokens[k:k + len(target)] == target]
            if not matches:
                raise ValueError(f"No timed beat for {phrase}")
            marks.append(matches[0])
        scenes.append({"index": i, "title": scene["title"], "visual": scene["visual"], "eyebrow": scene["eyebrow"],
                       "from": cursor_frames, "duration": duration, "audio": audio_parts,
                       "captions": captions, "marks": marks})
        all_cues.extend((cursor_frames / fps + cue["startMs"] / 1000,
                         cursor_frames / fps + cue["endMs"] / 1000, cue["text"]) for cue in captions)
        transcript.append(scene["title"] + "\n" + "\n".join(part["text"] for part in scene["parts"]))
        cursor_frames += duration
        print(f"ALIGNED {i + 1}: {duration / fps:.2f}s; marks {[round(mark, 1) for mark in marks]}", flush=True)
    timeline = {key: data[key] for key in ("id", "title", "voice", "model", "speed", "relative_speed", "fps", "sources", "evidence_date")}
    timeline.update(content_digest=hashlib.sha256((PROJECT / "content.json").read_bytes()).hexdigest(),
                    voice_region="Bắc", caption_font_size=30, caption_layers=1,
                    durationInFrames=cursor_frames, scenes=scenes)
    (PROJECT / "src").mkdir(exist_ok=True)
    (PROJECT / "src" / "timeline.json").write_text(json.dumps(timeline, ensure_ascii=False, indent=2) + "\n")
    (BUILD / "narration.txt").write_text("\n\n".join(transcript) + "\n")
    (BUILD / "captions.srt").write_text("\n\n".join(
        f"{i}\n{timestamp(start)} --> {timestamp(end)}\n{text}"
        for i, (start, end, text) in enumerate(all_cues, 1)) + "\n")
    chapters = [";FFMETADATA1", "title=HospitalLake — giới thiệu từ nền tảng", "artist=Nhóm HospitalLake"]
    for scene in scenes:
        chapters.extend(["[CHAPTER]", "TIMEBASE=1/1000", f"START={round(scene['from'] / fps * 1000)}",
                         f"END={round((scene['from'] + scene['duration']) / fps * 1000)}", f"title={scene['title']}"])
    (BUILD / "chapters.txt").write_text("\n".join(chapters) + "\n")
    print(f"READY: {cursor_frames / fps:.2f}s, {len(scenes)} scenes", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["voice", "align"])
    parser.add_argument("--limit", type=int, help="Generate only the first N scenes for voice preview")
    parser.add_argument("--regenerate", type=int, help="Regenerate only this 1-based scene after a failed speech check")
    parser.add_argument("--attempt", type=int, default=0, help="Deterministic alternate seed for speech regeneration")
    args = parser.parse_args()
    if args.limit is not None and (args.limit < 1 or args.phase != "voice"):
        parser.error("--limit is a positive voice-preview-only option")
    if args.phase == "align" and os.environ.get("VIDEO_ALIGN_DEVICE") == "cuda":
        # The NVIDIA wheels may carry cuDNN/cuBLAS outside the system loader path.
        directories = []
        for name in ("nvidia.cudnn", "nvidia.cublas"):
            try:
                spec = importlib.util.find_spec(name)
            except ModuleNotFoundError:
                spec = None
            if spec and spec.submodule_search_locations:
                directories.append(str(Path(next(iter(spec.submodule_search_locations))) / "lib"))
        current = os.environ.get("LD_LIBRARY_PATH", "").split(":")
        missing = [directory for directory in directories if directory not in current]
        if missing:
            env = dict(os.environ, LD_LIBRARY_PATH=":".join(missing + current))
            os.execve(sys.executable, [sys.executable, *sys.argv], env)
    BUILD.mkdir(parents=True, exist_ok=True)
    content = load_content()
    if args.regenerate is not None and (args.phase != "voice" or not 1 <= args.regenerate <= len(content["scenes"])):
        parser.error("--regenerate requires voice and a valid 1-based scene number")
    if args.phase == "voice":
        prepare_voice(content, args.limit, args.regenerate, args.attempt)
    else:
        prepare_alignment(content)
