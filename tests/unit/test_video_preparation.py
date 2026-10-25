"""Fast checks for narration/caption failures, without importing speech models."""
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = spec_from_file_location("video_prepare", ROOT / "docs/video/prepare.py")
prepare = module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)


class VideoPreparationTests(unittest.TestCase):
    def test_missing_speech_cannot_be_silently_timed(self):
        with self.assertRaisesRegex(RuntimeError, "Speech alignment"):
            prepare.align_words("một câu cần được đọc đầy đủ", [], 3)

    def test_zero_time_whisper_words_do_not_make_invisible_captions(self):
        words = [{"text": "Bây giờ.", "start": 0, "end": .01},
                 {"text": "Câu tiếp.", "start": 0, "end": .2}]
        with self.assertRaisesRegex(ValueError, "no positive duration"):
            prepare.make_captions(words, .8, 3)

    def test_caption_text_keeps_punctuation_and_gaps(self):
        words = [{"text": "Giữ", "start": 0, "end": .2},
                 {"text": "gốc.", "start": .2, "end": .5},
                 {"text": "Tính", "start": 1.5, "end": 1.8},
                 {"text": "lại.", "start": 1.8, "end": 2}]
        cues = prepare.make_captions(words, .8, 2.2)
        self.assertEqual([cue["text"] for cue in cues], ["Giữ gốc.", "Tính lại."])
        self.assertLess(cues[0]["endMs"], cues[1]["startMs"])
        self.assertEqual(cues[0]["startMs"], 800)
        self.assertLessEqual(cues[-1]["endMs"], 3000)

    def test_long_caption_splits_without_losing_words(self):
        text = "HospitalLake giữ dữ liệu gốc rồi kiểm tra các mã liên kết trước khi tổng hợp theo khoa"
        tokens = text.split()
        words = [{"text": token, "start": i * .3, "end": i * .3 + .2} for i, token in enumerate(tokens)]
        cues = prepare.make_captions(words, 0, len(tokens) * .3)
        self.assertGreater(len(cues), 1)
        self.assertTrue(all(len(cue["text"]) <= 64 for cue in cues))
        self.assertEqual(" ".join(cue["text"] for cue in cues), text)
        self.assertTrue(all(a["endMs"] <= b["startMs"] for a, b in zip(cues, cues[1:])))

    def test_changed_narration_or_speed_invalidates_cache(self):
        content = prepare.load_content()
        with tempfile.TemporaryDirectory() as directory:
            audio = Path(directory) / "sample.mp3"
            metadata = audio.with_suffix(".json")
            audio.write_bytes(b"fixture")
            metadata.write_text(json.dumps({"digest": prepare.digest(content, "Câu cũ.")}))
            self.assertIsNotNone(prepare.read_cache(content, "Câu cũ.", audio, metadata))
            self.assertIsNone(prepare.read_cache(content, "Câu mới.", audio, metadata))
            self.assertIsNone(prepare.read_cache({**content, "speed": 1}, "Câu cũ.", audio, metadata))

    def test_unicode_script_alignment_preserves_authored_words(self):
        recognized = [{"text": "Bản", "start": 0, "end": .2},
                      {"text": "gốc", "start": .2, "end": .5},
                      {"text": "được", "start": .5, "end": .7},
                      {"text": "giữ", "start": .7, "end": .9}]
        words, coverage = prepare.align_words("Bản gốc được giữ.", recognized, 1)
        self.assertEqual(coverage, 1)
        self.assertEqual(" ".join(word["text"] for word in words), "Bản gốc được giữ.")
        self.assertTrue(all(0 <= word["start"] < word["end"] <= 1 for word in words))


if __name__ == "__main__":
    unittest.main()
