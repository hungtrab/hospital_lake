"""Protect query identities and revisions from accidental checkpoint reset."""
from pathlib import Path
import tempfile
import unittest

from spark.streaming.clinical_metrics import main, runtime_identity


class ClinicalRuntimeTests(unittest.TestCase):
    def test_invalid_partition_count_fails_before_starting_spark(self):
        with self.assertRaises(SystemExit) as result:
            main(["--shuffle-partitions", "0"])
        self.assertEqual(result.exception.code, 2)

    def test_identity_reused_and_source_changes_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = runtime_identity(root, {"topic": "a"})
            self.assertEqual(runtime_identity(root, {"topic": "a"}), first)
            with self.assertRaisesRegex(ValueError, "source/settings changed"):
                runtime_identity(root, {"topic": "b"})

    def test_reset_checkpoint_or_missing_replay_buffer_fails(self):
        for name in ("accepted", "dedup", "lab", "icu"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                runtime_identity(root, {})
                target = root / ("buffers" if name in ("accepted", "dedup") else "output") / name
                if name in ("accepted", "dedup"):
                    target = target / "_spark_metadata"
                    target.mkdir(parents=True)
                    (target / "0").write_text("fixture")
                else:
                    target.mkdir(parents=True)
                with self.assertRaisesRegex(ValueError, "checkpoint removed"):
                    runtime_identity(root, {})
                (root / "checkpoints" / name / "offsets").mkdir(parents=True)
                self.assertIsInstance(runtime_identity(root, {}), dict)
                if name in ("accepted", "dedup"):
                    (target / "0").unlink()
                    target.rmdir()
                    with self.assertRaisesRegex(ValueError, "replay buffer removed"):
                        runtime_identity(root, {})

    def test_compacted_or_newer_logs_still_reject_a_removed_checkpoint(self):
        for name in ("accepted", "dedup", "quarantine"):
            for log_name in ("9.compact", "100"):
                with self.subTest(query=name, log=log_name), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    original = runtime_identity(root, {"topic": "fixture"})
                    metadata = root / ("buffers" if name in ("accepted", "dedup") else "output") / name / "_spark_metadata"
                    metadata.mkdir(parents=True)
                    (metadata / log_name).write_text("v1\nfixture-commit")
                    self.assertFalse((metadata / "0").exists())
                    # Normal restart is legal even after original logs disappear.
                    offsets = root / "checkpoints" / name / "offsets"
                    offsets.mkdir(parents=True)
                    self.assertEqual(runtime_identity(root, {"topic": "fixture"}), original)
                    offsets.rmdir()
                    with self.assertRaisesRegex(ValueError, "checkpoint removed for " + name):
                        runtime_identity(root, {"topic": "fixture"})
