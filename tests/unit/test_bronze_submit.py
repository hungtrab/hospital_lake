"""Exercise the Make/Compose boundary without requiring Docker services."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class BronzeComposeSubmitTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.arguments = root / "arguments"
        self.input = root / "input"
        self.compose = root / "compose"
        self.compose.write_text(
            '#!/bin/sh\n'
            'printf \'%s\\n\' "$@" > "$BRONZE_TEST_ARGV"\n'
            'printf \'%s\' "$BRONZE_INPUT_DIR" > "$BRONZE_TEST_INPUT"\n'
        )
        self.compose.chmod(0o755)
        self.env = {**os.environ, "BRONZE_TEST_ARGV": str(self.arguments),
                    "BRONZE_TEST_INPUT": str(self.input)}
        # Keep tests independent of shell/CI overrides of Make variables.
        for name in ("MAKEFLAGS", "MFLAGS", "BATCH_ID"):
            self.env.pop(name, None)

    def submit(self, *variables):
        return subprocess.run(["make", "--no-print-directory", "batch-bronze-docker",
                               f"COMPOSE={self.compose}", *variables],
                              cwd=ROOT, env=self.env, capture_output=True, text=True)

    def test_forwards_selected_batch_entities_and_external_path_as_separate_arguments(self):
        source = str(Path(self.directory.name) / "input with spaces")
        result = self.submit("BATCH_ID=batch with spaces", f"INPUT_DIR={source}",
                             "ENTITIES=patients encounters")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.arguments.read_text().splitlines(), [
            "run", "--rm", "bronze-ingest", "--input-dir", "/opt/hospital-input",
            "--batch-id", "batch with spaces", "--entities", "patients", "encounters",
        ])
        self.assertEqual(self.input.read_text(), source)

    def test_missing_batch_id_fails_before_starting_compose(self):
        result = self.submit("BATCH_ID=")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BATCH_ID is required", result.stderr)
        self.assertFalse(self.arguments.exists())


if __name__ == "__main__":
    unittest.main()
