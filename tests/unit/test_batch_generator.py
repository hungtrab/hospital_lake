import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from generators.batch_generator.generate import FIELDS, generate


class BatchGeneratorTests(unittest.TestCase):
    def test_counts_contracts_relationships_and_time_order(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(generate(directory), dict.fromkeys(FIELDS, 1000))
            tables = {}
            for table, fields in FIELDS.items():
                with (Path(directory) / f"{table}.csv").open(newline="") as handle:
                    reader = csv.DictReader(handle)
                    self.assertEqual(reader.fieldnames, list(fields))
                    tables[table] = list(reader)
                    self.assertEqual(len(tables[table]), 1000)
            patients = {row["patient_id"]: row for row in tables["patients"]}
            encounters = {row["encounter_id"]: row for row in tables["encounters"]}
            self.assertEqual(len(patients), 1000)
            self.assertEqual(len(encounters), 1000)
            self.assertEqual(len({r["lab_result_id"] for r in tables["lab_results"]}), 1000)
            for lab in tables["lab_results"]:
                encounter = encounters[lab["encounter_id"]]
                patient = patients[lab["patient_id"]]
                self.assertEqual(encounter["patient_id"], patient["patient_id"])
                times = [datetime.fromisoformat(value) for value in (
                    patient["created_at"], encounter["admission_time"],
                    lab["measured_at"], encounter["discharge_time"],
                )]
                self.assertEqual(times, sorted(times))
                self.assertTrue(all(t.utcoffset().total_seconds() == 0 for t in times))
                value = float(lab["result_value"])
                self.assertEqual(lab["is_abnormal"], str(value < 3.9 or value > 7.8))

    def test_seed_and_rerun_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            def contents():
                return [(Path(directory) / f"{table}.csv").read_bytes() for table in FIELDS]
            generate(directory, 20, seed=7)
            original = contents()
            generate(directory, 20, seed=7)
            self.assertEqual(original, contents())
            generate(directory, 20, seed=8)
            self.assertNotEqual(original, contents())
            generate(directory, 1, seed=7)
            with (Path(directory) / "patients.csv").open(newline="") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 1)

    def test_invalid_counts_do_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "invalid"
            for count in (0, -1, True, 1.5):
                with self.subTest(count=count), self.assertRaises(ValueError):
                    generate(output, count)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
