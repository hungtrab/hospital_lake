import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from spark.batch.ingest_bronze import METADATA_TYPES, source_path, validate_schema


def schema(types):
    return SimpleNamespace(fields=[
        SimpleNamespace(name=name, dataType=SimpleNamespace(simpleString=lambda t=kind: t))
        for name, kind in types.items()
    ])


class BronzeContractTests(unittest.TestCase):
    def test_requires_one_unambiguous_parquet_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "patients"):
                source_path(root, "patients")
            file = root / "patients.parquet"
            file.touch()
            self.assertEqual(source_path(root, "patients"), file.resolve())
            file.unlink()
            (root / "patients").mkdir()
            self.assertEqual(source_path(root, "patients"), (root / "patients").resolve())
            file.touch()
            with self.assertRaisesRegex(ValueError, "exactly one"):
                source_path(root, "patients")

    def test_matching_schema_accepts_different_column_order(self):
        validate_schema(schema({"patient_id": "string"}),
                        schema({**METADATA_TYPES, "patient_id": "string"}), "patients_raw")

    def test_schema_drift_fails_instead_of_losing_or_casting_values(self):
        source = schema({"patient_id": "string", "birth_year": "int"})
        target = {"patient_id": "string", "birth_year": "int", **METADATA_TYPES}
        for change in ({"birth_year": "string"}, {"new_field": "string"},
                       {"_ingested_at": "string"}):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "mismatch"):
                validate_schema(source, schema({**target, **change}), "patients_raw")
        del target["birth_year"]
        with self.assertRaisesRegex(ValueError, "mismatch"):
            validate_schema(source, schema(target), "patients_raw")

    def test_reserved_metadata_cannot_overwrite_raw_columns(self):
        for name in METADATA_TYPES:
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "reserved"):
                validate_schema(schema({name.upper(): "string"}), schema({}), "patients_raw")

    def test_case_insensitive_duplicate_fields_fail(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_schema(schema({"patient_id": "string", "PATIENT_ID": "string"}),
                            schema({}), "patients_raw")


if __name__ == "__main__":
    unittest.main()
