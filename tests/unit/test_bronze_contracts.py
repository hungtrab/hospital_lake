"""Bronze must remain compatible with actual generator and serving contracts."""

from pathlib import Path
import tempfile
import unittest

import pyarrow as pa

from generators.batch_generator.schemas import ENTITY_SCHEMAS
from scripts.verify_bronze import ENTITY_IDS
from spark.common.bronze import DDL_PATH, load_contracts


class BronzeContractTests(unittest.TestCase):
    def test_source_columns_types_and_ids_match_generator(self):
        mapping = {pa.string(): "STRING", pa.int32(): "INT", pa.float64(): "DOUBLE",
                   pa.bool_(): "BOOLEAN", pa.timestamp("us", tz="UTC"): "TIMESTAMP_LTZ"}
        contracts = {table.name[:-4]: table for table in load_contracts()}
        self.assertEqual(set(contracts), set(ENTITY_SCHEMAS))
        for entity, schema in ENTITY_SCHEMAS.items():
            with self.subTest(entity=entity):
                fields = [line.strip().split() for line in contracts[entity].columns.split(",")]
                domain = [(field[0], field[1]) for field in fields if not field[0].startswith("_")]
                self.assertEqual(domain, [(field.name, mapping[field.type]) for field in schema])
                self.assertIn(ENTITY_IDS[entity], dict(domain))
                self.assertTrue(all(len(field) == 2 for field in fields if not field[0].startswith("_")))

    def test_ingestion_metadata_and_safe_table_properties(self):
        for contract in load_contracts():
            with self.subTest(table=contract.name):
                fields = [line.strip().split() for line in contract.columns.split(",")]
                metadata = {field[0]: " ".join(field[1:]) for field in fields if field[0].startswith("_")}
                self.assertEqual(metadata, {"_source": "STRING NOT NULL", "_ingested_at": "TIMESTAMP_LTZ NOT NULL",
                                            "_batch_id": "STRING NOT NULL", "_event_id": "STRING",
                                            "_schema_version": "INT NOT NULL"})
                self.assertEqual(contract.properties["format-version"], "2")
                self.assertEqual(contract.properties["write.format.default"], "parquet")
                self.assertTrue(all(column not in ("patient_id", "event_id", "measurement_id")
                                    for column, _ in contract.partitions))

    def test_loader_rejects_missing_or_unsupported_contracts(self):
        text = DDL_PATH.read_text()
        for modified in (text.split("CREATE TABLE IF NOT EXISTS iceberg.bronze.bed_events_raw")[0],
                         text.replace("days(admission_time)", "bucket(1000, patient_id)")):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "invalid.sql"
                path.write_text(modified)
                with self.assertRaises(ValueError):
                    load_contracts(path)
