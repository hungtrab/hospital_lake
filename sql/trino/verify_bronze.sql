-- Rendered with quoted identifiers and an escaped _batch_id by scripts/verify_bronze.py.
SELECT count(*) FROM {table} WHERE {where};
SELECT CAST({id} AS varchar) FROM {table} WHERE {where};
