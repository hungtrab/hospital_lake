# Generated hospital source data — TV1 Weeks 1–2

The batch generator implements all six synthetic entities from `plan.md` §5.
These are raw source files for future Bronze ingestion, not Iceberg tables.

## Source schemas

| File | Primary key | Foreign keys | Other fields |
|---|---|---|---|
| `patients.parquet` | `patient_id` | — | `gender`, `birth_year`, `city`, `created_at` |
| `encounters.parquet` | `encounter_id` | `patient_id` | `encounter_type`, `department`, `admission_time`, `discharge_time`, `status` |
| `lab_results.parquet` | `lab_result_id` | `patient_id`, `encounter_id` | `test_code`, `test_name`, `result_value`, `unit`, `reference_low`, `reference_high`, `is_abnormal`, `measured_at` |
| `medications.parquet` | `medication_id` | `patient_id`, `encounter_id` | `drug_name`, `dose`, `dose_unit`, `route`, `start_time`, `end_time` |
| `vitals.parquet` | `measurement_id` | `patient_id`, `encounter_id` | `heart_rate`, `spo2`, `systolic_bp`, `diastolic_bp`, `temperature`, `measured_at` |
| `bed_events.parquet` | `event_id` | `patient_id`, `encounter_id` | `department`, `bed_id`, `event_type`, `event_time` |

The executable Arrow schemas live in `generators/batch_generator/schemas.py`.
All IDs are non-null strings. All timestamps use `timestamp[us, tz=UTC]`;
CSV uses ISO timestamps with UTC offsets. Vital units are bpm, percent, mmHg
and Celsius. Labs and medication orders carry their own units.

Patients use `P00000001` IDs, encounters `E000000001`, labs `L0000000001`.
Medication, vital and bed event IDs use the encounter number plus an ordinal:
`M000000001-01`, `V000000001-01`, `B000000001-01`. These IDs are deterministic
for the same seed/config. Each lab/order/measurement/bed event references the
same patient as its encounter. Additional entity RNGs are independent, so
changing medication/vital/transfer settings leaves core patients/encounters/labs
unchanged.

## Journey and defect semantics

Clean patient encounters do not overlap. Labs, medications and vitals fall
inside their encounter, using the configured end date as the cutoff for open
encounters. Medication end time may be null for open encounters.

Non-outpatient encounters emit an assigned bed at admission, an optional
within-department transfer, and a released bed at discharge. Transfers close
the old assignment and name the new bed. Open encounters have no release.
Bed IDs are scoped to encounters to prevent double booking. This dataset does
not model a fixed physical bed inventory; analytical bed capacities are a
separate serving/Gold configuration.

Raw defects are injected after clean child records are generated. Rates in
`config.yaml` control exact encounter/lab duplicates, department aliases and
discharge times earlier than admission. Foreign keys remain valid. Therefore
raw child event times need not fit an intentionally invalid raw discharge;
Silver must quarantine the invalid encounter instead of treating its times
as clean. `_manifest.json` records the counts of injected defects.

## Commands and downstream compatibility

```bash
make setup
make generate-data PATIENTS=1000 SEED=42
make verify-generated INPUT_DIR=data/generated
make validate-generator SCALES="10000 100000"
make test
```

`make generate-data` defaults to Parquet. CSV remains available through
`FORMAT=csv` or `--format csv`; use a separate output directory when switching
format. The generator rejects mixed source formats before writing, because
TV4's Bronze verifier requires exactly one file per entity.

The `generate(output_dir, patients, seed)` Python API writes clean CSV through
the same generator and returns actual row counts. Migration from the initial
CSV fixture: IDs change from `patient-...`/`encounter-...`/`lab-...` to the stable
formats above; encounter/lab counts vary by journey instead of being one per
patient. Consumers must read actual IDs and manifest counts. `schemas.py` is
the source schema contract. Bronze ingestion adds its own `_batch_id` and other
ingestion metadata; these source files do not add them prematurely.

`verify-generated` checks schemas, manifest row counts, non-empty keys, duplicate
counts, and all patient/encounter relationships in actual Parquet files.
`validate-generator` executes the CLI twice at each scale, performs these checks
on both outputs, and compares all file hashes and manifest bytes. It records
results in `results/generator-validation.json` (git ignored).

## Local generation measurements, 2026-10-11

Seed 42, default configuration, all six entities. The following single-run
measurements were collected with GNU time before the repeatability checks:

| Patients | Encounters | Labs | Medications | Vitals | Bed events | Process seconds | Peak RSS KiB |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 30,493 | 127,294 | 60,403 | 91,130 | 38,593 | 2.56 | 250,448 |
| 100,000 | 302,560 | 1,268,448 | 599,241 | 898,373 | 384,346 | 23.50 | 1,282,468 |

Generation currently materializes the dataset in memory. Normal development
still defaults to 1,000 patients; larger sizes require proportionally more
memory. These measurements cover local source generation, not distributed
Spark throughput or the Week 8 benchmark milestone. A reproducible timing command:

```bash
/usr/bin/time -f 'elapsed_seconds=%e max_rss_kib=%M' \
  python3 -m generators.batch_generator.generate \
  --patients 100000 --seed 42 --output-dir data/generated/scale100k
```

The live Week 2 gate (Parquet → Spark Bronze → Trino verification) remains
pending ingestion implementation. The local REST catalog and Bronze DDL are now available.


## Week 2 Bronze Iceberg contracts (TV2)

`sql/iceberg/create_bronze_tables.sql` defines the six typed raw tables with the
same source columns as the Arrow contracts above. Domain columns are nullable,
including IDs, so representable bad rows can be preserved for Silver validation.
No primary-key uniqueness or foreign-key business validation runs in Bronze.
The same timestamp values are stored as Iceberg `timestamptz` (Spark
`TIMESTAMP_LTZ`, UTC session); already-typed Parquet retains microsecond precision.
Malformed lexical CSV values cannot be preserved in a typed timestamp column:
TV3 ingestion must retain the original rejected record/reason in quarantine.

| Table | Hidden partition | Reason |
|---|---|---|
| `patients_raw` | None | Dimension-like raw input; avoids sparse partitions on registration dates |
| `encounters_raw` | `days(admission_time)` | Batch date scans without splitting on unnormalized department aliases |
| `lab_results_raw` | `days(measured_at)` | Historical date scans |
| `medications_raw` | `days(start_time)` | Historical date scans |
| `vitals_raw` | `days(measured_at)` | Bronze replay is batch-oriented; daily granularity limits fragmentation compared with hourly partitions |
| `bed_events_raw` | `days(event_time)` | Event-date replay |

These are starting access-pattern choices, not measured performance improvements.
The plan's hourly vital suggestion is more appropriate if measured sub-day access
later justifies it. No table partitions by patient/event ID. `bronze-smoke`
reports actual file/partition counts and average bytes on its small fixtures;
the files are expected to be small at this scale. Later benchmarks/maintenance
must evaluate real demo-scale behavior before claiming an improvement.
All tables use Iceberg format v2, Parquet files and a 128 MiB target file size;
the target does not guarantee that a small input creates large files.

### Ingestion handoff

TV3 supplies the five extra fields when appending source records:

| Metadata | Type / nullability | Meaning |
|---|---|---|
| `_source` | STRING, required | Traceable source identifier |
| `_ingested_at` | TIMESTAMP_LTZ, required | UTC ingestion time, distinct from event time |
| `_batch_id` | STRING, required | Stable batch/retry identity used by TV4 verification |
| `_event_id` | STRING, optional | Stable event ID where available; preserve bed `event_id` separately |
| `_schema_version` | INT, required | Source contract version, currently 1 |

Run `make bootstrap-bronze` before ingestion. The common
`spark/common/iceberg.py` extracts the previously used Week 1 SparkSession setup,
so bootstrap and fixture jobs reuse the same REST/S3 configuration. Connector
JARs are still provided by the Docker Spark submit command. `_batch_id` alone
does not make appends idempotent: retry logic remains the ingestion job's task.
Existing table drift fails visibly; apply a reviewed explicit schema/partition
migration rather than dropping, replacing or silently altering a table.

`make bronze-smoke PYTHON=.venv/bin/python` validates these contracts in an
isolated namespace, including duplicates, invalid raw vital values, full Spark
row equality, Trino ID checksums, MinIO metadata existence, safe bootstrap rerun
and refusal of incompatible tables. It is a fixture check, not the production
Week 2 batch gate. Canonical tables stay available for TV3/TV4 after the check.
