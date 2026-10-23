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

## Week 3 Silver: patients and encounters (TV1)

`spark/batch/bronze_to_silver.py` rebuilds Silver from **all** Bronze rows on
every run (Iceberg overwrite), so reruns and re-ingested Bronze batches give the
same Silver rows. DDL lives in `sql/iceberg/create_silver_tables.sql` and is
applied by the job itself (`CREATE ... IF NOT EXISTS`).

| Table | Key | Columns | Partition |
|---|---|---|---|
| `silver.patients` | `patient_id` | `gender` (`M/F/O`), `birth_year`, `city` (trimmed, empty → null), `created_at` | none |
| `silver.encounters` | `encounter_id` | `patient_id`, `encounter_type` (lower case), `department` (canonical), `admission_time`, `discharge_time` (null = still admitted), `status` (lower case), `length_of_stay_hours` (null when open) | `months(admission_time)` |
| `silver.quarantine` | — | `entity`, `record_key`, `reason_codes`, `raw_record` (JSON of source columns), `_bronze_batch_id`, `_processed_at` | `entity` |

Every Silver table also has `_bronze_batch_id` (earliest Bronze batch holding
the record) and `_processed_at` (run time). Monthly encounter partitions keep
the ~20-month demo history at ~20 files instead of hundreds of daily files.

Processing order per entity:

1. Exact duplicates (all source columns equal, Bronze metadata ignored) collapse
   to one row. This absorbs the generator's injected duplicates and the same
   file ingested under two batch IDs.
2. Values are normalized: department spellings map to the five canonical names
   through `spark/common/normalization.py` (`ER`, `ED`, `Emergency Department`
   → `EMERGENCY`; `Cardio` → `CARDIOLOGY`; ...).
3. `spark/common/data_quality.py` applies required columns and rules. Rows that
   share a key but differ in value are all quarantined (`DUPLICATE_KEY`).

| Entity | Reason codes (besides `MISSING_<COLUMN>` and `DUPLICATE_KEY`) |
|---|---|
| patients | `INVALID_GENDER`, `IMPLAUSIBLE_BIRTH_YEAR` (outside 1900..registration year) |
| encounters | `UNKNOWN_PATIENT` (not in valid `silver.patients`), `UNKNOWN_DEPARTMENT`, `INVALID_ENCOUNTER_TYPE`, `INVALID_STATUS`, `DISCHARGE_BEFORE_ADMISSION`, `STATUS_DISCHARGE_MISMATCH` |

Child entities (labs, medications, vitals, bed events) must check their
`encounter_id` against valid `silver.encounters`: an encounter quarantined here
makes its children unknown as well.

```bash
make batch-silver-docker                       # Compose Spark, all registered entities
make batch-silver-docker SILVER_ENTITIES=patients
```

`--entities` (or `SILVER_ENTITIES`) limits a run but does not rebuild
dependents; run all entities (the default) after upstream changes.

Each entity logs one JSON line: `run_id`, `bronze_rows`, `exact_duplicate_rows`,
`input_rows`, `invalid_rows`, `duplicate_key_rows`, `output_rows`,
`quarantine_rows`, `duration_seconds`.

Live run on 2026-10-22 (1,000 patients, seed 42, Bronze batch `checkpoint-001`):
patients 1,000 → 1,000; encounters 3,094 Bronze rows → 31 exact duplicates
collapsed (manifest: 31 injected) → 3,058 Silver rows + 5 quarantined
`DISCHARGE_BEFORE_ADMISSION` (manifest: 5 injected). All 140 injected
department aliases resolved to canonical names. A second run produced identical
counts.

### One-command batch path

```bash
make batch PYTHON=.venv/bin/python [PATIENTS=1000 SEED=42]
```

runs `generate-data` → `bootstrap-bronze` → `batch-bronze-docker` →
`verify-bronze` → `batch-silver-docker` and stops at the first failing step.
The Bronze batch ID defaults to `synthetic-p<PATIENTS>-s<SEED>`; the same
inputs regenerate identical files, so a rerun skips already committed Bronze
tables. Gold joins this target with TV4's `gold-docker` (next_step.md T4.1).

Live, 2026-10-22: the stack already held the same dataset as Bronze batch
`checkpoint-001`; `make batch` added it again as `synthetic-p1000-s42` (Bronze
now 2,000 patient and 6,188 encounter rows). Silver still produced 1,000
patients and 3,058 encounters with 5 quarantined rows: 1,000 and 3,125 exact
duplicates collapsed. A second `make batch` reported `skipped` for all six
Bronze tables and the same Silver counts.

## Week 4 realtime events (TV1)

`generators/stream_generator/` simulates live hospital activity and publishes
JSON envelopes to Kafka (default topic `hospital.events`, key = `patient_id`):

```json
{"event_id": "<uuid4>", "event_type": "ADMISSION", "event_time": "2026-10-22T10:00:00.000Z",
 "source": "hospital-simulator", "version": 1,
 "payload": {"patient_id": "PS00000001", "encounter_id": "ES000000001",
             "encounter_type": "inpatient", "department": "SURGERY"}}
```

| Event type | Payload fields (all carry `patient_id`; encounter events carry `encounter_id`) |
|---|---|
| `PATIENT_REGISTERED` | `gender`, `birth_year`, `city` |
| `ADMISSION` | `encounter_type`, `department` (canonical) |
| `BED_ASSIGNED`, `BED_RELEASED` | `department`, `bed_id` |
| `TRANSFER` | `department`, `from_bed_id`, `to_bed_id` |
| `LAB_RESULT` | `test_code`, `test_name`, `result_value`, `unit`, `reference_low`, `reference_high`, `is_abnormal` |
| `VITAL_MEASUREMENT` | `heart_rate`, `spo2`, `systolic_bp`, `diastolic_bp`, `temperature` |
| `MEDICATION_ORDER` | `drug_name`, `dose`, `dose_unit`, `route` |
| `DISCHARGE` | `department` |

The admission/discharge time is the envelope `event_time` (UTC, wall clock).
Stream IDs use their own prefixes (`PS…`, `ES…`) so they never collide with the
batch dataset. Every producer start gets a run ID (UTC start time, e.g.
`PS261022133000-00000001`); event UUIDs are mixed with it too, so a restarted
producer never replays earlier IDs that the stream would drop as duplicates or
treat as re-admissions. `--run-id` reproduces a run exactly (same seed + run ID
⇒ same messages). Per encounter the order is `ADMISSION` → `BED_ASSIGNED` (non
outpatient) → clinical events / `TRANSFER` → `DISCHARGE` → `BED_RELEASED`; new
patients are registered first, and a returning patient is only re-admitted
after discharge. At most `max_active_encounters` encounters are open.

Defects (rates in `config.yaml`, separate RNG so clean content is unchanged):
late events (`event_time` 11–30 minutes old, beyond the 10-minute watermark),
exact duplicate messages, and invalid messages (truncated JSON, missing
`event_id`, unknown event type, unparseable `event_time`, non-object payload).
A unit test feeds generated messages through TV3's `parse_kafka_events` and
`deduplicate_events`: only invalid messages are rejected and duplicates collapse.

```bash
make produce-dry-run MAX_EVENTS=5                                 # JSON lines, no Kafka
make produce KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RATE=1000 DURATION=60
```

The producer logs one JSON stats line every 10 seconds and at exit
(`events_sent`, `events_per_second`, `valid/late/duplicate/invalid_events`).
`events_per_second` is the send rate; buffered records are flushed at exit, so
end-to-end delivery can finish slightly later. Delivery failures (after the
client's retries) are counted and make the producer exit with status 1; Ctrl-C
flushes, logs final stats and exits normally.
Local measurement, 2026-10-22, against a temporary single-node Kafka 3.9.1
container (3 partitions) on the same 16-core machine: target 1,000/s for 10 s →
10,000 messages consumed, every key on a single partition; target 2,000/s → 20,000 in
10.0 s; target 10,000/s → 100,000 in 10.4 s (9,615/s, one Python process).
These are producer-only numbers, not end-to-end stream throughput.

## Week 3 clinical Silver (TV2, T2.1)

The existing entity registry now rebuilds `lab_results`, `bed_events`,
`medications`, and `vitals` after `encounters`. The default
`make batch-silver-docker` (also called by `make batch`) processes all six
entities. To rebuild only clinical tables against already rebuilt encounters:

```bash
make batch-silver-docker SILVER_ENTITIES="lab_results bed_events medications vitals"
```

Each clinical table keeps its Bronze domain columns/types plus
`_bronze_batch_id` and `_processed_at`. Labs recompute `is_abnormal`:
values strictly below/above the reference interval are abnormal; endpoints
are normal. The source flag is ignored. Missing/nonfinite values or reversed
reference bounds are quarantined. Reference intervals and units remain those
of the source test; no cross-test unit conversion is inferred.

All four check `encounter_id` against **valid Silver encounters**, not Bronze.
Children of rejected encounters receive `UNKNOWN_ENCOUNTER`. A known encounter
with a different patient receives `ENCOUNTER_PATIENT_MISMATCH`. Required IDs,
names/times and entity-specific fields are checked; raw rejected values remain
in `silver.quarantine` with reasons. Exact duplicates collapse and conflicting
duplicate keys quarantine (including invalid variants) using the existing shared framework.

| Entity | Additional rules / normalization |
|---|---|
| `lab_results` | `INVALID_LAB_VALUE`, `INVALID_REFERENCE_RANGE`; recomputed boolean is directly consumed by Gold |
| `bed_events` | Canonical department aliases; unknown departments → `UNKNOWN_DEPARTMENT`; event type trimmed/lowercase, only `assigned/transferred/released` (`INVALID_BED_EVENT_TYPE`) |
| `medications` | Finite positive dose (`INVALID_DOSE`); optional end time >= start (`END_BEFORE_START`); trim drug/unit, lowercase route; dose units retain case (`mL`) |
| `vitals` | Positive heart rate; finite SpO2 in [0,100]; positive BP with systolic >= diastolic; finite temperature in [25,45] Celsius. Failures receive `INVALID_HEART_RATE`, `INVALID_SPO2`, `INVALID_BLOOD_PRESSURE`, `INVALID_TEMPERATURE` respectively. Missing measurements fail these rules. |

Temperature bounds are broad synthetic-data validation bounds, not medical
recommendations. Source units are bpm, percent, mmHg and Celsius; values are
not rescaled. Bronze source types already enforce typed UTC timestamps.

Clinical tables use monthly hidden partitions on `measured_at` (labs/vitals),
`event_time` (beds), and `start_time` (medications), following the existing Silver
encounter choice. This deliberately coarsens the starting day/hour suggestions
in `plan.md`: sparse development data spans ~20 months. Live file/partition
counts should be measured before changing this strategy for larger workloads.
Full recomputation replaces data; it does not migrate existing table schemas.

Key conflicts are detected before excluding invalid rows: both variants receive
`DUPLICATE_KEY`, with invalid variants retaining their specific reasons. Metrics
remain disjoint: `invalid_rows` includes invalid conflicting rows, while
`duplicate_key_rows` counts the remaining otherwise-valid conflicting rows.

### Clinical validation evidence (local, 2026-10-24)

Dataset: 1,000 synthetic patients, seed 42, batch
`tv2-clinical-check-20261024`. The local `make batch` path passed generation,
Bronze bootstrap/ingestion/Trino ID checks and all six Silver transforms using
existing pinned Week 1 Docker images (validation-only Compose override of
`pull_policy` to `never`; no infrastructure source changes). This is local
integration evidence, not certification of a fresh-clone checkpoint deploy.

| Clinical entity | Bronze rows | Exact duplicates collapsed | Silver rows | Quarantined |
|---|---:|---:|---:|---:|
| lab_results | 12,839 | 108 | 12,719 | 12 |
| bed_events | 3,938 | 0 | 3,934 | 4 |
| medications | 6,149 | 0 | 6,139 | 10 |
| vitals | 9,261 | 0 | 9,248 | 13 |

All 39 rejected clinical rows reference one of the five rejected encounters
(`UNKNOWN_ENCOUNTER`), demonstrating parent-quarantine propagation. Independent
Trino checks found no duplicate IDs, broken FK/patient pairs, wrong lab flags,
invalid bed enums/departments, medication dose/time errors or invalid vital
values in Silver. Boundary, missing and nonfinite-value cases are additionally
covered by `tests/unit/test_silver_clinical.py`.

All four tables had 20 current files and 20 monthly partitions. Mean file bytes:
lab_results 16,224.9; bed_events 7,110.5; medications 10,471.45; vitals 12,518.85.
These are tiny demonstration files, not a performance benchmark or a target
file-size guarantee. Larger scales need new measurements before repartitioning.

A read-only run of the existing Gold builder consumed actual Silver inputs and
matched 2,864 abnormal labs and 268 bed transfers in hourly metrics; daily lab
totals matched all 12,719 Silver labs at cutoff `2026-10-24T00:00:00Z`.
It did not exercise Gold table writes or certify `make query`.

Full regression with Java 21 / pinned host PySpark 4.0.2: UTC **139 passed,
10 skipped, 81 subtests**. At `TZ=Asia/Ho_Chi_Minh`: **138 passed, 10 skipped,
81 subtests**, with the one previously documented TV4 Gold hourly timestamp
failure (`test_gold_metrics.py::test_hourly_and_icu_metrics`, `next_step.md` T4.1).
All new clinical/shared-quality regressions passed in both timezones. The skipped
checks are opt-in live integration tests; the relevant REST/S3/Trino path was
exercised separately above with Compose Spark 3.5 / Python 3.8.

A second clinical rebuild retained identical counts and order-insensitive
Trino full-row checksums for all six Silver tables and the complete quarantine
table, excluding only `_processed_at`. Each clinical quarantine partition was
replaced without accumulating rows or changing upstream entity partitions.
