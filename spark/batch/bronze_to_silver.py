"""Bronze -> Silver: full recomputation of validated, normalized entity tables.

Run from the repo root: python -m spark.batch.bronze_to_silver [--entities patients encounters]

Each run rebuilds Silver from everything in Bronze, so reruns and re-ingested
Bronze batches give the same Silver rows. Per entity:

1. exact duplicates (identical source columns, e.g. the same file ingested in
   two Bronze batches) collapse to one row, keeping the earliest ``_batch_id``;
2. ``prepare`` normalizes values and adds ``_chk_*`` helper columns
   (foreign-key lookups, canonical values);
3. ``spark/common/data_quality.assess_dataframe`` applies required columns and
   rules; conflicting rows that share a key are quarantined, never resolved;
4. valid rows are projected by ``output`` and overwrite ``silver.<entity>``;
   rejected rows overwrite this entity's partition of ``silver.quarantine``.

Adding an entity (e.g. lab_results): write ``prepare``/``rules``/``output``
functions, append a ``SilverEntity`` to ``ENTITIES`` after its dependencies,
and add its DDL to ``sql/iceberg/create_silver_tables.sql``. ``prepare``
receives already-built Silver frames for ``depends_on`` entities.

``--entities`` limits a run to some entities but does not rebuild their
dependents: after rebuilding ``patients`` alone, ``encounters`` still reflects
the previous patient set. Run all entities (the default) after upstream changes.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Callable, Dict, Mapping, Sequence, Tuple

from spark.common.data_quality import REASON_COLUMN, assess_dataframe
from spark.common.normalization import (
    ENCOUNTER_STATUSES,
    ENCOUNTER_TYPES,
    GENDERS,
    canonical_department,
)

LOGGER = logging.getLogger(__name__)
DDL_PATH = Path(__file__).resolve().parents[2] / "sql" / "iceberg" / "create_silver_tables.sql"
BRONZE_METADATA = ("_source", "_ingested_at", "_batch_id", "_event_id", "_schema_version")
HELPER_PREFIX = "_chk_"
QUARANTINE_TABLE = "quarantine"


@dataclass(frozen=True)
class SilverEntity:
    name: str
    key: str
    required: Tuple[str, ...]
    prepare: Callable  # (frame, silver: Mapping[str, DataFrame]) -> DataFrame
    rules: Callable  # (frame) -> Dict[reason_code, boolean Column that must be true]
    output: Callable  # (frame) -> list of output Columns, in DDL order
    depends_on: Tuple[str, ...] = ()

    @property
    def bronze_table(self) -> str:
        return f"{self.name}_raw"


@dataclass(frozen=True)
class SilverResult:
    valid: object
    quarantine: object
    report: dict


# --- patients ----------------------------------------------------------------

def _patients_prepare(frame, silver):
    return frame


def _patients_rules(frame):
    from pyspark.sql import functions as F

    return {
        "INVALID_GENDER": F.upper(F.trim("gender")).isin(*GENDERS),
        "IMPLAUSIBLE_BIRTH_YEAR": F.col("birth_year").between(F.lit(1900), F.year("created_at")),
    }


def _patients_output(frame):
    from pyspark.sql import functions as F

    city = F.trim("city")
    return [
        F.col("patient_id"),
        F.upper(F.trim("gender")).alias("gender"),
        F.col("birth_year"),
        F.when(city != "", city).alias("city"),
        F.col("created_at"),
    ]


# --- encounters --------------------------------------------------------------

def _encounters_prepare(frame, silver):
    from pyspark.sql import functions as F

    known = silver["patients"].select("patient_id").distinct().withColumn("_chk_patient_known", F.lit(True))
    return (frame.join(known, "patient_id", "left")
            .withColumn("_chk_department", canonical_department(F.col("department"))))


def _encounters_rules(frame):
    from pyspark.sql import functions as F

    status = F.lower(F.trim("status"))
    discharge = F.col("discharge_time")
    return {
        "UNKNOWN_PATIENT": F.col("_chk_patient_known"),
        "UNKNOWN_DEPARTMENT": F.col("_chk_department").isNotNull(),
        "INVALID_ENCOUNTER_TYPE": F.lower(F.trim("encounter_type")).isin(*ENCOUNTER_TYPES),
        "INVALID_STATUS": status.isin(*ENCOUNTER_STATUSES),
        "DISCHARGE_BEFORE_ADMISSION": discharge.isNull() | (discharge >= F.col("admission_time")),
        "STATUS_DISCHARGE_MISMATCH": (((status == "discharged") & discharge.isNotNull())
                                      | ((status == "admitted") & discharge.isNull())),
    }


def _encounters_output(frame):
    from pyspark.sql import functions as F

    stay_seconds = F.unix_seconds("discharge_time") - F.unix_seconds("admission_time")
    return [
        F.col("encounter_id"),
        F.col("patient_id"),
        F.lower(F.trim("encounter_type")).alias("encounter_type"),
        F.col("_chk_department").alias("department"),
        F.col("admission_time"),
        F.col("discharge_time"),
        F.lower(F.trim("status")).alias("status"),
        (stay_seconds / F.lit(3600.0)).alias("length_of_stay_hours"),
    ]


# --- clinical entities (TV2) --------------------------------------------------

CLINICAL_COLUMNS = {
    "lab_results": ("lab_result_id", "patient_id", "encounter_id", "test_code", "test_name",
                    "result_value", "unit", "reference_low", "reference_high", "is_abnormal", "measured_at"),
    "bed_events": ("event_id", "patient_id", "encounter_id", "department", "bed_id", "event_type", "event_time"),
    "medications": ("medication_id", "patient_id", "encounter_id", "drug_name", "dose", "dose_unit",
                    "route", "start_time", "end_time"),
    "vitals": ("measurement_id", "patient_id", "encounter_id", "heart_rate", "spo2", "systolic_bp",
               "diastolic_bp", "temperature", "measured_at"),
}


def _clinical_prepare(frame, silver):
    from pyspark.sql import functions as F

    known = silver["encounters"].select(
        "encounter_id", F.col("patient_id").alias("_chk_encounter_patient"),
    ).withColumn("_chk_encounter_known", F.lit(True))
    return frame.join(known, "encounter_id", "left")


def _clinical_rules(frame):
    from pyspark.sql import functions as F

    return {
        "UNKNOWN_ENCOUNTER": F.col("_chk_encounter_known"),
        "ENCOUNTER_PATIENT_MISMATCH": (F.col("_chk_encounter_known").isNull()
                                       | (F.col("patient_id") == F.col("_chk_encounter_patient"))),
    }


def _finite(name):
    from pyspark.sql import functions as F

    value = F.col(name)
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))


def _lab_rules(frame):
    from pyspark.sql import functions as F

    return dict(_clinical_rules(frame),
                INVALID_LAB_VALUE=_finite("result_value"),
                INVALID_REFERENCE_RANGE=(_finite("reference_low") & _finite("reference_high")
                                         & (F.col("reference_low") <= F.col("reference_high"))))


def _lab_output(frame):
    from pyspark.sql import functions as F

    abnormal = ((F.col("result_value") < F.col("reference_low"))
                | (F.col("result_value") > F.col("reference_high")))
    return [abnormal.alias(name) if name == "is_abnormal" else F.col(name)
            for name in CLINICAL_COLUMNS["lab_results"]]


def _bed_prepare(frame, silver):
    from pyspark.sql import functions as F

    return _clinical_prepare(frame, silver).withColumn("_chk_department", canonical_department(F.col("department")))


def _bed_rules(frame):
    from pyspark.sql import functions as F

    return dict(_clinical_rules(frame),
                UNKNOWN_DEPARTMENT=F.col("_chk_department").isNotNull(),
                INVALID_BED_EVENT_TYPE=F.lower(F.trim("event_type")).isin("assigned", "transferred", "released"))


def _bed_output(frame):
    from pyspark.sql import functions as F

    return [F.col("_chk_department").alias("department") if name == "department"
            else F.lower(F.trim(name)).alias(name) if name == "event_type" else F.col(name)
            for name in CLINICAL_COLUMNS["bed_events"]]


def _medication_rules(frame):
    from pyspark.sql import functions as F

    return dict(_clinical_rules(frame), INVALID_DOSE=_finite("dose") & (F.col("dose") > 0),
                END_BEFORE_START=F.col("end_time").isNull() | (F.col("end_time") >= F.col("start_time")))


def _medication_output(frame):
    from pyspark.sql import functions as F

    return [F.lower(F.trim(name)).alias(name) if name == "route"
            else F.trim(name).alias(name) if name in ("drug_name", "dose_unit") else F.col(name)
            for name in CLINICAL_COLUMNS["medications"]]


def _vital_rules(frame):
    from pyspark.sql import functions as F

    # Synthetic-data quality bounds, not clinical advice; preserve the same units.
    return dict(_clinical_rules(frame),
                INVALID_HEART_RATE=F.col("heart_rate") > 0,
                INVALID_SPO2=_finite("spo2") & F.col("spo2").between(0.0, 100.0),
                INVALID_BLOOD_PRESSURE=((F.col("systolic_bp") > 0) & (F.col("diastolic_bp") > 0)
                                        & (F.col("systolic_bp") >= F.col("diastolic_bp"))),
                INVALID_TEMPERATURE=_finite("temperature") & F.col("temperature").between(25.0, 45.0))


def _vital_output(frame):
    from pyspark.sql import functions as F

    return [F.col(name) for name in CLINICAL_COLUMNS["vitals"]]


ENTITIES: Tuple[SilverEntity, ...] = (
    SilverEntity(
        name="patients", key="patient_id",
        required=("patient_id", "gender", "birth_year", "created_at"),
        prepare=_patients_prepare, rules=_patients_rules, output=_patients_output,
    ),
    SilverEntity(
        name="encounters", key="encounter_id",
        required=("encounter_id", "patient_id", "encounter_type", "department",
                  "admission_time", "status"),
        prepare=_encounters_prepare, rules=_encounters_rules, output=_encounters_output,
        depends_on=("patients",),
    ),
    SilverEntity(
        name="lab_results", key="lab_result_id",
        required=("lab_result_id", "patient_id", "encounter_id", "test_code", "test_name", "unit", "measured_at"),
        prepare=_clinical_prepare, rules=_lab_rules, output=_lab_output, depends_on=("encounters",),
    ),
    SilverEntity(
        name="bed_events", key="event_id",
        required=("event_id", "patient_id", "encounter_id", "department", "bed_id", "event_type", "event_time"),
        prepare=_bed_prepare, rules=_bed_rules, output=_bed_output, depends_on=("encounters",),
    ),
    SilverEntity(
        name="medications", key="medication_id",
        required=("medication_id", "patient_id", "encounter_id", "drug_name", "dose_unit", "route", "start_time"),
        prepare=_clinical_prepare, rules=_medication_rules, output=_medication_output, depends_on=("encounters",),
    ),
    SilverEntity(
        name="vitals", key="measurement_id",
        required=("measurement_id", "patient_id", "encounter_id", "measured_at"),
        prepare=_clinical_prepare, rules=_vital_rules, output=_vital_output, depends_on=("encounters",),
    ),
)
ENTITY_NAMES = tuple(entity.name for entity in ENTITIES)


# --- core transform (pure: no catalog reads or writes) -----------------------

def collapse_exact_duplicates(bronze):
    """One row per distinct source record; earliest Bronze batch kept as lineage."""
    from pyspark.sql import functions as F

    source_columns = [name for name in bronze.columns if name not in BRONZE_METADATA]
    return (bronze.groupBy(*source_columns)
            .agg(F.min("_batch_id").alias("_bronze_batch_id"))
            .select(*source_columns, "_bronze_batch_id"))


def transform_entity(entity: SilverEntity, bronze, silver: Mapping[str, object],
                     processed_at: datetime) -> SilverResult:
    from pyspark.sql import functions as F

    started = monotonic()
    missing = [name for name in entity.depends_on if name not in silver]
    if missing:
        raise ValueError(f"{entity.name}: missing Silver dependencies {missing}")
    source_columns = [name for name in bronze.columns if name not in BRONZE_METADATA]
    bronze_rows = bronze.count()
    collapsed = collapse_exact_duplicates(bronze)
    distinct_rows = collapsed.count()

    prepared = entity.prepare(collapsed, silver)
    result = assess_dataframe(
        prepared, required_columns=entity.required, rules=entity.rules(prepared),
        duplicate_keys=(entity.key,), job_name=f"bronze_to_silver.{entity.name}", logger=LOGGER,
    )
    processed = F.lit(processed_at).cast("timestamp").alias("_processed_at")
    valid = result.valid_rows.select(*entity.output(result.valid_rows),
                                     F.col("_bronze_batch_id"), processed)
    quarantine = result.quarantine_rows.select(
        F.lit(entity.name).alias("entity"),
        F.col(entity.key).cast("string").alias("record_key"),
        F.col(REASON_COLUMN).alias("reason_codes"),
        F.to_json(F.struct(*source_columns)).alias("raw_record"),
        F.col("_bronze_batch_id"),
        processed,
    )
    metrics = result.metrics
    report = {
        "job": "bronze_to_silver", "entity": entity.name,
        "run_id": processed_at.astimezone(timezone.utc).isoformat(),
        "bronze_rows": bronze_rows,
        "exact_duplicate_rows": bronze_rows - distinct_rows,
        "input_rows": metrics.input_rows,
        "invalid_rows": metrics.invalid_rows,
        "duplicate_key_rows": metrics.duplicate_rows,
        "output_rows": metrics.valid_rows,
        "quarantine_rows": metrics.invalid_rows + metrics.duplicate_rows,
        "duration_seconds": round(monotonic() - started, 3),
    }
    return SilverResult(valid, quarantine, report)


# --- catalog I/O --------------------------------------------------------------

def ddl_statements(catalog: str = "iceberg"):
    sql = "\n".join(line for line in DDL_PATH.read_text(encoding="utf-8").splitlines()
                    if not line.lstrip().startswith("--"))
    return [statement.strip().replace("iceberg.silver", f"{catalog}.silver")
            for statement in sql.split(";") if statement.strip()]


def select_entities(names: Sequence[str]) -> Tuple[SilverEntity, ...]:
    unknown = set(names) - set(ENTITY_NAMES)
    if not names or unknown or len(set(names)) != len(names):
        raise ValueError(f"entities must be a non-empty unique subset of {ENTITY_NAMES}")
    return tuple(entity for entity in ENTITIES if entity.name in names)


def run(spark, entity_names: Sequence[str] = ENTITY_NAMES, catalog: str = "iceberg") -> list:
    from pyspark.sql import functions as F

    selected = select_entities(entity_names)
    for statement in ddl_statements(catalog):
        spark.sql(statement)
    processed_at = datetime.now(timezone.utc).replace(microsecond=0)
    silver: Dict[str, object] = {}
    reports = []
    for entity in selected:
        for dependency in entity.depends_on:
            if dependency not in silver:
                silver[dependency] = spark.table(f"{catalog}.silver.{dependency}")
        target = f"{catalog}.silver.{entity.name}"
        if not spark.catalog.tableExists(target):
            raise ValueError(f"Missing {target}; add its DDL to {DDL_PATH.name}")
        result = transform_entity(entity, spark.table(f"{catalog}.bronze.{entity.bronze_table}"),
                                  silver, processed_at)
        # Clustered (non-fanout) writes keep one Parquet writer open at a time.
        result.valid.writeTo(target).option("fanout-enabled", "false").overwrite(F.lit(True))
        (result.quarantine.writeTo(f"{catalog}.silver.{QUARANTINE_TABLE}")
         .option("fanout-enabled", "false").overwrite(F.col("entity") == F.lit(entity.name)))
        silver[entity.name] = spark.table(target)
        LOGGER.info(json.dumps(result.report, sort_keys=True))
        reports.append(result.report)
    return reports


def main(argv=None) -> int:
    from spark.common.config import SparkConfig
    from spark.common.iceberg import create_spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--entities", nargs="+", choices=ENTITY_NAMES, default=list(ENTITY_NAMES))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = create_spark_session("bronze_to_silver", SparkConfig.from_env())
    try:
        run(spark, args.entities)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
