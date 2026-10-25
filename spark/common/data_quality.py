"""Reusable Spark row validation, quarantine, and duplicate metrics."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from time import perf_counter
from typing import Mapping, Sequence


LOG = logging.getLogger(__name__)
REASON_COLUMN = "_quality_reason_codes"
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class QualityMetrics:
    """Disjoint counts: duplicate_rows excludes rows already counted invalid."""

    input_rows: int
    valid_rows: int
    invalid_rows: int
    duplicate_rows: int


@dataclass(frozen=True)
class QualityResult:
    """Accepted unique rows and rejected rows with traceable reason codes.

    Every row in a duplicated key group is quarantined. The framework does not
    choose a winner when duplicate records disagree; a downstream transform can
    apply an entity-specific resolution rule without losing the original rows.
    """

    valid_rows: object
    quarantine_rows: object
    metrics: QualityMetrics


def _quoted(name: str):
    from pyspark.sql import functions as F

    return F.col("`" + name.replace("`", "``") + "`")


def assess_dataframe(
    frame,
    *,
    required_columns: Sequence[str],
    rules: Mapping[str, object],
    duplicate_keys: Sequence[str],
    job_name: str,
    logger: logging.Logger = LOG,
) -> QualityResult:
    """Validate one batch DataFrame and emit input/valid/invalid/duplicate counts.

    ``rules`` maps stable uppercase reason codes to Spark boolean expressions
    that must evaluate true. A null predicate is treated as a failed rule.
    Duplicate-key groups are quarantined in full, so no conflicting row is
    silently selected or discarded.
    """
    from pyspark.sql import functions as F

    if not job_name or not job_name.strip():
        raise ValueError("job_name must be non-empty")
    if REASON_COLUMN in frame.columns:
        raise ValueError(f"input already contains reserved column {REASON_COLUMN}")

    required = tuple(required_columns)
    keys = tuple(duplicate_keys)
    if len(required) != len(set(required)) or any(not name for name in required):
        raise ValueError("required_columns must contain unique non-empty names")
    if not keys or len(keys) != len(set(keys)) or not set(keys).issubset(required):
        raise ValueError("duplicate_keys must be unique required columns")
    if not required and not rules:
        raise ValueError("at least one required column or validation rule is needed")

    missing = (set(required) | set(keys)) - set(frame.columns)
    if missing:
        raise ValueError("input missing required columns: " + ", ".join(sorted(missing)))

    reason_expressions = []
    generated_codes = set()
    for name in required:
        suffix = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()
        code = f"MISSING_{suffix or 'FIELD'}"
        if code in generated_codes:
            raise ValueError("required column names produce duplicate reason codes")
        generated_codes.add(code)
        value = _quoted(name)
        missing_value = value.isNull() | (F.trim(value.cast("string")) == "")
        reason_expressions.append(F.when(missing_value, F.lit(code)))

    for code, predicate in rules.items():
        if not _REASON_CODE.fullmatch(code):
            raise ValueError(f"invalid reason code: {code!r}")
        if code in generated_codes:
            raise ValueError(f"duplicate reason code: {code}")
        generated_codes.add(code)
        passed = F.coalesce(predicate, F.lit(False))
        reason_expressions.append(F.when(~passed, F.lit(code)))

    started = perf_counter()
    tagged = frame.withColumn(
        REASON_COLUMN,
        F.filter(F.array(*reason_expressions), lambda reason: reason.isNotNull()),
    ).persist()
    try:
        summary = tagged.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.coalesce(
                F.sum(F.when(F.size(_quoted(REASON_COLUMN)) > 0, 1).otherwise(0)),
                F.lit(0),
            ).alias("invalid_rows"),
        ).first()
        input_rows = int(summary.input_rows)
        invalid_rows = int(summary.invalid_rows)

        source_columns = list(frame.columns)
        candidates = tagged.where(F.size(_quoted(REASON_COLUMN)) == 0)
        # A bad variant must not cause its conflicting valid sibling to win.
        # The caller collapses exact duplicates before this key-level check.
        duplicate_keys_frame = (
            tagged.groupBy(*[_quoted(name) for name in keys])
            .count()
            .where(F.col("count") > 1)
            .select(*[_quoted(name) for name in keys])
        )
        duplicate_rows = candidates.join(duplicate_keys_frame, list(keys), "left_semi")
        duplicate_count = duplicate_rows.count()

        valid_rows = (
            candidates.join(duplicate_keys_frame, list(keys), "left_anti")
            .select(*[_quoted(name) for name in source_columns])
        )
        invalid_rows_frame = (
            tagged.where(F.size(_quoted(REASON_COLUMN)) > 0)
            .select(*[_quoted(name) for name in source_columns], _quoted(REASON_COLUMN))
        )
        invalid_conflicts = (invalid_rows_frame.join(duplicate_keys_frame, list(keys), "left_semi")
                             .withColumn(REASON_COLUMN,
                                         F.concat(_quoted(REASON_COLUMN), F.array(F.lit("DUPLICATE_KEY")))))
        invalid_rows_frame = (invalid_rows_frame.join(duplicate_keys_frame, list(keys), "left_anti")
                              .unionByName(invalid_conflicts))
        duplicate_quarantine = duplicate_rows.select(
            *[_quoted(name) for name in source_columns],
            F.array(F.lit("DUPLICATE_KEY")).alias(REASON_COLUMN),
        )
        quarantine_rows = invalid_rows_frame.unionByName(duplicate_quarantine)
        valid_count = input_rows - invalid_rows - duplicate_count
        metrics = QualityMetrics(
            input_rows=input_rows,
            valid_rows=valid_count,
            invalid_rows=invalid_rows,
            duplicate_rows=duplicate_count,
        )
        logger.info(
            "job=%s input_rows=%d valid_rows=%d invalid_rows=%d duplicate_rows=%d duration_seconds=%.3f",
            job_name,
            metrics.input_rows,
            metrics.valid_rows,
            metrics.invalid_rows,
            metrics.duplicate_rows,
            perf_counter() - started,
        )
        return QualityResult(valid_rows, quarantine_rows, metrics)
    finally:
        tagged.unpersist()
