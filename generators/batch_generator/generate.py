"""Generate deterministic synthetic CSV inputs for the initial batch pipeline."""

import argparse
import csv
import random
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIELDS = {
    "patients": ("patient_id", "gender", "birth_year", "city", "created_at"),
    "encounters": ("encounter_id", "patient_id", "encounter_type", "department",
                   "admission_time", "discharge_time", "status"),
    "lab_results": ("lab_result_id", "patient_id", "encounter_id", "test_code",
                    "test_name", "result_value", "unit", "reference_low",
                    "reference_high", "is_abnormal", "measured_at"),
}


def generate(output_dir, patients=1000, seed=42):
    """Write one encounter and lab per patient; memory use is independent of scale.

    Existing output files are replaced, so a rerun never appends duplicate rows.
    All timestamps use UTC and fields follow plan.md section 5.
    """
    if isinstance(patients, bool) or not isinstance(patients, int) or patients < 1:
        raise ValueError("patients must be a positive integer")
    rng = random.Random(seed)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    epoch = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with ExitStack() as stack:
        writers = {}
        for table, fields in FIELDS.items():
            handle = stack.enter_context(
                (output_dir / f"{table}.csv").open("w", newline="", encoding="utf-8")
            )
            writers[table] = csv.DictWriter(handle, fieldnames=fields)
            writers[table].writeheader()
        for index in range(1, patients + 1):
            patient_id = f"patient-{index:08d}"
            encounter_id = f"encounter-{index:08d}"
            admission = epoch + timedelta(minutes=rng.randrange(180 * 24 * 60))
            discharge = admission + timedelta(hours=rng.randint(2, 168))
            value = round(rng.uniform(2.0, 12.0), 2)
            writers["patients"].writerow(dict(zip(FIELDS["patients"], (
                patient_id, rng.choice(("M", "F", "OTHER")), rng.randint(1930, 2025),
                "SYNTHETIC_CITY", (admission - timedelta(days=1)).isoformat(),
            ))))
            writers["encounters"].writerow(dict(zip(FIELDS["encounters"], (
                encounter_id, patient_id, "inpatient",
                rng.choice(("INTERNAL_MEDICINE", "CARDIOLOGY", "SURGERY")),
                admission.isoformat(), discharge.isoformat(), "discharged",
            ))))
            writers["lab_results"].writerow(dict(zip(FIELDS["lab_results"], (
                f"lab-{index:08d}", patient_id, encounter_id, "GLUCOSE", "Glucose",
                value, "mmol/L", 3.9, 7.8, value < 3.9 or value > 7.8,
                (admission + timedelta(hours=1)).isoformat(),
            ))))
    return {table: patients for table in FIELDS}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--patients", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    if args.patients < 1:
        parser.error("--patients must be positive")
    counts = generate(args.output_dir, args.patients, args.seed)
    for table, count in counts.items():
        print(f"{table}: {count} rows -> {args.output_dir / (table + '.csv')}")


if __name__ == "__main__":
    main()
