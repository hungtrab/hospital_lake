"""Compare observed journey snapshots with an explicit expected fixture."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re

DEPARTMENTS = {"ICU", "CARDIOLOGY"}


def _text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")


def _utc(value, field):
    _text(value, field)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z", value):
        raise ValueError(f"{field} must be YYYY-MM-DDTHH:MM:SS[.ffffff]Z")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid UTC timestamp") from exc


def _counts(snapshot, field):
    if not isinstance(snapshot, dict) or not {"current_patients", "by_department"} <= snapshot.keys():
        raise ValueError(f"{field}: missing current_patients or by_department")
    departments = snapshot["by_department"]
    if not isinstance(departments, dict) or set(departments) != DEPARTMENTS:
        raise ValueError(f"{field}: by_department must contain exactly ICU and CARDIOLOGY")
    for name, value in [("current_patients", snapshot["current_patients"]), *departments.items()]:
        if type(value) is not int or value < 0:
            raise ValueError(f"{field}.{name} must be a nonnegative integer")


def _steps(document, *, expected):
    if not isinstance(document, dict):
        raise ValueError("journey document must be an object")
    _text(document.get("scenario_id"), "scenario_id")
    steps = document.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError("steps must be a nonempty array")
    indexed, events_seen = {}, {}
    previous = None
    for step in steps:
        if not isinstance(step, dict):
            raise ValueError("each step must be an object")
        step_id = step.get("step_id")
        _text(step_id, "step_id")
        if step_id in indexed:
            raise ValueError(f"duplicate step_id: {step_id}")
        _counts(step.get("expected") if expected else step, step_id)
        if expected:
            as_of = _utc(step.get("as_of"), f"{step_id}.as_of")
            if previous is not None and as_of < previous:
                raise ValueError("expected as_of timestamps must be chronological")
            previous = as_of
            if not isinstance(step.get("events"), list):
                raise ValueError(f"{step_id}.events must be an array")
            for event in step["events"]:
                if not isinstance(event, dict):
                    raise ValueError("event must be an object")
                for key in ("event_id", "event_type", "source"):
                    _text(event.get(key), key)
                if type(event.get("version")) is not int or event["version"] != 1:
                    raise ValueError("event version must be 1")
                if _utc(event.get("event_time"), "event_time") > as_of:
                    raise ValueError("event_time must not be later than observation as_of")
                payload = event.get("payload")
                if not isinstance(payload, dict):
                    raise ValueError("event payload must be an object")
                for key in ("patient_id", "encounter_id", "department"):
                    _text(payload.get(key), f"payload.{key}")
                if event["event_type"] == "TRANSFER":
                    for key in ("from_bed_id", "to_bed_id"):
                        _text(payload.get(key), f"payload.{key}")
                if event["event_type"] == "ADMISSION":
                    _text(payload.get("encounter_type"), "payload.encounter_type")
                event_id = event["event_id"]
                if event_id in events_seen and event != events_seen[event_id]:
                    raise ValueError(f"replayed event changed content: {event_id}")
                events_seen[event_id] = event
        indexed[step_id] = step
    return indexed


def compare_journey(expected: dict, actual: dict) -> dict:
    expected_steps = _steps(expected, expected=True)
    actual_steps = _steps(actual, expected=False)
    if expected["scenario_id"] != actual["scenario_id"]:
        raise ValueError("scenario_id does not match")
    if set(expected_steps) != set(actual_steps):
        missing = sorted(set(expected_steps) - set(actual_steps))
        extra = sorted(set(actual_steps) - set(expected_steps))
        raise ValueError(f"step IDs do not match: missing={missing}, extra={extra}")
    mismatches = []
    for step_id, step in expected_steps.items():
        wanted, observed = step["expected"], actual_steps[step_id]
        fields = [("current_patients", wanted["current_patients"], observed["current_patients"])]
        fields += [(f"by_department.{dept}", wanted["by_department"][dept], observed["by_department"][dept])
                   for dept in sorted(DEPARTMENTS)]
        for field, target, value in fields:
            if target != value:
                mismatches.append(dict(step_id=step_id, field=field, expected=target, actual=value))
    if mismatches:
        return dict(status="FAIL", checked_steps=len(expected_steps), mismatches=mismatches)
    return dict(status="PASS", checked_steps=len(expected_steps))


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--actual", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        expected = json.loads(args.expected.read_text(encoding="utf-8"), object_pairs_hook=_unique_keys)
        actual = json.loads(args.actual.read_text(encoding="utf-8"), object_pairs_hook=_unique_keys)
        result = compare_journey(expected, actual)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, allow_nan=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
