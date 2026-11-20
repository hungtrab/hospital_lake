"""Build a deterministic daily arrival plan without starting a producer."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
import random
import re
import tempfile


def _minute(value, *, end=False):
    if end and value == "24:00":
        return 1440
    if not isinstance(value, str) or not re.fullmatch(r"[0-2][0-9]:[0-5][0-9]", value):
        raise ValueError("slot time must be HH:MM")
    hour, minute = map(int, value.split(":"))
    if hour > 23:
        raise ValueError("slot hour must be 00..23 (24:00 allowed only as end)")
    return hour * 60 + minute


def build_schedule(config: dict) -> list[dict]:
    if not isinstance(config, dict):
        raise ValueError("config must be an object")
    day_text = config.get("date")
    if not isinstance(day_text, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_text):
        raise ValueError("date must be YYYY-MM-DD")
    day = date.fromisoformat(day_text)
    if config.get("timezone") != "Asia/Ho_Chi_Minh":
        raise ValueError("timezone must be Asia/Ho_Chi_Minh")
    visits, seed = config.get("visits"), config.get("seed")
    if type(visits) is not int or visits < 0:
        raise ValueError("visits must be a nonnegative integer")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    slots = config.get("slots")
    if not isinstance(slots, list) or not slots:
        raise ValueError("slots must be a nonempty array")
    intervals, weights = [], []
    for slot in slots:
        if not isinstance(slot, dict):
            raise ValueError("each slot must be an object")
        start, end = _minute(slot.get("start")), _minute(slot.get("end"), end=True)
        if start >= end:
            raise ValueError("slot start must precede end within the same day")
        weight = slot.get("weight")
        try:
            valid = type(weight) in (int, float) and math.isfinite(weight) and weight > 0
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("slot weight must be finite and positive")
        intervals.append((start, end))
        weights.append(weight)
    ordered = sorted(intervals)
    if any(left[1] > right[0] for left, right in zip(ordered, ordered[1:])):
        raise ValueError("slots must not overlap")
    # Scaling keeps finite individual weights from overflowing their sum.
    weights = [weight / max(weights) for weight in weights]
    rng = random.Random(seed)
    minutes = []
    for _ in range(visits):
        start, end = rng.choices(intervals, weights=weights, k=1)[0]
        minutes.append(rng.randrange(start, end))
    midnight = datetime.combine(day, time(), timezone(timedelta(hours=7)))
    result = []
    for index, minute in enumerate(sorted(minutes), 1):
        try:
            arrival = (midnight + timedelta(minutes=minute)).astimezone(timezone.utc)
        except OverflowError as exc:
            raise ValueError("date cannot be represented in UTC") from exc
        result.append({"visit_id": f"VISIT-{day_text.replace('-', '')}-{index:06d}",
                       "arrival_time": arrival.isoformat(timespec="seconds").replace("+00:00", "Z")})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    temporary = None
    try:
        result = build_schedule(json.loads(args.config.read_text(encoding="utf-8")))
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.output.parent,
                                         prefix=".schedule-", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(args.output)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
