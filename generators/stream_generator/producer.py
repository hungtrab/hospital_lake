"""Publish simulated hospital events to Kafka at a configurable rate.

Usage:
    python -m generators.stream_generator.producer --rate 1000 --duration 60
    python -m generators.stream_generator.producer --max-events 20 --dry-run   # print JSON, no Kafka

Messages are keyed by patient_id so one patient's events stay ordered within
a partition (plan.md §9.2). Bootstrap servers and topic come from flags or the
KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPICS environment variables.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Optional

import yaml

from generators.stream_generator.event_models import HospitalSimulator, SimulatorConfig

LOG = logging.getLogger("stream_producer")
DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")
TICK_SECONDS = 0.1
STATS_EVERY_SECONDS = 10.0


def load_config(path: Path, **overrides) -> dict:
    raw = yaml.safe_load(path.read_text()) or {}
    raw.update({key: value for key, value in overrides.items() if value is not None})
    if float(raw.get("events_per_second", 0)) <= 0:
        raise ValueError("events_per_second must be > 0")
    return raw


def simulator_config(raw: dict) -> SimulatorConfig:
    return SimulatorConfig(
        seed=int(raw.get("seed", 42)),
        max_active_encounters=int(raw.get("max_active_encounters", 500)),
        returning_patient_probability=float(raw.get("returning_patient_probability", 0.3)),
        late_event_probability=float(raw.get("late_event_probability", 0.0)),
        late_delay_minutes=tuple(float(v) for v in raw.get("late_delay_minutes", (11, 30))),
        duplicate_probability=float(raw.get("duplicate_probability", 0.0)),
        invalid_event_probability=float(raw.get("invalid_event_probability", 0.0)),
    )


def run(simulator: HospitalSimulator, send: Callable[[str, bytes], None], *, rate: float,
        duration: Optional[float] = None, max_events: Optional[int] = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep) -> Counter:
    """Send messages paced to ``rate`` per second until a limit is reached.

    Pacing targets the cumulative schedule (events sent = rate x elapsed), so
    short stalls are caught up instead of permanently lowering throughput.
    """
    if rate <= 0:
        raise ValueError("rate must be > 0")
    if duration is None and max_events is None:
        raise ValueError("set duration or max_events (use a large duration to run indefinitely)")
    stats: Counter = Counter()
    started = monotonic()
    last_report = started
    sent = 0
    while True:
        now = monotonic()
        elapsed = now - started
        if (max_events is not None and sent >= max_events) or (duration is not None and elapsed >= duration):
            break
        due = int(rate * (elapsed + TICK_SECONDS)) - sent
        if max_events is not None:
            due = min(due, max_events - sent)
        if duration is not None:
            due = min(due, int(rate * duration) - sent)
        for _ in range(max(due, 0)):
            message = simulator.next_message()
            send(message.key, message.value)
            stats[message.kind] += 1
            sent += 1
        if now - last_report >= STATS_EVERY_SECONDS:
            _log_stats(stats, sent, now - started)
            last_report = now
        remaining = started + (sent / rate) - monotonic()
        if remaining > 0:
            sleep(min(remaining, TICK_SECONDS))
    _log_stats(stats, sent, monotonic() - started)
    return stats


def _log_stats(stats: Counter, sent: int, elapsed: float) -> None:
    LOG.info(json.dumps({
        "component": "stream_producer", "events_sent": sent,
        "elapsed_seconds": round(elapsed, 2),
        "events_per_second": round(sent / elapsed, 1) if elapsed > 0 else None,
        **{f"{kind}_events": stats[kind] for kind in ("valid", "late", "duplicate", "invalid")},
    }, sort_keys=True))


def kafka_sender(bootstrap_servers: str, topic: str):
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",") if s.strip()],
                             acks="all", linger_ms=20, retries=5)

    def send(key: str, value: bytes) -> None:
        producer.send(topic, key=key.encode(), value=value)

    return send, producer


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--rate", type=float, help="events per second (overrides config)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--duration", type=float, help="seconds to run")
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPICS") or "hospital.events",
                        help="target topic; for a comma-separated list (KAFKA_TOPICS) the first is used")
    parser.add_argument("--dry-run", action="store_true", help="print messages to stdout instead of Kafka")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    logging.getLogger("kafka").setLevel(logging.WARNING)

    raw = load_config(args.config, events_per_second=args.rate, seed=args.seed)
    simulator = HospitalSimulator(simulator_config(raw))
    limits = {"duration": args.duration, "max_events": args.max_events}
    if args.duration is None and args.max_events is None:
        limits["duration"] = float(raw.get("duration_seconds", 60))

    if args.dry_run:
        def send(key: str, value: bytes) -> None:
            sys.stdout.write(value.decode("utf-8", "replace") + "\n")
        run(simulator, send, rate=float(raw["events_per_second"]), **limits)
        return 0

    if not args.bootstrap_servers:
        parser.error("--bootstrap-servers or KAFKA_BOOTSTRAP_SERVERS is required (or use --dry-run)")
    topic = args.topic.split(",")[0].strip()
    if not topic:
        parser.error("--topic must name a Kafka topic")
    send, producer = kafka_sender(args.bootstrap_servers, topic)
    try:
        run(simulator, send, rate=float(raw["events_per_second"]), **limits)
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
