"""Run TV2's Week 5 Kafka -> validated/dedup buffer -> realtime metrics."""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
from pathlib import Path
import shutil
import time

from spark.streaming.kafka_reader import deduplicate_events, parse_kafka_events, read_kafka_events
from spark.streaming.realtime_aggregates import (
    department_metrics, icu_encounter_state, icu_snapshot, lab_metrics, prepare_metric_events,
)

LOG = logging.getLogger(__name__)
TABLES = {"department": "realtime_department_metrics", "lab": "realtime_lab_metrics"}


def runtime_identity(root, source):
    """Pin source/settings and the revision epoch to a persistent query lifetime.

One flock-guarded driver owns a state directory. Normal restarts reuse its
epoch/checkpoints; removing a child checkpoint is an error. A fresh run uses a
new root and must replay the same complete source before replacing serving data.
"""
    path = root / "identity.json"
    if path.exists():
        identity = json.loads(path.read_text())
        if identity["source"] != source:
            raise ValueError("state directory source/settings changed; use a new state directory")
        for name in ("accepted", "quarantine", "dedup", "department", "lab", "lab15m", "icu"):
            target = root / ("buffers" if name in ("accepted", "dedup") else "output") / name
            checkpoint = root / "checkpoints" / name / "offsets"
            published = (target / "_spark_metadata" / "0").exists() if name in ("accepted", "dedup", "quarantine") else target.exists()
            if published and not checkpoint.exists():
                raise ValueError("checkpoint removed for " + name + "; do not reset a query inside an existing state directory")
            if name in ("accepted", "dedup") and checkpoint.exists() and not (target / "_spark_metadata").exists():
                raise ValueError("persistent replay buffer removed for " + name)
        return identity
    identity = {"source": source, "revision_base": time.time_ns() // 1000, "format": 1}
    with path.open("x") as handle:
        json.dump(identity, handle, sort_keys=True)
    return identity


def publish_snapshot(frame, batch_id, *, root, name, revision_base, clickhouse_writer=None):
    """Publish immutable batch snapshots, replaying one revision after failure.

Local/PVC filesystem only: atomic directory rename is the commit marker. The
optional TV3 sink runs before this marker, so partial remote writes are retried
with the same absolute values/revision. ICU is a TV1 fragment, never sent as a
partial hospital-state row. Historical snapshots are retained for inspection.
"""
    from pyspark.sql import functions as F

    if name not in ("department", "lab", "lab15m", "icu") or batch_id < 0:
        raise ValueError("unknown metric or invalid batch id")
    output = root / "output" / name
    output.mkdir(parents=True, exist_ok=True)
    committed = output / ("batch-%020d" % batch_id)
    if committed.exists():
        LOG.info("job=clinical_metrics metric=%s batch_id=%d replay=already_published", name, batch_id)
        return
    snapshot = (icu_snapshot(frame).where("event_time IS NOT NULL") if name == "icu" else frame)
    snapshot = snapshot.withColumn("revision", F.lit(revision_base + batch_id).cast("long")).persist()
    pending = output / (".pending-%020d" % batch_id)
    try:
        rows = snapshot.count()
        if clickhouse_writer is not None and name in TABLES:
            clickhouse_writer(snapshot, batch_id, table=TABLES[name])
        if pending.exists():
            shutil.rmtree(pending)
        snapshot.write.mode("errorifexists").json(str(pending))
        pending.rename(committed)
        LOG.info("job=clinical_metrics metric=%s batch_id=%d output_rows=%d revision=%d",
                 name, batch_id, rows, revision_base + batch_id)
    finally:
        snapshot.unpersist()


def start_queries(parsed, *, state_dir, revision_base, watermark_delay="10 minutes", clickhouse_writer=None):
    """Start independently checkpointed queries; return them in dependency order.

The two Parquet buffers are internal replay state, not analytical Lakehouse
tables. Splitting dedup from window aggregation supports Spark 3.5 update mode.
ICU consumes pre-watermark valid events so old admissions are not discarded.
"""
    from pyspark.sql import functions as F

    root = Path(state_dir).resolve()
    spark = parsed.sparkSession
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("clinical metrics require Spark session timezone UTC")
    prepared = prepare_metric_events(parsed)
    accepted = root / "buffers" / "accepted"
    dedup = root / "buffers" / "dedup"
    for path in (accepted, dedup):
        if not path.exists():
            spark.createDataFrame([], prepared.schema).write.parquet(str(path))
    queries = []

    def checkpoint(name):
        return str(root / "checkpoints" / name)

    try:
        for name, predicate, fmt, target in (
            ("accepted", F.col("validation_error").isNull(), "parquet", accepted),
            ("quarantine", F.col("validation_error").isNotNull(), "json", root / "output" / "quarantine"),
        ):
            queries.append(prepared.where(predicate).writeStream.format(fmt).outputMode("append")
                           .option("path", str(target)).option("checkpointLocation", checkpoint(name)).start())
        all_events = spark.readStream.schema(prepared.schema).parquet(str(accepted))
        unique = deduplicate_events(all_events, watermark_delay=watermark_delay)
        queries.append(unique.writeStream.format("parquet").outputMode("append")
                       .option("path", str(dedup)).option("checkpointLocation", checkpoint("dedup")).start())
        window_events = spark.readStream.schema(prepared.schema).parquet(str(dedup))
        frames = {
            "department": department_metrics(window_events, watermark_delay=watermark_delay),
            "lab": lab_metrics(window_events, watermark_delay=watermark_delay),
            "lab15m": lab_metrics(window_events, watermark_delay=watermark_delay, rolling=True),
            "icu": icu_encounter_state(all_events),
        }
        for name, frame in frames.items():
            def write(batch, batch_id, metric=name):
                publish_snapshot(batch, batch_id, root=root, name=metric,
                                 revision_base=revision_base, clickhouse_writer=clickhouse_writer)
            queries.append(frame.writeStream.outputMode("complete" if name == "icu" else "update")
                           .foreachBatch(write).option("checkpointLocation", checkpoint(name)).start())
        return queries
    except Exception:
        for query in reversed(queries):
            query.stop()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topics", default=os.environ.get("KAFKA_TOPICS", "hospital.events"))
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--state-dir", default=os.environ.get("CLINICAL_METRICS_STATE_DIR", "checkpoints/clinical-metrics"))
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--clickhouse", action="store_true", help="Also send department/lab snapshots through TV3's installed sink")
    args = parser.parse_args(argv)
    if not args.state_dir.strip():
        parser.error("--state-dir must be a persistent local/PVC directory")
    root = Path(args.state_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with (root / "writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("another clinical-metrics writer owns this state directory")
        source = {"bootstrap_servers": args.bootstrap_servers, "topics": args.topics,
                  "starting_offsets": args.starting_offsets, "watermark": args.watermark,
                  "clickhouse": args.clickhouse}
        identity = runtime_identity(root, source)
        writer = None
        if args.clickhouse:
            try:
                from spark.streaming.clickhouse_sink import ClickHouseConfig, write_microbatch
            except ImportError:
                parser.error("TV3 ClickHouse sink is not installed; merge its serving prerequisite before --clickhouse")
            config = ClickHouseConfig.from_env()
            def writer(frame, batch_id, *, table):
                write_microbatch(frame, batch_id, table=table, config=config)
        import pyspark
        from spark.common.config import SparkConfig
        from spark.common.iceberg import create_spark_session
        from spark.streaming.kafka_reader import kafka_connector_package
        spark = create_spark_session("HospitalClinicalMetrics", SparkConfig.from_env(),
                                     extra_packages=(kafka_connector_package(pyspark.__version__),))
        queries = []
        try:
            records = read_kafka_events(spark, bootstrap_servers=args.bootstrap_servers,
                                       topics=args.topics, starting_offsets=args.starting_offsets)
            queries = start_queries(parse_kafka_events(records), state_dir=root,
                                    revision_base=identity["revision_base"], watermark_delay=args.watermark,
                                    clickhouse_writer=writer)
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            LOG.info("job=clinical_metrics stopping=interrupt checkpoints=preserved")
        finally:
            for query in reversed(queries):
                query.stop()
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
