"""Prometheus text endpoint backed by completed Spark batches and Kafka offsets.

No synthetic success counters: absence of a completed batch remains observable.
Kafka lag is log-end offset minus the last *completed* Spark end offset. Spark
does not commit its progress as a Kafka consumer group, so group lag is unsuitable.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger(__name__)


def render(progress, ends, *, kafka_ok, active):
    lines = ['# TYPE hospital_stream_active gauge', f'hospital_stream_active {int(active)}',
             '# TYPE hospital_kafka_probe_success gauge', f'hospital_kafka_probe_success {int(kafka_ok)}']
    def metric(name, value, **labels):
        tags = ','.join(f'{key}={json.dumps(str(val))}' for key, val in labels.items())
        lines.append(f'hospital_{name}{{{tags}}} {float(value)}')
    for name, value in progress.items():
        label = dict(query=name)
        metric('stream_batch_id', value['batchId'], **label)
        metric('stream_input_rows_per_second', value.get('inputRowsPerSecond', 0), **label)
        metric('stream_processed_rows_per_second', value.get('processedRowsPerSecond', 0), **label)
        metric('stream_batch_input_rows', value.get('numInputRows', 0), **label)
        metric('stream_batch_duration_seconds', value.get('durationMs', {}).get('triggerExecution', 0)/1000, **label)
        duration = value.get('durationMs', {}).get('triggerExecution', 0)/1000
        started = datetime.fromisoformat(value['timestamp'].replace('Z', '+00:00')).timestamp()
        metric('stream_last_completed_timestamp_seconds', started + duration, **label)
        for key, field in [('late_events', 'numRowsDroppedByWatermark'), ('state_rows', 'numRowsTotal'),
                           ('state_memory_bytes', 'memoryUsedBytes')]:
            metric('stream_batch_'+key, sum(s.get(field, 0) for s in value.get('stateOperators', [])), **label)
        metric('stream_batch_duplicate_events', sum(s.get('customMetrics', {}).get('numDroppedDuplicateRows', 0)
               for s in value.get('stateOperators', [])), **label)
        for source in value.get('sources', []):
            offsets = source.get('endOffset') or {}
            if isinstance(offsets, str):
                offsets = json.loads(offsets)
            for topic, partitions in offsets.items():
                for partition, offset in partitions.items():
                    tags = dict(**label, topic=topic, partition=partition)
                    metric('stream_committed_offset', offset, **tags)
                    if kafka_ok and (topic, int(partition)) in ends:
                        end = ends[topic, int(partition)]
                        metric('kafka_end_offset', end, **tags)
                        metric('kafka_lag', max(0, end-offset), **tags)
    return '\n'.join(lines)+'\n'


class StreamMetrics:
    def __init__(self, spark, bootstrap, topics, port=8090):
        self.progress = {}
        self.ends = {}
        self.text = render({}, {}, kafka_ok=False, active=False)
        self.jvm = spark._jvm
        self.topics = topics.split(',')
        props = self.jvm.java.util.Properties()
        props.setProperty('bootstrap.servers', bootstrap)
        props.setProperty('request.timeout.ms', '5000')
        props.setProperty('default.api.timeout.ms', '5000')
        self.admin = self.jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        outer = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path not in ('/metrics', '/progress'):
                    self.send_error(404)
                    return
                body = (outer.text if self.path == '/metrics' else json.dumps(outer.progress)).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; version=0.0.4' if self.path == '/metrics' else 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *_args):
                pass
        self.server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def refresh(self, spark):
        active = list(spark.streams.active)
        for query in active:
            progress = query.lastProgress
            if progress is not None:
                self.progress[query.name or str(query.id)] = progress
        kafka_ok = False
        try:
            topics = self.jvm.java.util.ArrayList()
            for name in self.topics:
                topics.add(name.strip())
            descriptions = self.admin.describeTopics(topics).allTopicNames().get()
            requested = self.jvm.java.util.HashMap()
            for entry in descriptions.entrySet():
                for part in entry.getValue().partitions():
                    key = self.jvm.org.apache.kafka.common.TopicPartition(entry.getKey(), part.partition())
                    requested.put(key, self.jvm.org.apache.kafka.clients.admin.OffsetSpec.latest())
            result = self.admin.listOffsets(requested).all().get()
            self.ends = {(entry.getKey().topic(), entry.getKey().partition()): entry.getValue().offset()
                         for entry in result.entrySet()}
            kafka_ok = True
        except Exception:
            # Metrics failure must be visible without killing a healthy pipeline.
            LOG.exception('Kafka metrics probe failed')
        self.text = render(self.progress, self.ends, kafka_ok=kafka_ok, active=len(active) == 2)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.admin.close()
