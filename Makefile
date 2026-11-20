PYTHON ?= python3
CLINICAL_STATE_DIR ?= $(if $(strip $(CLINICAL_METRICS_STATE_DIR)),$(CLINICAL_METRICS_STATE_DIR),checkpoints/clinical-metrics)
CLINICAL_ARGS ?=
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then printf 'docker compose'; else printf 'docker-compose'; fi)

.PHONY: kafka-recovery kafka-up kafka-stop kafka-topics kafka-smoke batch batch-silver batch-silver-docker batch-bronze batch-bronze-docker test-bronze test-stream-bronze help setup up down config minio-up minio-stop bootstrap-bucket bootstrap-bronze bronze-smoke week1-smoke week1-recovery smoke-minio test-minio test trino-config trino-up trino-stop trino-smoke verify-bronze gold gold-docker stream-bronze stream-bronze-docker stream-bronze-probe produce produce-dry-run query slides slides-explained stream-gate dashboard dashboard-build dashboard-smoke
.DEFAULT_GOAL := help
.PHONY: video-setup video-voice video-timeline video-preview video slides-report
.PHONY: quality-demo
.PHONY: quality-web
.PHONY: k8s-tools k8s-image k8s-up k8s-down k8s-status k8s-batch k8s-stream k8s-smoke k8s-produce k8s-stop-producer benchmark

K8S_PATIENTS ?= 1000
K8S_EXECUTORS ?= 2
K8S_RATE ?= 100
BENCH_RATE ?= 100
BENCH_DURATION ?= 1800
BENCH_RESTART_AT ?= 900

k8s-tools:
	$(PYTHON) -m scripts.k8s_tools

k8s-image:
	$(COMPOSE) build minio iceberg-rest
	docker build -f infra/docker/spark/Dockerfile -t hospital-lake/spark:completion .

k8s-up: k8s-tools k8s-image
	$(PYTHON) -m scripts.k8s deploy

k8s-down:
	$(PYTHON) -m scripts.k8s stop

k8s-status:
	$(PYTHON) -m scripts.k8s status

k8s-batch:
	$(PYTHON) -m scripts.k8s batch --patients $(K8S_PATIENTS) --executors $(K8S_EXECUTORS)

k8s-stream:
	$(PYTHON) -m scripts.k8s stream --executors $(K8S_EXECUTORS)

k8s-smoke:
	$(PYTHON) -m scripts.k8s smoke

k8s-produce:
	$(PYTHON) -m scripts.k8s produce --rate $(K8S_RATE)

k8s-stop-producer:
	$(PYTHON) -m scripts.k8s stop-producer

benchmark:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.realtime_benchmark --env-file "$(REALTIME_ENV)" --rate $(BENCH_RATE) --duration $(BENCH_DURATION) --restart-at $(BENCH_RESTART_AT)

.PHONY: compact
COMPACT_TABLE ?= iceberg.bronze.stream_events_raw
compact:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.compact --table "$(COMPACT_TABLE)"

QUALITY_WEB_PORT ?= 8765

quality-web:
	$(PYTHON) -m dashboard.quality_manager.server --port "$(QUALITY_WEB_PORT)" --results "$(QUALITY_OUTPUT_DIR)"

QUALITY_OUTPUT_DIR ?= results/data-quality

quality-demo:
	$(PYTHON) -m scripts.data_quality_demo --output-dir "$(QUALITY_OUTPUT_DIR)"

VIDEO_PYTHON ?= python3
CLINICAL_STATE_DIR ?= $(if $(strip $(CLINICAL_METRICS_STATE_DIR)),$(CLINICAL_METRICS_STATE_DIR),checkpoints/clinical-metrics)
CLINICAL_ARGS ?=
VIDEO_CONCURRENCY ?= 2

help:
	@printf '%s\n' 'make stream-clinical PYTHON=.venv/bin/python  TV2 Kafka -> ICU/department/lab snapshots (persistent CLINICAL_STATE_DIR)' 'make test-clinical-stream  Exercise real local stream/checkpoint recovery' 'make test-clinical-kafka  Exercise live Kafka -> clinical metrics (start Kafka first)'
	@printf '%s\n' 'make clickhouse-up  Start aggregate serving tables and Grafana reader account' 'make test-clickhouse  Test Spark snapshots against a running disposable ClickHouse'
	@printf '%s\n' 'make realtime-up / realtime-smoke / realtime-recovery  Start and verify the full local realtime path' 'make monitoring  Start Prometheus for the local stream' 'make benchmark PYTHON=.venv/bin/python [BENCH_RATE=100 BENCH_DURATION=1800 BENCH_RESTART_AT=900]  Measure sustained workload and planned restart' 'make compact PYTHON=.venv/bin/python [COMPACT_TABLE=iceberg.bronze.stream_events_raw]  Measure Iceberg compaction; stop table writers first' 'make k8s-up / k8s-status / k8s-batch / k8s-stream / k8s-smoke  Isolated single-node kind profile; full integration validation pending' 'make k8s-down  Stop the kind node and preserve its data'
	@printf '%s\n' 'make quality-web PYTHON=.venv/bin/python [QUALITY_WEB_PORT=8765]  Open local quality reports, review notes and run the real batch demo from a browser'
	@printf '%s\n' 'make quality-demo PYTHON=.venv/bin/python  Run synthetic dirty-data cases through real Silver/Gold; write isolated Parquet and before/after reports'
	@printf '%s\n' 'make slides-report  Build the minimal English Group 22 milestone report with default Beamer and pdfLaTeX'
	@printf '%s\n' 'make video-setup  Install the optional video toolchain' 'make video-voice VIDEO_PYTHON=<speech-environment-python>  Generate Northern narration' 'make video-timeline VIDEO_PYTHON=<speech-environment-python>  Align narration and captions' 'make video-preview  Type-check and render key video frames' 'make video [VIDEO_CONCURRENCY=2]  Render, normalize, verify and package the introduction'
	@printf '%s\n' 'make slides-explained  Build the Vietnamese beginner Beamer deck; open docs/hospitallake-explained.html for the offline interactive guide'
	@printf '%s\n' 'make kafka-topics  Safely create/validate Kafka topics' 'make kafka-recovery PYTHON=.venv/bin/python  Verify data/offset persistence after broker recreation'
	@printf '%s\n' 'make kafka-up  Start optional local KRaft broker and bootstrap topics' 'make kafka-smoke PYTHON=.venv/bin/python  Verify host/internal keyed produce-consume' 'make kafka-stop  Stop Kafka; preserve its data volume'
	@printf '%s\n' 'make batch-bronze BATCH_ID=demo-001 [INPUT_DIR=data/generated] [ENTITIES=patients]' 'make test-bronze Run local Spark/Iceberg ingestion checks'
	@printf '%s\n' 'make dashboard-build  Regenerate provisioned Grafana dashboard JSON' 'make dashboard  Start provisioned Grafana and Trino' 'make dashboard-smoke  Check provisioning and run Trino-backed panels through Grafana'
	@printf '%s\n' 'make batch-bronze-docker BATCH_ID=demo-001  Run ingestion with Compose Spark and a read-only INPUT_DIR mount'
	@printf '%s\n' 'make batch [PATIENTS=1000 SEED=42]  Full batch path: generate -> bootstrap Bronze -> ingest Bronze -> verify -> Silver -> Gold'
	@printf '%s\n' 'make batch-silver-docker [SILVER_ENTITIES="patients encounters"]  Rebuild Silver + quarantine from Bronze with Compose Spark (default: all entities)' 'make batch-silver PYTHON=.venv/bin/python  Same job with host PySpark (host-reachable endpoints required)'
	@printf '%s\n' 'make produce KAFKA_BOOTSTRAP_SERVERS=localhost:9092 [RATE=1000 DURATION=60]  Publish simulated hospital events to Kafka' 'make produce-dry-run [RATE=1000 MAX_EVENTS=20]  Print simulated events as JSON lines (no Kafka)'
	@printf '%s\n' 'make stream-bronze PYTHON=.venv/bin/python KAFKA_BOOTSTRAP_SERVERS=localhost:9092 CHECKPOINT_LOCATION=$$PWD/checkpoints/stream-bronze'
	@printf '%s\n' 'make kafka-up && make stream-bronze-docker  Run the Compose Structured Streaming reader with persistent checkpoint volume'
	@printf '%s\n' 'make bootstrap-bronze Create/validate six Bronze tables without rewriting data' 'make bronze-smoke PYTHON=.venv/bin/python  Validate Parquet/schema/S3/Trino/reruns in isolated tables'
	@printf '%s\n' 'make minio-up    Build/start MinIO and bootstrap warehouse bucket' 'make minio-stop  Stop MinIO while preserving data' 'make bootstrap-bucket  Rerun bucket bootstrap safely' 'make test-minio  Verify S3 object persistence after recreating MinIO' 'make week1-smoke Verify Spark → Iceberg → Trino' 'make week1-recovery Verify the same fixture after recreating storage/catalog containers'
	@printf '%s\n' 'make setup       Create .env and install Python dependencies' 'make generate-data PATIENTS=1000 SEED=42 [FORMAT=parquet|csv]' 'make verify-generated INPUT_DIR=data/generated' 'make validate-generator SCALES="10000 100000"'  'make config      Validate Compose configuration' 'make up          Start local services and wait for health checks' 'make down        Stop local services; preserve data volumes' 'make smoke-minio Verify MinIO readiness through its published port' 'make test        Run unit and verifier contract tests' 'make trino-config Validate Compose configuration' 'make trino-up    Start Trino and wait for readiness' 'make trino-stop  Stop Trino' 'make trino-smoke TABLE=iceberg.bronze.spark_smoke EXPECTED_ROWS=3' 'make verify-bronze BATCH_ID=<id> [ENTITIES="patients encounters ..."]' 'make gold        Submit Silver-to-Gold Spark batch job' 'make gold-docker Rebuild Gold from Silver in the Compose Spark image' 'make query       Run sql/trino/queries.sql through Trino; fail on errors or empty results' 'make stream-gate [GATE_RATE=1000 GATE_DURATION=600]  Week 4 gate: producer -> Kafka -> stream reader -> Bronze, all offsets exactly once'

setup:
	@test -f .env || cp .env.example .env
	$(PYTHON) -m pip install -r requirements.txt

config:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up -d --build --wait --wait-timeout 180

minio-up:
	$(COMPOSE) up -d --build --wait --wait-timeout 90 minio
	$(COMPOSE) run --rm --no-deps minio-init

minio-stop:
	$(COMPOSE) stop minio

bootstrap-bucket:
	$(COMPOSE) run --rm --no-deps minio-init

test-minio:
	RUN_LIVE_TESTS=1 COMPOSE="$(COMPOSE)" $(PYTHON) -m unittest discover -s tests/integration -p test_minio.py -v

week1-smoke:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.lakehouse_smoke

week1-recovery:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.lakehouse_smoke --restart

bootstrap-bronze:
	$(COMPOSE) run --rm --no-deps bronze-bootstrap

bronze-smoke:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.bronze_smoke

down:
	$(COMPOSE) down

smoke-minio:
	$(COMPOSE) exec -T minio sh -c 'curl --fail --silent --show-error http://localhost:9000/minio/health/ready'
	@address=$$($(COMPOSE) port minio 9000) && test -n "$$address" && curl --fail --silent --show-error --max-time 10 "http://$$address/minio/health/ready"
	@printf '%s\n' 'PASS: MinIO readiness checks succeeded inside the container and through the published port.'

test:
	$(PYTHON) -m pytest tests/unit tests/integration -q

trino-config:
	$(COMPOSE) config --quiet

trino-up:
	$(COMPOSE) up -d --wait trino

trino-stop:
	$(COMPOSE) stop trino

trino-smoke:
	COMPOSE="$(COMPOSE)" $(PYTHON) scripts/trino_smoke.py --compose --check-stack --table "$(TABLE)" --expected-rows "$(EXPECTED_ROWS)"

dashboard-build:
	$(PYTHON) -m dashboard.grafana.build_dashboards

dashboard:
	$(COMPOSE) up -d --wait grafana

dashboard-smoke:
	$(PYTHON) scripts/grafana_smoke.py

.PHONY: realtime-up stream realtime-stop realtime-smoke realtime-recovery monitoring
REALTIME_ENV ?= .env
realtime-smoke:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.realtime_smoke --env-file "$(REALTIME_ENV)"

realtime-recovery:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.realtime_smoke --env-file "$(REALTIME_ENV)" --restart

monitoring:
	$(COMPOSE) --profile monitoring up -d --wait prometheus
	$(COMPOSE) restart grafana

realtime-up:
	$(COMPOSE) --profile kafka --profile realtime up -d --wait kafka clickhouse
	$(MAKE) kafka-topics
	$(COMPOSE) --profile kafka --profile realtime run --rm clickhouse-init

stream:
	$(COMPOSE) --profile kafka --profile realtime up -d stream

realtime-stop:
	$(COMPOSE) --profile kafka --profile realtime stop stream

ENTITIES ?= patients encounters lab_results medications vitals bed_events
INPUT_DIR ?= data/generated

verify-bronze:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.verify_bronze --compose --input-dir "$(INPUT_DIR)" --batch-id "$(BATCH_ID)" --entities $(ENTITIES)

gold:
	$(PYTHON) -m scripts.run_gold

# Historical Silver/Gold queries through Trino; fails on any error or empty result.
query:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.run_queries --compose

# Full Gold overwrite from Silver inside the Compose Spark image (GOLD_* from .env).
gold-docker:
	$(COMPOSE) run --rm gold

# Week 4 integration gate; start `make stream-bronze-docker` first.
GATE_RATE ?= 1000
GATE_DURATION ?= 600
stream-gate:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.stream_gate --compose --rate $(GATE_RATE) --duration $(GATE_DURATION)

stream-bronze-probe:
	$(PYTHON) -m scripts.stream_bronze_probe --compose --duration 300 --min-events 1

KAFKA_TOPICS ?= hospital.events
STARTING_OFFSETS ?= latest
CHECKPOINT_LOCATION ?= $(STREAM_CHECKPOINT_LOCATION)

stream-bronze:
	@test -n "$(KAFKA_BOOTSTRAP_SERVERS)" || (echo 'KAFKA_BOOTSTRAP_SERVERS is required' >&2; exit 2)
	@test -n "$(CHECKPOINT_LOCATION)" || (echo 'CHECKPOINT_LOCATION is required and must persist across restarts' >&2; exit 2)
	$(PYTHON) -m spark.streaming.kafka_to_bronze --bootstrap-servers "$(KAFKA_BOOTSTRAP_SERVERS)" --topics "$(KAFKA_TOPICS)" --starting-offsets "$(STARTING_OFFSETS)" --checkpoint-location "$(CHECKPOINT_LOCATION)"

STREAM_KAFKA_BOOTSTRAP_SERVERS ?= kafka:29092
STREAM_CHECKPOINT_LOCATION_DOCKER ?= /tmp/checkpoints/stream-bronze
stream-bronze-docker:
	STREAM_KAFKA_BOOTSTRAP_SERVERS="$(STREAM_KAFKA_BOOTSTRAP_SERVERS)" KAFKA_TOPICS="$(KAFKA_TOPICS)" KAFKA_STARTING_OFFSETS="$(STARTING_OFFSETS)" STREAM_CHECKPOINT_LOCATION_DOCKER="$(STREAM_CHECKPOINT_LOCATION_DOCKER)" $(COMPOSE) run --rm stream-bronze

test-stream-bronze:
	RUN_STREAM_BRONZE_TESTS=1 $(PYTHON) -m unittest discover -s tests/integration -p test_stream_bronze_iceberg.py -v

PATIENTS ?= 1000
SEED ?= 42
OUTPUT_DIR ?= data/generated
FORMAT ?= parquet

.PHONY: generate-data
generate-data:
	$(PYTHON) -m generators.batch_generator.generate --patients $(PATIENTS) --seed $(SEED) --output-dir "$(OUTPUT_DIR)" --format $(FORMAT)

SCALES ?= 10000 100000
GENERATOR_REPORT ?= results/generator-validation.json

.PHONY: verify-generated validate-generator
verify-generated:
	$(PYTHON) -m scripts.check_generator --input-dir "$(INPUT_DIR)"

validate-generator:
	$(PYTHON) -m scripts.check_generator --scales $(SCALES) --seed $(SEED) --report "$(GENERATOR_REPORT)"

batch-bronze:
	@test -n "$(BATCH_ID)" || (echo 'BATCH_ID is required' >&2; exit 2)
	$(PYTHON) -m spark.batch.ingest_bronze --input-dir "$(INPUT_DIR)" --batch-id "$(BATCH_ID)" --entities $(ENTITIES)

batch-bronze-docker:
	@test -n "$(BATCH_ID)" || (echo 'BATCH_ID is required' >&2; exit 2)
	BRONZE_INPUT_DIR="$(INPUT_DIR)" $(COMPOSE) run --rm bronze-ingest --input-dir /opt/hospital-input --batch-id "$(BATCH_ID)" --entities $(ENTITIES)

# Empty = every entity registered in spark/batch/bronze_to_silver.py.
SILVER_ENTITIES ?=
SILVER_ARGS = $(if $(strip $(SILVER_ENTITIES)),--entities $(SILVER_ENTITIES))

batch-silver:
	$(PYTHON) -m spark.batch.bronze_to_silver $(SILVER_ARGS)

batch-silver-docker:
	$(COMPOSE) run --rm silver-transform $(SILVER_ARGS)

# Same PATIENTS/SEED regenerate identical files, so the derived batch ID makes
# reruns skip committed Bronze tables; changed inputs need a new PIPELINE_BATCH_ID.
PIPELINE_BATCH_ID ?= synthetic-p$(PATIENTS)-s$(SEED)

batch:
	$(MAKE) generate-data OUTPUT_DIR="$(INPUT_DIR)"
	$(MAKE) bootstrap-bronze
	$(MAKE) batch-bronze-docker BATCH_ID="$(PIPELINE_BATCH_ID)"
	$(MAKE) verify-bronze BATCH_ID="$(PIPELINE_BATCH_ID)"
	$(MAKE) batch-silver-docker
	$(MAKE) gold-docker

RATE ?= 1000
DURATION ?= 60
MAX_EVENTS ?= 20

produce:
	@test -n "$(KAFKA_BOOTSTRAP_SERVERS)" || (echo 'KAFKA_BOOTSTRAP_SERVERS is required' >&2; exit 2)
	$(PYTHON) -m generators.stream_generator.producer --bootstrap-servers "$(KAFKA_BOOTSTRAP_SERVERS)" --topic "$(KAFKA_TOPICS)" --rate $(RATE) --duration $(DURATION)

produce-dry-run:
	$(PYTHON) -m generators.stream_generator.producer --dry-run --rate $(RATE) --max-events $(MAX_EVENTS)

test-bronze:
	RUN_BRONZE_TESTS=1 $(PYTHON) -m unittest discover -s tests/integration -p test_bronze_ingestion.py -v

# Explicit services avoid starting/rebuilding unrelated batch infrastructure.
kafka-up:
	$(COMPOSE) up -d --wait --wait-timeout 180 kafka
	$(MAKE) kafka-topics

kafka-topics:
	$(COMPOSE) run --rm --no-deps kafka-init

kafka-stop:
	$(COMPOSE) stop kafka

kafka-smoke:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.kafka_smoke

kafka-recovery:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m scripts.kafka_smoke --restart

# Checkpoint 1 Beamer deck (TeX Live with xelatex, latexmk, Metropolis and Fira fonts).
slides:
	latexmk -xelatex -interaction=nonstopmode -outdir=results/slides docs/slides/checkpoint-1.tex

# Beginner companion; same XeLaTeX/Fira dependencies as the checkpoint deck.
slides-explained:
	latexmk -xelatex -halt-on-error -interaction=nonstopmode -outdir=results/slides docs/slides/hospitallake-explained.tex

# English milestone report: stock Beamer theme/fonts/colors, pdfLaTeX.
slides-report:
	latexmk -pdf -halt-on-error -interaction=nonstopmode -outdir=results/slides docs/slides/checkpoint-1-report-en.tex

# Optional presentation tooling; independent from the data pipeline.
video-setup:
	cd docs/video && npm ci --ignore-scripts --no-audit --no-fund

video-voice:
	$(VIDEO_PYTHON) docs/video/prepare.py voice

video-timeline:
	$(VIDEO_PYTHON) docs/video/prepare.py align

video-preview:
	cd docs/video && npm run check
	cd docs/video && npm run stills

video:
	cd docs/video && npm run check
	cd docs/video && VIDEO_CONCURRENCY=$(VIDEO_CONCURRENCY) npm run render
	$(PYTHON) docs/video/publish.py

# Persistent VPS demo. Credentials are supplied through an untracked env file.
LIVE_ENV ?= homecoming.env
LIVE_PROJECT ?= hospital-live
LIVE_COMPOSE = $(COMPOSE) -p $(LIVE_PROJECT) --env-file $(LIVE_ENV) -f docker-compose.yml -f infra/deploy/compose.live.yml --profile kafka --profile realtime --profile monitoring
.PHONY: live-up live-stop live-status live-check
live-up:
	$(LIVE_COMPOSE) up -d --no-build minio iceberg-rest trino kafka kafka-init clickhouse clickhouse-init stream grafana prometheus hospital-api live-batch tunnel
live-stop:
	$(LIVE_COMPOSE) stop
live-status:
	$(LIVE_COMPOSE) ps
live-check:
	$(PYTHON) -m scripts.live_gate --env-file $(LIVE_ENV) --project $(LIVE_PROJECT) --restart

.PHONY: k8s-recovery
k8s-recovery:
	$(PYTHON) -m scripts.k8s_recovery --rate $(K8S_RATE)

.PHONY: stream-clinical test-clinical-stream test-clinical-kafka

stream-clinical:
	$(PYTHON) -m spark.streaming.clinical_metrics --state-dir "$(CLINICAL_STATE_DIR)" $(CLINICAL_ARGS)

test-clinical-stream:
	RUN_CLINICAL_STREAM_TESTS=1 $(PYTHON) -m pytest tests/integration/test_clinical_metrics_stream.py -q

test-clinical-kafka:
	RUN_CLINICAL_KAFKA_TESTS=1 $(PYTHON) -m pytest tests/integration/test_clinical_metrics_kafka.py -q

.PHONY: clickhouse-up test-clickhouse
clickhouse-up:
	$(COMPOSE) -p hospital-revision --project-directory . -f infra/docker/clickhouse/serving.yml --profile serving up -d --wait clickhouse

test-clickhouse:
	RUN_CLICKHOUSE_TESTS=1 $(PYTHON) -m pytest tests/integration/test_clickhouse_serving.py -q

.PHONY: k8s-observability-up
k8s-observability-up:
	$(PYTHON) scripts/deploy_observability_k8s.py
