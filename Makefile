PYTHON ?= python3
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then printf 'docker compose'; else printf 'docker-compose'; fi)

.PHONY: kafka-recovery kafka-up kafka-stop kafka-topics kafka-smoke batch batch-silver batch-silver-docker batch-bronze batch-bronze-docker test-bronze test-stream-bronze help setup up down config minio-up minio-stop bootstrap-bucket bootstrap-bronze bronze-smoke week1-smoke week1-recovery smoke-minio test-minio test trino-config trino-up trino-stop trino-smoke verify-bronze gold gold-docker stream-bronze stream-bronze-docker stream-bronze-probe produce produce-dry-run query slides stream-gate
.DEFAULT_GOAL := help

help:
	@printf '%s\n' 'make kafka-topics  Safely create/validate Kafka topics' 'make kafka-recovery PYTHON=.venv/bin/python  Verify data/offset persistence after broker recreation'
	@printf '%s\n' 'make kafka-up  Start optional local KRaft broker and bootstrap topics' 'make kafka-smoke PYTHON=.venv/bin/python  Verify host/internal keyed produce-consume' 'make kafka-stop  Stop Kafka; preserve its data volume'
	@printf '%s\n' 'make batch-bronze BATCH_ID=demo-001 [INPUT_DIR=data/generated] [ENTITIES=patients]' 'make test-bronze Run local Spark/Iceberg ingestion checks'
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
