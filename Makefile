PYTHON ?= python3
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then printf 'docker compose'; else printf 'docker-compose'; fi)

.PHONY: help setup up down config smoke-minio test trino-config trino-up trino-stop trino-smoke verify-bronze gold
.DEFAULT_GOAL := help

help:
	@printf '%s\n' 'make setup       Create .env without replacing an existing file' 'make config      Validate Compose configuration' 'make up          Start local services and wait for health checks' 'make down        Stop local services; preserve data volumes' 'make smoke-minio Verify MinIO readiness through its published port' 'make test        Run unit and verifier contract tests' 'make trino-config Validate Compose configuration' 'make trino-up    Start Trino and wait for readiness' 'make trino-stop  Stop Trino' 'make trino-smoke TABLE=iceberg.bronze.spark_smoke EXPECTED_ROWS=3' 'make verify-bronze BATCH_ID=<id> [ENTITIES="patients encounters ..."]' 'make gold        Submit Silver-to-Gold Spark batch job'

setup:
	@test -f .env || cp .env.example .env

config:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up -d --wait --wait-timeout 90

down:
	$(COMPOSE) down

smoke-minio:
	$(COMPOSE) exec -T minio sh -c 'curl --fail --silent --show-error http://localhost:9000/minio/health/ready'
	@address=$$($(COMPOSE) port minio 9000) && test -n "$$address" && curl --fail --silent --show-error --max-time 10 "http://$$address/minio/health/ready"
	@printf '%s\n' 'PASS: MinIO readiness checks succeeded inside the container and through the published port.'

test:
	$(PYTHON) -m unittest discover -s tests/unit -v
	$(PYTHON) -m unittest discover -s tests/integration -v

trino-config:
	$(COMPOSE) config --quiet

trino-up:
	$(COMPOSE) up -d --wait trino

trino-stop:
	$(COMPOSE) stop trino

trino-smoke:
	$(PYTHON) scripts/trino_smoke.py --compose --check-stack --table "$(TABLE)" --expected-rows "$(EXPECTED_ROWS)"

ENTITIES ?= patients encounters lab_results medications vitals bed_events
INPUT_DIR ?= data/generated

verify-bronze:
	$(PYTHON) -m scripts.verify_bronze --compose --input-dir "$(INPUT_DIR)" --batch-id "$(BATCH_ID)" --entities $(ENTITIES)

gold:
	$(PYTHON) -m scripts.run_gold
