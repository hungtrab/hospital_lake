PYTHON ?= python3
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then printf 'docker compose'; else printf 'docker-compose'; fi)

.PHONY: help setup up down config minio-up minio-stop smoke-minio test-minio test trino-config trino-up trino-stop trino-smoke
.DEFAULT_GOAL := help

help:
	@printf '%s\n' 'make minio-up    Build and start MinIO only; wait for readiness' 'make minio-stop  Stop MinIO; preserve its volume' 'make test-minio  Live S3 write/read and container recreation test'
	@printf '%s\n' 'make setup       Create .env without replacing an existing file' 'make config      Validate Compose configuration' 'make up          Start local services and wait for health checks' 'make down        Stop local services; preserve data volumes' 'make smoke-minio Verify MinIO readiness through its published port' 'make test        Run unit tests' 'make trino-config Validate Compose configuration' 'make trino-up    Start Trino and wait for readiness' 'make trino-stop  Stop Trino' 'make trino-smoke TABLE=iceberg.bronze.spark_smoke EXPECTED_ROWS=3'

setup:
	@test -f .env || cp .env.example .env

config:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up -d --wait --wait-timeout 90

down:
	$(COMPOSE) down

minio-up:
	$(COMPOSE) up -d --build --wait --wait-timeout 90 minio

minio-stop:
	$(COMPOSE) stop minio

test-minio:
	COMPOSE="$(COMPOSE)" $(PYTHON) -m unittest discover -s tests/integration -p test_minio.py -v

smoke-minio:
	$(COMPOSE) exec -T minio sh -c 'curl --fail --silent --show-error http://localhost:9000/minio/health/ready'
	@address=$$($(COMPOSE) port minio 9000) && test -n "$$address" && curl --fail --silent --show-error --max-time 10 "http://$$address/minio/health/ready"
	@printf '%s\n' 'PASS: MinIO readiness checks succeeded inside the container and through the published port.'

test:
	$(PYTHON) -m unittest discover -s tests/unit -v

trino-config:
	$(COMPOSE) config --quiet

trino-up:
	$(COMPOSE) up -d --wait trino

trino-stop:
	$(COMPOSE) stop trino

trino-smoke:
	$(PYTHON) scripts/trino_smoke.py --compose --check-stack --table "$(TABLE)" --expected-rows "$(EXPECTED_ROWS)"
