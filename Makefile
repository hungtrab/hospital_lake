COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then printf 'docker compose'; else printf 'docker-compose'; fi)

.PHONY: help setup up down config smoke-minio
.DEFAULT_GOAL := help

help:
	@printf '%s\n' 'make setup       Create .env without replacing an existing file' 'make config      Validate Compose configuration' 'make up          Start local services and wait for health checks' 'make down        Stop local services; preserve data volumes' 'make smoke-minio Verify MinIO readiness through its published port'

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
