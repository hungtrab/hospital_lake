# HospitalLake — top-level command interface (plan.md §25).
# Targets are added milestone by milestone; run `make help` to see what exists.

PYTHON     ?= python3
PATIENTS   ?= 1000
SEED       ?= 42
OUTPUT_DIR ?= data/generated

.DEFAULT_GOAL := help
.PHONY: help setup generate-data test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

generate-data: ## Generate synthetic batch Parquet (PATIENTS=1000 SEED=42 OUTPUT_DIR=data/generated)
	$(PYTHON) -m generators.batch_generator.generate --patients $(PATIENTS) --seed $(SEED) --output-dir $(OUTPUT_DIR)

test: ## Run unit tests
	$(PYTHON) -m pytest tests/unit -q
