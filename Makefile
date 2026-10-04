PYTHON ?= python

.PHONY: help test trino-config trino-up trino-stop trino-smoke

help:
	@$(PYTHON) -c "print('make test\nmake trino-config\nmake trino-up\nmake trino-stop\nmake trino-smoke TABLE=iceberg.bronze.spark_smoke EXPECTED_ROWS=3')"

test:
	$(PYTHON) -m unittest discover -s tests/unit -v

trino-config:
	docker compose config --quiet

trino-up:
	docker compose up -d --wait trino

trino-stop:
	docker compose stop trino

trino-smoke:
	$(PYTHON) scripts/trino_smoke.py --compose --check-stack --table "$(TABLE)" --expected-rows "$(EXPECTED_ROWS)"
