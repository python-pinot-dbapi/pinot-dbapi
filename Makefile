init: poetry lock
	poetry install --all-extras --with=dev

test:
	poetry run pytest

PINOT_HOST ?= localhost
PINOT_BROKER_PORT ?= 8000
PINOT_CONTROLLER_PORT ?= 9000

check-pinot:
	@command -v nc >/dev/null 2>&1 || (echo "nc (netcat) is required to precheck Pinot connectivity for integration tests." && exit 1)
	@HOST="$(PINOT_HOST)"; BROKER_PORT="$(PINOT_BROKER_PORT)"; CONTROLLER_PORT="$(PINOT_CONTROLLER_PORT)"; \
		missing=""; \
		nc -z -w 2 "$$HOST" "$$BROKER_PORT" >/dev/null 2>&1 || missing="$$missing broker=$$HOST:$$BROKER_PORT"; \
		nc -z -w 2 "$$HOST" "$$CONTROLLER_PORT" >/dev/null 2>&1 || missing="$$missing controller=$$HOST:$$CONTROLLER_PORT"; \
		if [ -n "$$missing" ]; then \
			echo "Pinot is not reachable ($$missing)."; \
			echo "Start Pinot (local quickstart): make run-pinot"; \
			echo "Or set PINOT_HOST / PINOT_BROKER_PORT / PINOT_CONTROLLER_PORT to point to an existing Pinot cluster."; \
			exit 1; \
		fi

test-integration: check-pinot
	poetry run pytest tests/integration/

test-unit:
	poetry run pytest -s tests/unit/

coverage:
	poetry run pytest -s tests/unit/ --cov-report=term-missing --cov-report=xml:coverage.xml --cov-report=json:coverage.json

lint:
	poetry run flake8 pinotdb

lock:
	poetry lock

poetry:
	pip install poetry

run-pinot:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 apachepinot/pinot:latest QuickStart -type MULTI_STAGE

.PHONY: init test test-integration test-unit coverage lint lock poetry run-pinot check-pinot
