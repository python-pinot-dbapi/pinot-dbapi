init: poetry lock
	poetry install --all-extras --with=dev

test:
	poetry run pytest

check-pinot:
	@command -v docker >/dev/null 2>&1 || (echo "docker is required for integration tests. Install Docker Desktop, then run: make run-pinot" && exit 1)
	@docker inspect -f '{{.State.Running}}' pinot-quickstart >/dev/null 2>&1 || (echo "Pinot quickstart container 'pinot-quickstart' is not running. Start it first in another terminal: make run-pinot" && exit 1)
	@docker inspect -f '{{.State.Running}}' pinot-quickstart 2>/dev/null | grep -q true || (echo "Pinot quickstart container 'pinot-quickstart' exists but is not running. Start it: docker start pinot-quickstart (or re-create it via: make run-pinot)" && exit 1)

test-integration: check-pinot
	poetry run pytest tests/integration/

test-unit:
	poetry run pytest -s tests/unit/

lint:
	poetry run flake8 pinotdb

lock:
	poetry lock

poetry:
	pip install poetry

run-pinot:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 apachepinot/pinot:latest QuickStart -type MULTI_STAGE

.PHONY: init test test-integration test-unit lint lock poetry run-pinot check-pinot
