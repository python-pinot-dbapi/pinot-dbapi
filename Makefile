init: poetry lock
	poetry install --all-extras --with=dev

test:
	poetry run pytest

test-integration:
	poetry run pytest tests/integration/

test-unit:
	poetry run pytest tests/unit/

lint:
	poetry run flake8 pinotdb

lock:
	poetry lock

poetry:
	pip install poetry

run-pinot:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 apachepinot/pinot:latest QuickStart -type batch

.PHONY: init test lock poetry run-pinot
