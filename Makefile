init: lock
	poetry install --all-extras

test:
	poetry run pytest

lock:
	poetry lock

run-pinot:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 apachepinot/pinot:latest QuickStart -type batch

.PHONY: init test lock run-pinot
