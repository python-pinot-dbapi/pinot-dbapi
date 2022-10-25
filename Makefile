init: lock
	poetry install --all-extras

test:
	tox

lock:
	poetry lock

.PHONY: init test lock
