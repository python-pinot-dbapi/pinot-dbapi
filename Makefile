init: lock
	poetry install

test:
	tox

lock:
	poetry lock

.PHONY: init test lock
