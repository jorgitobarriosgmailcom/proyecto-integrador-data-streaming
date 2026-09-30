.PHONY: install test lint up produce results demo stop
install:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check app tests

up:
	docker compose up -d --build kafka topic-init pipeline

produce:
	docker compose --profile tools run --rm producer

results:
	docker compose --profile tools run --rm consumer

demo:
	bash scripts/e2e_smoke.sh

stop:
	bash scripts/stop.sh
