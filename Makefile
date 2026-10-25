SHELL := bash
.DEFAULT_GOAL := help

.PHONY: help install lint fmt proto test-fast test-spark test up down demo merge ps logs ui

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## Install all dependencies, extras included
	uv sync --all-extras

lint: ## Ruff check, ruff format check, mypy (same as the CI lint job)
	uv run ruff check
	uv run ruff format --check
	uv run mypy

fmt: ## Format and apply safe lint fixes
	uv run ruff format
	uv run ruff check --fix

proto: ## Recompile the Protobuf descriptor set from the .proto
	uv run python scripts/compile_proto.py

test-fast: ## Unit tests, no JVM
	uv run pytest -m "not spark and not integration"

# Linux, WSL and CI only: Spark does not run on Windows Python.
test-spark: ## Spark tests (local session)
	uv run pytest -m spark

test: test-fast test-spark ## Everything

up: ## Build every image (tools profile included) and start the stack in the background
	docker compose --profile tools build
	docker compose up -d --wait

down: ## Stop the stack and delete its volumes
	docker compose --profile tools --profile ui down -v --remove-orphans

demo: ## End-to-end demo on fresh volumes
	bash scripts/demo.sh

merge: ## Merge the CDC log into the current table (stack must be up)
	docker compose run --rm spark-job merge

ps: ## Show service status
	docker compose ps

logs: ## Follow the logs of every service
	docker compose logs -f --tail 100

ui: ## Start Kafka UI (http://localhost:8090)
	docker compose --profile ui up -d kafka-ui
