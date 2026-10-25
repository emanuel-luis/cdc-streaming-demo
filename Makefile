SHELL := bash
.DEFAULT_GOAL := help

.PHONY: help install lint fmt test-fast test-spark test up down demo ps logs ui

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

test-fast: ## Unit tests, no JVM
	uv run pytest -m "not spark and not integration"

# Linux, WSL and CI only: Spark does not run on Windows Python.
test-spark: ## Spark tests (local session)
	uv run pytest -m spark

test: test-fast test-spark ## Everything

up: ## Start the stack in the background
	docker compose up -d --build --wait

down: ## Stop the stack and delete its volumes
	docker compose down -v --remove-orphans

demo: ## End-to-end demo on fresh volumes
	bash scripts/demo.sh

ps: ## Show service status
	docker compose ps

logs: ## Follow the logs of every service
	docker compose logs -f --tail 100

ui: ## Start Kafka UI (http://localhost:8090)
	docker compose --profile ui up -d kafka-ui
