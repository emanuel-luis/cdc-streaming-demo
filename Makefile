SHELL := bash
.DEFAULT_GOAL := help

.PHONY: help install lint fmt proto test-fast test-spark test up down demo mssql-demo lag stats merge marts dashboard ps logs ui

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## Install all dependencies, extras included
	uv sync --all-extras

lint: ## Ruff, mypy, dbt parse (same as the CI lint job)
	uv run ruff check
	uv run ruff format --check
	uv run mypy
	uv run dbt parse --project-dir dbt --profiles-dir dbt

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
	docker compose --profile tools --profile ui --profile mssql down -v --remove-orphans

demo: ## End-to-end demo on fresh volumes
	bash scripts/demo.sh

mssql-demo: ## SQL Server path end to end (needs MSSQL_ACCEPT_EULA=Y; see README)
	bash scripts/mssql-demo.sh

# Picks the stack that is up: the SQL Server one when its server runs, otherwise Debezium's.
lag: ## p50, p95 and max lag of the running stack's CDC log
	@if docker compose --profile mssql ps --status running --services | grep -qx mssql; then \
		docker compose --profile mssql run --rm mssql-job lag-report; \
	else \
		docker compose run --rm spark-job lag-report; \
	fi

stats: ## Memory use of every running container (one sample)
	docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'

merge: ## Merge the CDC log into the current table (stack must be up)
	docker compose run --rm spark-job merge

marts: ## Build the DuckDB marts with dbt (stack up, current table merged)
	docker compose --profile tools build analytics
	docker compose run --rm analytics build-marts

dashboard: ## Start the dashboard (http://localhost:8501)
	docker compose --profile tools build analytics
	docker compose --profile ui up -d --wait dashboard

ps: ## Show service status
	docker compose ps

logs: ## Follow the logs of every service
	docker compose logs -f --tail 100

ui: ## Start Kafka UI (http://localhost:8090)
	docker compose --profile ui up -d kafka-ui
