SHELL := bash
.DEFAULT_GOAL := help

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.svg (committed). One pinned D2 everywhere.
# Layout engine, themes and padding live in docs/diagrams/_style.d2 (d2-config), so d2 runs in a
# clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))

.PHONY: help install lint fmt proto test-fast test-spark test up down demo mssql-demo lag stats merge marts dashboard ps logs ui d2-install diagrams diagrams-check diagrams-watch

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'

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

# Strict shell flags for the diagram targets only: a failed render inside the loop must fail the
# target. The other targets keep the default flags (`lag` pipes into `grep -q`, which pipefail
# could turn into a false negative).
d2-install diagrams diagrams-check diagrams-watch: .SHELLFLAGS := -eu -o pipefail -c

d2-install: ## install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## render docs/diagrams/*.d2 to SVG with the pinned D2 (run make d2-install once)
	@v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
		echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
	@for f in $(DIAGRAMS); do $(D2_RUN) "$$f" "$${f%.d2}.svg"; done

diagrams-check: diagrams ## render, then fail if a committed SVG is stale, missing or has no source
	@shopt -s nullglob; for s in docs/diagrams/*.svg; do \
		[ -f "$${s%.svg}.d2" ] || { echo "$$s has no .d2 source" >&2; exit 1; }; done
	@changed="$$(git status --porcelain --untracked-files=all -- 'docs/diagrams/*.svg')"; \
	if [ -n "$$changed" ]; then echo "$$changed" >&2; \
		echo "rendered SVGs differ from the committed ones: run make diagrams and commit" >&2; exit 1; fi

diagrams-watch: ## live preview in the browser while editing: make diagrams-watch NAME=architecture
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).svg
