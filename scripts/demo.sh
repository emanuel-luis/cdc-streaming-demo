#!/usr/bin/env bash
# The `make demo` flow: fresh stack, snapshot, mutations, end-to-end verification.
# On success the stack stays up for exploration; `make down` removes it with its volumes.
set -euo pipefail
cd "$(dirname "$0")/.."

step=0
banner() {
    step=$((step + 1))
    printf '\n==> [%d/8] %s\n' "$step" "$1"
}

on_error() {
    code=$?
    trap - ERR
    printf '\n!! Demo failed (exit %d) at step %d. Diagnostics:\n' "$code" "$step" >&2
    docker compose ps -a >&2 || true
    for service in connect consumer; do
        printf '\n--- last 50 log lines: %s ---\n' "$service" >&2
        docker compose logs --no-color --tail 50 "$service" >&2 || true
    done
    exit "$code"
}
trap on_error ERR

banner "Removing any previous stack and its volumes"
docker compose --profile tools --profile ui down -v --remove-orphans

banner "Building images and starting the stack"
# `up` skips the tools profile (mutator, spark-job) and `run` only builds a missing image,
# never a stale one, so every image is built here.
docker compose --profile tools build
docker compose up -d --wait

banner "Waiting for the 500-row snapshot to reach Kafka"
bash scripts/wait-for-snapshot.sh 500 300

banner "Checking the checked-in Protobuf schema against Schema Registry"
docker compose run --rm mutator check-schema

banner "Applying 200 seeded mutations in Postgres"
docker compose run --rm mutator mutate --count 200 --seed 42 --manifest /runs/demo.json

banner "Verifying Postgres, Kafka and the Delta tables agree"
docker compose run --rm spark-job verify --manifest /runs/demo.json --timeout 300

banner "Merging the CDC log into the current table (twice: the second run must change nothing)"
docker compose run --rm spark-job merge
# Capture, then print: `tee /dev/stderr` reopens a redirected log file and truncates it.
second=$(docker compose run --rm -T spark-job merge)
printf '%s\n' "$second"
grep -q 'inserted 0, updated 0, deleted 0' <<<"$second"

banner "Verifying the current table equals Postgres row by row"
docker compose run --rm spark-job verify-current

printf '\nDemo passed. Stack is still up (make logs, make ui, make down).\n'
