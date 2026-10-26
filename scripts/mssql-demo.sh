#!/usr/bin/env bash
# The `make mssql-demo` flow: the SQL Server path end to end (SQL Server CDC read by Spark with
# mssql-cdc-pyspark, no Debezium, no Kafka). Run it instead of `make demo`, never next to it.
# On success the stack stays up; `make down` removes it with its volumes.
#
# Running it means accepting Microsoft's SQL Server Developer EULA: set MSSQL_ACCEPT_EULA=Y.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${MSSQL_ACCEPT_EULA:-}" != "Y" ]; then
    cat >&2 <<'EOF'
This demo starts SQL Server 2022 Developer edition from Microsoft's container image.
Using it means accepting the SQL Server EULA (https://go.microsoft.com/fwlink/?linkid=857698).
If you accept it, run:  MSSQL_ACCEPT_EULA=Y make mssql-demo
EOF
    exit 1
fi
export MSSQL_ACCEPT_EULA

total=11
step=0
banner() {
    step=$((step + 1))
    printf '\n==> [%d/%d] %s\n' "$step" "$total" "$1"
}

profiles=(--profile mssql)
lake=s3a://cdc-demo/mssql
manifest=/runs/mssql-demo.json

on_error() {
    code=$?
    trap - ERR
    printf '\n!! Demo failed (exit %d) at step %d. Diagnostics:\n' "$code" "$step" >&2
    docker compose "${profiles[@]}" ps -a >&2 || true
    for service in mssql mssql-init mssql-consumer; do
        printf '\n--- last 50 log lines: %s ---\n' "$service" >&2
        docker compose "${profiles[@]}" logs --no-color --tail 50 "$service" >&2 || true
    done
    exit "$code"
}
trap on_error ERR

banner "Accepted: Microsoft SQL Server EULA (MSSQL_ACCEPT_EULA=Y)"

banner "Removing any previous stack and its volumes"
docker compose --profile tools --profile ui --profile mssql down -v --remove-orphans

banner "Building the Spark and analytics images"
# `consumer` builds the cdc-demo-spark:local image that mssql-consumer and mssql-job run.
docker compose --profile tools build consumer analytics

banner "Starting SQL Server, its setup and the consumer"
docker compose "${profiles[@]}" up -d --wait mssql-consumer

banner "Waiting for the 500-row snapshot to reach the CDC log"
deadline=$((SECONDS + 300))
until docker compose "${profiles[@]}" logs --no-color mssql-consumer 2>&1 | grep -q '"snapshot_written"'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "Timed out waiting for snapshot_written" >&2
        exit 1
    fi
    sleep 3
done
echo "snapshot_written seen"

banner "Applying 200 seeded mutations in SQL Server"
docker compose "${profiles[@]}" run --rm mssql-job mutate --target mssql \
    --count 200 --seed 42 --manifest "$manifest"

banner "Verifying SQL Server and the Delta tables agree"
docker compose "${profiles[@]}" run --rm mssql-job verify --source mssql \
    --manifest "$manifest" --timeout 300

banner "Merging the CDC log into the current table (twice: the second run must change nothing)"
docker compose "${profiles[@]}" run --rm mssql-job merge
# Capture, then print: `tee /dev/stderr` reopens a redirected log file and truncates it.
second=$(docker compose "${profiles[@]}" run --rm -T mssql-job merge)
printf '%s\n' "$second"
grep -q 'inserted 0, updated 0, deleted 0' <<<"$second"

banner "Verifying the current table equals SQL Server row by row"
docker compose "${profiles[@]}" run --rm mssql-job verify-current --source mssql

banner "Building and verifying the dbt marts on the SQL Server lake"
docker compose "${profiles[@]}" run --rm -e "LAKE_ROOT=${lake}" analytics build-marts
docker compose "${profiles[@]}" run --rm analytics verify-marts --manifest "$manifest"

banner "Lag and memory"
docker compose "${profiles[@]}" run --rm mssql-job lag-report
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'

printf '\nSQL Server demo passed. Stack is still up (make lag, make stats, make down).\n'
