#!/bin/sh
# One-shot, runs in the curl image: create or update the Debezium connector, then wait until it
# and its task report RUNNING. PUT is create-or-update, so re-running on every `up` is safe.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONFIG_FILE="${CONFIG_FILE:-/config/orders-connector.json}"
NAME="orders-connector"
TIMEOUT="${REGISTER_TIMEOUT:-180}"

echo "Waiting for Kafka Connect at ${CONNECT_URL}"
curl -fsS --retry 60 --retry-delay 2 --retry-all-errors --retry-connrefused \
    -o /dev/null "${CONNECT_URL}/connectors"

echo "Registering ${NAME}"
# The worker can still be rebalancing right after it starts answering, hence the retries.
curl -fsS --retry 10 --retry-delay 3 --retry-all-errors \
    -X PUT -H "Content-Type: application/json" --data "@${CONFIG_FILE}" \
    -o /dev/null "${CONNECT_URL}/connectors/${NAME}/config"

elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
    status=$(curl -fsS "${CONNECT_URL}/connectors/${NAME}/status" 2>/dev/null || true)
    if echo "$status" | grep -q '"state":"FAILED"'; then
        echo "Connector or task FAILED:" >&2
        echo "$status" >&2
        exit 1
    fi
    # Two RUNNING states: the connector and its single task (tasks.max=1).
    running=$(echo "$status" | grep -o '"state":"RUNNING"' | wc -l)
    if [ "$running" -ge 2 ]; then
        echo "${NAME} is RUNNING"
        exit 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
done

echo "Timed out after ${TIMEOUT}s waiting for ${NAME}. Last status:" >&2
echo "${status:-<none>}" >&2
exit 1
