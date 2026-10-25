#!/usr/bin/env bash
# Usage: wait-for-snapshot.sh N TIMEOUT
# Blocks until the CDC topic's end offsets sum to at least N (the Debezium snapshot is done).
set -euo pipefail

expected="${1:?usage: wait-for-snapshot.sh N TIMEOUT}"
timeout="${2:?usage: wait-for-snapshot.sh N TIMEOUT}"
topic="${CDC_TOPIC:-cdc.public.orders}"

deadline=$((SECONDS + timeout))
total=0
while [ "$SECONDS" -lt "$deadline" ]; do
    # Output is topic:partition:offset per line; the topic does not exist until the snapshot starts.
    total=$(docker compose exec -T kafka kafka-get-offsets \
        --bootstrap-server localhost:29092 --topic "$topic" 2>/dev/null \
        | awk -F: '{ s += $3 } END { print s + 0 }' || true)
    if [ "$total" -ge "$expected" ]; then
        echo "Topic ${topic} holds ${total} events (wanted ${expected})"
        exit 0
    fi
    echo "Topic ${topic}: ${total}/${expected} events"
    sleep 3
done

echo "Timed out after ${timeout}s: topic ${topic} holds ${total}/${expected} events" >&2
exit 1
