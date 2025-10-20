#!/usr/bin/env bash
set -euo pipefail

TOPIC="${TOPIC_SMOKE:-smoke.test}"

echo "Ensuring topic exists: $TOPIC"
docker exec kafka kafka-topics --bootstrap-server kafka:9093 \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 >/dev/null

MSG1="hello-$(date +%s)"
MSG2="world-$(date +%s)"
MSG3="from-smoke-test-$(date +%s)"

echo "Producing 3 messages to $TOPIC (inside container)"
printf "%s\n%s\n%s\n" "$MSG1" "$MSG2" "$MSG3" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka:9093 \
  --topic "$TOPIC" >/dev/null

echo "Consuming back 3 messages (inside container)"
OUT=$(docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9093 \
  --topic "$TOPIC" \
  --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null | tr -d '\r')

echo "----- consumed -----"
echo "$OUT"
echo "--------------------"

if echo "$OUT" | grep -q "$MSG1" && echo "$OUT" | grep -q "$MSG2" && echo "$OUT" | grep -q "$MSG3"; then
  echo "✓ Smoke test passed."
  exit 0
else
  echo "✗ Smoke test failed."
  exit 2
fi

