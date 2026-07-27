#!/usr/bin/env bash
set -euo pipefail

# Idempotent topic bootstrap. Never relies on broker auto-create: topics are
# created explicitly here with known partition counts, so ownership and
# partitioning are always intentional, not accidental.

BROKER="${KAFKA_BROKER:-kafka:9092}"
PARTITIONS=3
REPLICATION_FACTOR=1

TOPICS=(
  jobs.requested
  jobs.started
  jobs.completed
  jobs.failed
  jobs.retry-1
  jobs.retry-2
  jobs.retry-3
  jobs.dead-letter
)

echo "Waiting for Kafka at ${BROKER}..."
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BROKER}" >/dev/null

for topic in "${TOPICS[@]}"; do
  echo "Ensuring topic: ${topic}"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo "Topics ready:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BROKER}" --list
