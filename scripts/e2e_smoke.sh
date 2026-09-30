#!/usr/bin/env bash
set -euo pipefail
mkdir -p evidence/runtime
LOG=evidence/runtime/e2e_$(date +%Y%m%d_%H%M%S).log
exec > >(tee "$LOG") 2>&1

echo "[1/5] Starting Kafka, topic init and Beam pipeline"
docker compose up -d --build kafka topic-init pipeline

echo "[2/5] Waiting for pipeline startup"
sleep 18

echo "[3/5] Producing normal, duplicate, out-of-order and invalid events"
docker compose --profile tools run --rm producer

echo "[4/5] Waiting for processing"
sleep 20

echo "[5/5] Reading idempotent sink"
docker compose --profile tools run --rm consumer

echo "--- pipeline logs (tail) ---"
docker compose logs --no-color --tail=120 pipeline

echo "E2E_SMOKE_COMPLETED"
