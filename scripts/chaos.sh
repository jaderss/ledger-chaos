#!/bin/sh
set -eu

MODE=${1:-${LEDGER_MODE:-correct}}
SCENARIO=${2:-${SCENARIO:-contention}}
LOCKING=${LEDGER_LOCKING:-ordered}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$MODE-$LOCKING-$SCENARIO}
HOST_USER=$(id -u):$(id -g)
export LEDGER_MODE=$MODE SCENARIO RUN_ID HOST_USER

echo "run $RUN_ID: mode $MODE, locking $LOCKING, scenario $SCENARIO"
docker compose --profile tools down --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --wait postgres kafka toxiproxy
docker compose run --rm db-init >/dev/null
docker compose run --rm kafka-topics >/dev/null
docker compose up -d --build --wait ledger relay projector
docker compose exec -T postgres psql -U postgres -d ledger -qtAc "select pg_stat_reset()" >/dev/null
mkdir -p "results/$RUN_ID"

docker compose --profile tools build chaos checker >/dev/null
docker compose run --rm chaos
docker compose run --rm checker
