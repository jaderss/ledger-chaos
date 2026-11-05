#!/bin/bash
set -u
cd "$(dirname "$0")/.."

PREFIX=${PREFIX:-$(date -u +%Y%m%dT%H%M%SZ)}
DURATION=${DURATION:-60s}
LOCK_DURATION=${LOCK_DURATION:-180s}
failed=0

run() {
  local id=$PREFIX-$1
  shift
  env DURATION="$DURATION" "$@" RUN_ID="$id" scripts/chaos.sh || failed=1
}

run lock-ordered-20 LEDGER_MODE=correct LEDGER_LOCKING=ordered SCENARIO=contention ACCOUNTS=20 DURATION="$LOCK_DURATION"
run lock-serializable-20 LEDGER_MODE=correct LEDGER_LOCKING=serializable SCENARIO=contention ACCOUNTS=20 DURATION="$LOCK_DURATION"
run lock-ordered-200 LEDGER_MODE=correct LEDGER_LOCKING=ordered SCENARIO=contention ACCOUNTS=200 DURATION="$LOCK_DURATION"
run lock-serializable-200 LEDGER_MODE=correct LEDGER_LOCKING=serializable SCENARIO=contention ACCOUNTS=200 DURATION="$LOCK_DURATION"
run correct-ordered-all LEDGER_MODE=correct LEDGER_LOCKING=ordered SCENARIO=all FAULT_INTERVAL=8s
run correct-serializable-all LEDGER_MODE=correct LEDGER_LOCKING=serializable SCENARIO=all FAULT_INTERVAL=8s
run correct-ordered-db-latency LEDGER_MODE=correct SCENARIO=db-latency FAULT_INTERVAL=12s FAULT_DURATION=8s
run no-idempotency-db-latency LEDGER_MODE=no-idempotency SCENARIO=db-latency FAULT_INTERVAL=12s FAULT_DURATION=8s
run dual-write-kafka-down LEDGER_MODE=dual-write SCENARIO=kafka-down FAULT_INTERVAL=8s
run unordered-locks-contention LEDGER_MODE=unordered-locks SCENARIO=contention
run read-modify-write-contention LEDGER_MODE=read-modify-write SCENARIO=contention

exit $failed
