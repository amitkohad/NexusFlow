#!/usr/bin/env bash
set -euo pipefail

BASE_WORKFLOW_ID="${1:-lw-demo-$(date +%s)}"
TASK_QUEUE="${TEMPORAL_TASK_QUEUE:-lightweight-workflows}"
SPEC_FILE="${SPEC_FILE:-examples/customer_adjustment.json}"
RUN_COUNT="${RUN_COUNT:-10}"
QUERY_DELAY_SECONDS="${QUERY_DELAY_SECONDS:-3}"
INTERVAL_START_SECONDS="${INTERVAL_START_SECONDS:-0.1}"
INTERVAL_STEP_SECONDS="${INTERVAL_STEP_SECONDS:-0.1}"
PYTHON="${PYTHON:-python}"

SPEC_DIR=$(mktemp -d)
LOG_DIR=$(mktemp -d)
trap 'rm -rf "$SPEC_DIR" "$LOG_DIR"' EXIT

if [ "$RUN_COUNT" -gt 9001 ]; then
  printf 'RUN_COUNT cannot exceed 9001 when amounts must be unique in the range 1000-10000\n' >&2
  exit 1
fi

mapfile -t AMOUNTS < <("$PYTHON" - "$RUN_COUNT" <<'PY'
import random
import sys

for amount in random.SystemRandom().sample(range(1000, 10001), int(sys.argv[1])):
    print(amount)
PY
)

run_workflow() {
  local run="$1"
  local workflow_id="$2"
  local spec_path="$3"
  local log_path="$4"

  {
    printf '\n[%d/%d] Start the process (%s)\n' "$run" "$RUN_COUNT" "$workflow_id"
    temporal workflow start \
      --workflow-id "$workflow_id" \
      --type LightweightProcess \
      --task-queue "$TASK_QUEUE" \
      --input-file "$spec_path"

    printf '\n[%d/%d] Query live state\n' "$run" "$RUN_COUNT"
    sleep "$QUERY_DELAY_SECONDS"
    temporal workflow query --workflow-id "$workflow_id" --name status

    printf '\n[%d/%d] Approve the human task\n' "$run" "$RUN_COUNT"
    temporal workflow signal \
      --workflow-id "$workflow_id" \
      --name approve \
      --input '{"approved":true,"approver":"ops.manager","comment":"approved in CLI demo"}'

    printf '\n[%d/%d] Wait for final result\n' "$run" "$RUN_COUNT"
    temporal workflow result --workflow-id "$workflow_id"

    printf '\n[%d/%d] Show durable event history / audit trail\n' "$run" "$RUN_COUNT"
    temporal workflow show --workflow-id "$workflow_id" --detailed
  } >"$log_path" 2>&1
}

pids=()

for run in $(seq 1 "$RUN_COUNT"); do
  WORKFLOW_ID="${BASE_WORKFLOW_ID}-$(printf '%03d' "$run")"
  CUSTOMER_ID="C-DEMO-$(printf '%03d' "$run")"
  AMOUNT="${AMOUNTS[$((run - 1))]}"
  SPEC_PATH="$SPEC_DIR/$run.json"
  LOG_PATH="$LOG_DIR/$run.log"

  "$PYTHON" - "$SPEC_FILE" "$SPEC_PATH" "$CUSTOMER_ID" "$AMOUNT" <<'PY'
import json
import sys

source_path, target_path, customer_id, amount = sys.argv[1:]
with open(source_path, encoding="utf-8") as source:
    spec = json.load(source)
spec["request"]["customer_id"] = customer_id
spec["request"]["amount"] = int(amount)
with open(target_path, "w", encoding="utf-8") as target:
    json.dump(spec, target, indent=2)
    target.write("\n")
PY

  run_workflow "$run" "$WORKFLOW_ID" "$SPEC_PATH" "$LOG_PATH" &
  pids+=("$!")

  if [ "$run" -lt "$RUN_COUNT" ]; then
    interval=$(awk -v start="$INTERVAL_START_SECONDS" -v step="$INTERVAL_STEP_SECONDS" -v run="$run" \
      'BEGIN { printf "%.3f", start + (run - 1) * step }')
    printf '\nLaunched %s with customer %s and amount %s; waiting %s seconds before the next workflow\n' \
      "$WORKFLOW_ID" "$CUSTOMER_ID" "$AMOUNT" "$interval"
    sleep "$interval"
  fi
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

cat "$LOG_DIR"/*.log
