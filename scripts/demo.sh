#!/usr/bin/env bash
set -euo pipefail

BASE_WORKFLOW_ID="${1:-lw-demo-$(date +%s)}"
TASK_QUEUE="${TEMPORAL_TASK_QUEUE:-lightweight-workflows}"
SPEC_FILE="${SPEC_FILE:-examples/customer_adjustment.json}"
RUN_COUNT="${RUN_COUNT:-100}"
QUERY_DELAY_SECONDS="${QUERY_DELAY_SECONDS:-3}"
INTERVAL_START_SECONDS="${INTERVAL_START_SECONDS:-0.1}"
INTERVAL_STEP_SECONDS="${INTERVAL_STEP_SECONDS:-0.1}"

for run in $(seq 1 "$RUN_COUNT"); do
  WORKFLOW_ID="${BASE_WORKFLOW_ID}-$(printf '%03d' "$run")"

  printf '\n[%d/%d] Start the process (%s)\n' "$run" "$RUN_COUNT" "$WORKFLOW_ID"
  temporal workflow start \
    --workflow-id "$WORKFLOW_ID" \
    --type LightweightProcess \
    --task-queue "$TASK_QUEUE" \
    --input-file "$SPEC_FILE"

  printf '\n[%d/%d] Query live state\n' "$run" "$RUN_COUNT"
  sleep "$QUERY_DELAY_SECONDS"
  temporal workflow query --workflow-id "$WORKFLOW_ID" --name status

  printf '\n[%d/%d] Approve the human task\n' "$run" "$RUN_COUNT"
  temporal workflow signal \
    --workflow-id "$WORKFLOW_ID" \
    --name approve \
    --input '{"approved":true,"approver":"ops.manager","comment":"approved in CLI demo"}'

  printf '\n[%d/%d] Wait for final result\n' "$run" "$RUN_COUNT"
  temporal workflow result --workflow-id "$WORKFLOW_ID"

  printf '\n[%d/%d] Show durable event history / audit trail\n' "$run" "$RUN_COUNT"
  temporal workflow show --workflow-id "$WORKFLOW_ID" --detailed

  if [ "$run" -lt "$RUN_COUNT" ]; then
    interval=$(awk -v start="$INTERVAL_START_SECONDS" -v step="$INTERVAL_STEP_SECONDS" -v run="$run" \
      'BEGIN { printf "%.3f", start + (run - 1) * step }')
    printf '\nWaiting %s seconds before the next workflow\n' "$interval"
    sleep "$interval"
  fi
done
