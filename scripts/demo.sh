#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_ID="${1:-lw-demo-$(date +%s)}"
TASK_QUEUE="${TEMPORAL_TASK_QUEUE:-lightweight-workflows}"
SPEC_FILE="${SPEC_FILE:-examples/customer_adjustment.json}"

printf '\n1) Start the process\n'
temporal workflow start \
  --workflow-id "$WORKFLOW_ID" \
  --type LightweightProcess \
  --task-queue "$TASK_QUEUE" \
  --input-file "$SPEC_FILE"

printf '\n2) Query live state (risk score should route to human approval)\n'
sleep 3
temporal workflow query --workflow-id "$WORKFLOW_ID" --name status

printf '\n3) Approve the human task\n'
temporal workflow signal \
  --workflow-id "$WORKFLOW_ID" \
  --name approve \
  --input '{"approved":true,"approver":"ops.manager","comment":"approved in CLI demo"}'

printf '\n4) Wait for final result\n'
temporal workflow result --workflow-id "$WORKFLOW_ID"

printf '\n5) Show durable event history / audit trail\n'
temporal workflow show --workflow-id "$WORKFLOW_ID" --detailed
