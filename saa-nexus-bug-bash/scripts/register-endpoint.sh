#!/usr/bin/env bash
# Registers the PayFast Nexus endpoint against a locally running temporal-server, pointing it
# at the worker's task queue. Requires the `temporal` CLI.
set -euo pipefail

NAME="${PAYFAST_NEXUS_ENDPOINT:-payfast-bug-bash}"
NAMESPACE="${PAYFAST_NAMESPACE:-nex-saa-long.a2dd6}"
TASK_QUEUE="${PAYFAST_TASK_QUEUE:-payfast-bug-bash}"

echo "Registering Nexus endpoint \"$NAME\" -> namespace \"$NAMESPACE\", task queue \"$TASK_QUEUE\""
temporal operator nexus endpoint create \
  --name "$NAME" \
  --target-namespace "$NAMESPACE" \
  --target-task-queue "$TASK_QUEUE"
