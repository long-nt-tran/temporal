#!/usr/bin/env bash
# STEP 2 of the 2-step deploy: register the build pushed by push_image.sh as
# a Temporal Worker Deployment Version and set it Current. Run right after
# push_image.sh. Requires the `temporal` CLI on PATH.
#
# Reads BUILD_ID/QUALIFIED_ARN from .last-deploy (written by push_image.sh)
# unless overridden with --build-id/--function-arn.
#
# Env vars (see .env.local):
#   TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY   required
#   WORKER_DEPLOYMENT_NAME                                   required
#   TEMPORAL_CLOUD_WORKER_ROLE_ARN, TEMPORAL_CLOUD_EXTERNAL_ID
#     required. TEMPORAL_CLOUD_WORKER_ROLE_ARN is printed by setup_infra.sh
#     the first time — it must also be registered as an invocation role in
#     the Temporal Cloud UI (same external id) before this will work.
#
# Idempotent — re-running against an unchanged build-id is a no-op.
set -euo pipefail
cd "$(dirname "$0")"

load_env_file() {
  local file="$1" line key val
  while IFS= read -r line || [ -n "$line" ]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
    key="${BASH_REMATCH[1]}"
    val="${BASH_REMATCH[2]}"
    val="${val%\"}"; val="${val#\"}"
    val="${val%\'}"; val="${val#\'}"
    if [ -z "${!key:-}" ]; then
      export "$key=$val"
    fi
  done < "$file"
}
[ -f .env.local ] && load_env_file .env.local
[ -f .last-deploy ] && load_env_file .last-deploy

while [ $# -gt 0 ]; do
  case "$1" in
    --build-id) BUILD_ID="$2"; shift 2 ;;
    --function-arn) QUALIFIED_ARN="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

: "${TEMPORAL_ADDRESS:?must be set}"
: "${TEMPORAL_NAMESPACE:?must be set}"
: "${TEMPORAL_API_KEY:?must be set}"
: "${WORKER_DEPLOYMENT_NAME:?must be set}"
: "${TEMPORAL_CLOUD_WORKER_ROLE_ARN:?must be set — see setup_infra.sh and README.md}"
: "${TEMPORAL_CLOUD_EXTERNAL_ID:?must be set}"
: "${BUILD_ID:?must be set (run push_image.sh first, or pass --build-id)}"
: "${QUALIFIED_ARN:?must be set (run push_image.sh first, or pass --function-arn)}"

echo "==> creating deployment $WORKER_DEPLOYMENT_NAME (idempotent)"
if OUT=$(temporal worker deployment create \
    --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" \
    --name "$WORKER_DEPLOYMENT_NAME" 2>&1); then
  echo "$OUT"
elif echo "$OUT" | grep -qi "already exists"; then
  echo "==> $WORKER_DEPLOYMENT_NAME already exists, skipping create"
else
  echo "$OUT" >&2
  exit 1
fi

echo "==> registering build $BUILD_ID"
if OUT=$(temporal worker deployment create-version \
    --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" \
    --deployment-name "$WORKER_DEPLOYMENT_NAME" --build-id "$BUILD_ID" \
    --aws-lambda-function-arn "$QUALIFIED_ARN" \
    --aws-lambda-assume-role-arn "$TEMPORAL_CLOUD_WORKER_ROLE_ARN" \
    --aws-lambda-assume-role-external-id "$TEMPORAL_CLOUD_EXTERNAL_ID" 2>&1); then
  echo "$OUT"
elif echo "$OUT" | grep -qi "already exists"; then
  echo "==> $WORKER_DEPLOYMENT_NAME@$BUILD_ID already registered, skipping create"
else
  echo "$OUT" >&2
  exit 1
fi

echo "==> setting current version to $BUILD_ID"
temporal worker deployment set-current-version \
  --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" \
  --deployment-name "$WORKER_DEPLOYMENT_NAME" --build-id "$BUILD_ID" -y

echo
echo "==> done."
