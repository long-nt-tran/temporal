#!/usr/bin/env bash
# STEP 1 of the 2-step deploy: build the worker-lambda image, push it to
# ECR, and publish a new Lambda version. Requires the Lambda function to
# already exist — see setup_infra.sh for one-time creation.
#
# Env vars (see .env.local):
#   AWS_REGION            required
#   LAMBDA_FUNCTION_NAME  required. Also used as the ECR repository name.
#
# Flags:
#   --skip-function-update   build+push only; don't touch the Lambda
#                            function. Used by setup_infra.sh before the
#                            function exists.
#
# Writes BUILD_ID / QUALIFIED_ARN (or IMAGE_URI, with --skip-function-update)
# to .last-deploy (gitignored) for register_version.sh to pick up.
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

: "${AWS_REGION:?must be set}"
: "${LAMBDA_FUNCTION_NAME:?must be set}"

SKIP_FUNCTION_UPDATE=false
[ "${1:-}" = "--skip-function-update" ] && SKIP_FUNCTION_UPDATE=true

REPO_ROOT="$(cd .. && pwd)"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_HOST="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
ECR_URI="$ECR_HOST/$LAMBDA_FUNCTION_NAME"

aws ecr describe-repositories --repository-names "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" >/dev/null

echo "==> building image (this compiles the pre-release Temporal SDK from source — see the Dockerfile's TODO(saa-nexus-ga) comment)"
# --provenance=false --sbom=false: modern BuildKit attaches a provenance/SBOM
# attestation manifest by default, producing an OCI image *index* (manifest
# list) instead of a plain single-platform image manifest. AWS Lambda's
# CreateFunction/UpdateFunctionCode rejects that with "image manifest ... is
# not supported" — it wants a plain manifest.
docker build --platform linux/amd64 --provenance=false --sbom=false \
  -f docker/worker-lambda.Dockerfile -t "$LAMBDA_FUNCTION_NAME:latest" "$REPO_ROOT"

echo "==> pushing to $ECR_URI"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_HOST"
docker tag "$LAMBDA_FUNCTION_NAME:latest" "$ECR_URI:latest"
PUSH_OUTPUT=$(docker push "$ECR_URI:latest")
echo "$PUSH_OUTPUT"
DIGEST=$(echo "$PUSH_OUTPUT" | grep -o 'sha256:[a-f0-9]*' | tail -1)
BUILD_ID="${DIGEST#sha256:}"
BUILD_ID="${BUILD_ID:0:12}"

if [ "$SKIP_FUNCTION_UPDATE" = true ]; then
  echo "==> --skip-function-update set: pushed $ECR_URI@$DIGEST, not touching the Lambda function"
  echo "IMAGE_URI=$ECR_URI@$DIGEST" > .last-deploy
  exit 0
fi

echo "==> updating Lambda function code"
aws lambda update-function-code --function-name "$LAMBDA_FUNCTION_NAME" --image-uri "$ECR_URI@$DIGEST" --region "$AWS_REGION" >/dev/null
aws lambda wait function-updated --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION"

# worker-lambda.ts reports its own build-id to Temporal Cloud as
# process.env.WORKER_BUILD_ID (falling back to "dev" if unset). That value
# must match whatever build-id register_version.sh registers as Current, or
# the running worker never claims to *be* the current version, the task
# queue never binds to it, and the Lambda is never dispatched to — it just
# self-registers a spurious "dev" version instead. update-function-configuration
# replaces the whole Variables map, so merge rather than overwrite.
echo "==> syncing WORKER_BUILD_ID env var to $BUILD_ID"
ENVIRONMENT_JSON=$(aws lambda get-function-configuration --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" \
    --query "Environment.Variables" --output json \
  | jq --arg bid "$BUILD_ID" '{Variables: (. + {WORKER_BUILD_ID: $bid})}')
aws lambda update-function-configuration --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" \
  --environment "$ENVIRONMENT_JSON" >/dev/null
aws lambda wait function-updated --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION"

echo "==> publishing new version"
QUALIFIED_ARN=$(aws lambda publish-version --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" --query FunctionArn --output text)

{
  echo "BUILD_ID=$BUILD_ID"
  echo "QUALIFIED_ARN=$QUALIFIED_ARN"
} > .last-deploy

echo
echo "==> done. build-id=$BUILD_ID"
echo "    qualified arn=$QUALIFIED_ARN"
echo "    next: ./register_version.sh"
