#!/usr/bin/env bash
# ONE-TIME setup: ECR repo, an initial image, a Secrets Manager secret, the
# Lambda execution role, the Lambda function itself, and the
# Temporal-Cloud-invocation IAM role. NOT part of the 2-step deploy loop
# (push_image.sh + register_version.sh) — run this once per Lambda
# function, before the first real deploy. Safe to re-run (skips/updates
# anything that already exists).
#
# The invocation role is created by deploying Temporal Cloud's own
# CloudFormation template (cloudformation/temporal-cloud-serverless-worker-role.yaml,
# pulled verbatim from
# https://docs.temporal.io/production-deployment/worker-deployments/serverless-workers/aws-lambda) —
# not hand-rolled, since Temporal Cloud's invocation identities are theirs
# to publish, not ours to guess.
#
# Env vars (see .env.local):
#   AWS_REGION, LAMBDA_FUNCTION_NAME                         required
#   TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY   required
#   TEMPORAL_CLOUD_EXTERNAL_ID                               optional —
#     any string, 5-45 chars. Auto-generated if unset (and printed, so you
#     can save it to .env.local — register_version.sh needs the same value).
#   NEXUS_TASK_QUEUE, WORKER_DEPLOYMENT_NAME                 optional,
#     default "payfast-bug-bash"
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
: "${TEMPORAL_ADDRESS:?must be set}"
: "${TEMPORAL_NAMESPACE:?must be set}"
: "${TEMPORAL_API_KEY:?must be set}"
NEXUS_TASK_QUEUE="${NEXUS_TASK_QUEUE:-payfast-bug-bash}"
WORKER_DEPLOYMENT_NAME="${WORKER_DEPLOYMENT_NAME:-payfast-bug-bash}"

EXEC_ROLE_NAME="$LAMBDA_FUNCTION_NAME-execution"
SECRET_NAME="$LAMBDA_FUNCTION_NAME-temporal-api-key"

echo "==> pushing an initial image (the Lambda function doesn't exist yet)"
./push_image.sh --skip-function-update
IMAGE_URI=$(grep '^IMAGE_URI=' .last-deploy | cut -d= -f2-)

echo "==> secrets manager: $SECRET_NAME"
if ! aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string "$TEMPORAL_API_KEY" --region "$AWS_REGION" >/dev/null
fi
SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" --query ARN --output text)

echo "==> execution role: $EXEC_ROLE_NAME"
if ! aws iam get-role --role-name "$EXEC_ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role --role-name "$EXEC_ROLE_NAME" --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }' >/dev/null
  aws iam attach-role-policy --role-name "$EXEC_ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
fi
aws iam put-role-policy --role-name "$EXEC_ROLE_NAME" --policy-name read-temporal-api-key --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{\"Effect\": \"Allow\", \"Action\": \"secretsmanager:GetSecretValue\", \"Resource\": \"$SECRET_ARN\"}]
}" >/dev/null
EXEC_ROLE_ARN=$(aws iam get-role --role-name "$EXEC_ROLE_NAME" --query Role.Arn --output text)

echo "==> waiting for IAM role propagation"
sleep 10

echo "==> lambda function: $LAMBDA_FUNCTION_NAME"
if ! aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  aws lambda create-function \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --package-type Image \
    --code "ImageUri=$IMAGE_URI" \
    --role "$EXEC_ROLE_ARN" \
    --timeout 300 \
    --memory-size 512 \
    --region "$AWS_REGION" \
    --environment "Variables={TEMPORAL_ADDRESS=$TEMPORAL_ADDRESS,TEMPORAL_NAMESPACE=$TEMPORAL_NAMESPACE,NEXUS_TASK_QUEUE=$NEXUS_TASK_QUEUE,WORKER_DEPLOYMENT_NAME=$WORKER_DEPLOYMENT_NAME,TEMPORAL_API_KEY_SECRET_ARN=$SECRET_ARN}" \
    >/dev/null
  aws lambda wait function-active --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION"
fi
FUNCTION_ARN=$(aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" --query Configuration.FunctionArn --output text)

GENERATED_EXTERNAL_ID=false
if [ -z "${TEMPORAL_CLOUD_EXTERNAL_ID:-}" ]; then
  TEMPORAL_CLOUD_EXTERNAL_ID=$(openssl rand -hex 16)
  GENERATED_EXTERNAL_ID=true
fi

INVOKE_STACK_NAME="$LAMBDA_FUNCTION_NAME-temporal-cloud-invoke"
INVOKE_ROLE_NAME="$LAMBDA_FUNCTION_NAME-temporal-cloud-invoke"
echo "==> temporal cloud invocation role (CloudFormation stack $INVOKE_STACK_NAME)"
# Trailing wildcard, not the bare FUNCTION_ARN: Temporal Cloud calls
# GetFunction/InvokeFunction against the *qualified*, version-specific ARN
# (e.g. ...:function:payfast-bug-bash:1, the one register_version.sh
# registers) — IAM treats that as a different resource string than the bare
# ARN, so a policy scoped to just the bare ARN 403s. "name*" matches both
# "name" (zero-length match) and "name:1"/"name:2"/etc.
aws cloudformation deploy \
  --stack-name "$INVOKE_STACK_NAME" \
  --template-file cloudformation/temporal-cloud-serverless-worker-role.yaml \
  --parameter-overrides \
    AssumeRoleExternalId="$TEMPORAL_CLOUD_EXTERNAL_ID" \
    LambdaFunctionARNs="${FUNCTION_ARN}*" \
    RoleName="$INVOKE_ROLE_NAME" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$AWS_REGION"
INVOKE_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name "$INVOKE_STACK_NAME" --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='RoleARN'].OutputValue" --output text)

echo
echo "==> done."
echo "    function arn:        $FUNCTION_ARN"
echo "    invocation role arn:  $INVOKE_ROLE_ARN"
echo "    external id:          $TEMPORAL_CLOUD_EXTERNAL_ID"
if [ "$GENERATED_EXTERNAL_ID" = true ]; then
  echo "    (external id was auto-generated — save it, it's not recoverable from AWS afterward)"
fi
echo
echo "Add these two lines to .env.local (register_version.sh needs them):"
echo "  TEMPORAL_CLOUD_WORKER_ROLE_ARN=$INVOKE_ROLE_ARN"
echo "  TEMPORAL_CLOUD_EXTERNAL_ID=$TEMPORAL_CLOUD_EXTERNAL_ID"
echo
echo "Then run: ./push_image.sh && ./register_version.sh   (publishes the first real version)"
