# deploy/

Runs PayFast as one AWS Lambda worker (container image), serverless, reachable
from Temporal Cloud — serving the Nexus service, its Standalone-Activity-backed
operations' activities, and the `chargeOrderViaWorkflow` workflow, all in a
single worker (`ts/src/worker-lambda.ts`).

Deliberately much smaller than a full CDK setup: no infra-as-code, just
plain `aws`/`docker`/`temporal` CLI calls. One-time setup, then a 2-step
deploy loop.

## One-time setup

```bash
# fill in .env.local first (it exists with empty values — see the table below)
./setup_infra.sh
```

Creates (idempotently — safe to re-run): the ECR repo, an initial image, a
Secrets Manager secret holding `TEMPORAL_API_KEY`, the Lambda execution
role, the Lambda function itself, and the Temporal-Cloud-invocation IAM
role — the last one by deploying
`cloudformation/temporal-cloud-serverless-worker-role.yaml`, Temporal
Cloud's own template (pulled verbatim from their
[AWS Lambda serverless workers docs](https://docs.temporal.io/production-deployment/worker-deployments/serverless-workers/aws-lambda) —
its trust policy references Temporal Cloud's own invocation identities, not
anything we invented). If `TEMPORAL_CLOUD_EXTERNAL_ID` isn't set in
`.env.local`, the script generates one and prints it.

At the end it prints the invocation role ARN and the external ID — paste
both into `.env.local` (`TEMPORAL_CLOUD_WORKER_ROLE_ARN` /
`TEMPORAL_CLOUD_EXTERNAL_ID`); `register_version.sh` needs them.

Also register the Nexus endpoint once, same as the local-dev setup (see the
top-level README) — this deploy only manages the Lambda side, not the Nexus
endpoint → task queue wiring:

```bash
../scripts/register-endpoint.sh
```

## The 2-step deploy

```bash
./push_image.sh        # 1. build the image, push to ECR, publish a Lambda version
./register_version.sh  # 2. register that build as a Temporal Worker Deployment Version, set it current
```

Both auto-load `.env.local`. `push_image.sh` writes the build-id + qualified
Lambda ARN to `.last-deploy` (gitignored); `register_version.sh` reads them
from there automatically. Re-running `register_version.sh` against an
unchanged build-id is a no-op, not a failure.

## Env vars

| Var | Used by | Notes |
|---|---|---|
| `AWS_REGION` | all | |
| `LAMBDA_FUNCTION_NAME` | all | also the ECR repository name |
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | all | |
| `TEMPORAL_API_KEY` | `setup_infra.sh`, `register_version.sh` | seeds Secrets Manager once; used directly (never stored) to call the `temporal` CLI |
| `TEMPORAL_CLOUD_EXTERNAL_ID` | `setup_infra.sh` (optional — auto-generated if unset), `register_version.sh` (required) | must be the same value in both |
| `TEMPORAL_CLOUD_WORKER_ROLE_ARN` | `register_version.sh` | printed by `setup_infra.sh` |
| `NEXUS_TASK_QUEUE` / `WORKER_DEPLOYMENT_NAME` | `setup_infra.sh` (bakes into the function), `register_version.sh` | default `payfast-bug-bash` |

## Testing the image without deploying anything

The base image bundles the [Lambda Runtime Interface Emulator](https://github.com/aws/aws-lambda-runtime-interface-emulator),
so you can sanity-check the image builds and the handler loads without any
AWS/Temporal Cloud credentials:

```bash
docker build --platform linux/amd64 -f docker/worker-lambda.Dockerfile -t payfast-bug-bash:test ..
docker run -d --name payfast-test -p 9000:8080 \
  -e WORKER_DEPLOYMENT_NAME=test -e TEMPORAL_ADDRESS=localhost:7233 -e TEMPORAL_NAMESPACE=default \
  payfast-bug-bash:test
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
docker rm -f payfast-test
```

A clean `TransportError` / `ConnectionRefused` (there's no server at
`localhost:7233`) means the image is structurally fine — the handler
loaded, the native addon initialized, and it got as far as actually trying
to connect. A `Cannot find module` or `MODULE_NOT_FOUND` error instead
means something's wrong with the packaging.

## Why the image build is slow

`@temporalio/nexus`'s `TemporalNexusClient.startActivity` — the whole point
of a Standalone-Activity-backed Nexus operation — isn't in any published
`@temporalio/*` release yet. `docker/worker-lambda.Dockerfile`'s builder
stage clones and builds the pre-release SDK from source for Linux x86_64,
Rust bridge included, same as `ts/`'s local `file:../sdk-typescript/...`
dependencies but for the container's platform. Once SAA-via-Nexus ships in
a real release, delete that stage (marked `TODO(saa-nexus-ga)` in the
Dockerfile) and switch to a plain `npm ci` against the registry.
