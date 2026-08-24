# PayFast: a Nexus + Standalone Activity bug bash

PayFast is a small payments Nexus service purpose-built to poke at six
Nexus + Standalone Activity (SAA) scenarios. The IDL is hand-written Nexus
RPC/JSON-Schema, run through [nexgen](https://github.com/temporalio/nexgen)
to produce typed TypeScript models + service definition; the handler,
activities, workflows, and a client-side scenario harness are hand-written
on top.

Server-side terminology: this repo (and its tests) call the feature
**Standalone Activity (SAA)** — an Activity Execution with no parent
Workflow. "SAA-via-Nexus" means a Workflow schedules a Nexus operation
whose *handler* backs it with a Standalone Activity instead of a child
Workflow (see `tests/nexus_workflow_test.go`,
`TestNexusOperationStartsStandaloneActivityBidirectionalLinks`). That's the
path this bug bash exercises.

## Layout

```
idl/payfast.nexusrpc.yaml   Nexus RPC / JSON-Schema IDL (nexgen input)
idl/CARD_TOKENS.md          Chaos vocabulary for cardToken / cancelBehavior
generated/                  nexgen output: models.ts, services.ts, definitions.ts
dynamicconfig/               Dynamic config overrides needed on the server
ts/src/
  activities.ts              The four activities: runCharge, runRefund,
                              runSubscriptionCycle, runLongTaskActivity
  chaos.ts                   Interprets cardToken into pass/fail/hang/etc.
  workflows.ts                chargeOrderWorkflow (workflow-backed twin) +
                              runPayfastOperation (generic scenario driver)
  handler.ts                  The Nexus service handler wiring
  worker.ts                   Local-dev worker entrypoint (long-polling)
  worker-lambda.ts             AWS Lambda worker entrypoint (serverless, see deploy/)
  client/scenarios.ts          CLI scenario runner (drives a caller workflow)
  client/caller.ts              Generic standalone caller — no caller workflow,
                                no dependency on this repo's own handler/worker
                                code; only the IDL-generated service contract
scripts/
  link-sdk.sh                 Clones + builds sdk-typescript pre-release from source
  register-endpoint.sh         Registers the Nexus endpoint via the temporal CLI
deploy/                       AWS Lambda deploy (Temporal Cloud, serverless) — see deploy/README.md
```

## Scenario -> operation map

| # | Scenario | Drive it with |
|---|---|---|
| 1 | Idempotency under retry | `chargeOrder` with a shared `orderId` fired concurrently (watch `idConflictPolicy` behavior in `handler.ts`); `startSubscription` with `idStrategy: random` to see duplicate cycles. `npm run scenario -- idempotency` |
| 2 | Uncooperative cancellation | `runLongTask` with `cancelBehavior: cooperative` vs `ignore_cancel_entirely`, cancelled via the driver workflow's `requestCancel` signal. `npm run scenario -- cancellation` |
| 3 | Failure and timeout semantics | Same `cardToken` (`tok_fail_nonretryable`, `tok_timeout_20s`) sent to both `chargeOrder` (SAA) and `chargeOrderViaWorkflow`, diffed side by side. `npm run scenario -- failure-semantics` |
| 4 | Worker restart mid-Activity | `runLongTask` for 10 minutes with heartbeats; restart the worker process mid-flight. `npm run scenario -- worker-restart`, then `npm run scenario -- status <workflowId>` |
| 5 | Circuit breaker | Eight concurrent `chargeOrder` calls with `tok_retry_forever`. `npm run scenario -- circuit-breaker` |
| 6 | Token and link handling | Capture the Nexus operation token mid-flight via a workflow query, then rehydrate the same charge from nothing but its business `chargeId` via `getCharge`. `npm run scenario -- token-and-links` |

See `idl/CARD_TOKENS.md` for the full `cardToken` vocabulary.

## Setup

### 1. Server: dynamic config + build + run

SAA-backed Nexus operations need two flags off by default; see
`dynamicconfig/saa-nexus-bugbash.yaml`. Point your server config's
`dynamicConfigClient.filepath` at that file (absolute path), e.g. in
`config/development-sqlite.yaml`, then:

```sh
make bins
./temporal-server --config-file config/development-sqlite.yaml --allow-no-auth start
```

### 2. Register the Nexus endpoint

Requires the `temporal` CLI:

```sh
./saa-nexus-bug-bash/scripts/register-endpoint.sh
```

### 3. Get the pre-release TypeScript SDK

`@temporalio/nexus`'s `TemporalNexusClient.startActivity` /
`typedActivity` (the whole point of "SAA-backed Nexus operation") is not
in any published npm release as of this writing — it exists only on
`sdk-typescript`'s `main` branch. `npm install` alone will not give you a
working SDK for `chargeOrder`, `startSubscription`, or `runLongTask`.

```sh
./saa-nexus-bug-bash/scripts/link-sdk.sh
```

This clones `sdk-typescript` (a pnpm monorepo) next to this checkout
(`saa-nexus-bug-bash/sdk-typescript`, gitignored) at a pinned, verified-working
commit, and builds the packages we need — including compiling
`core-bridge`'s Rust addon from source. `ts/package.json`'s `@temporalio/*`
dependencies already point at `file:../sdk-typescript/packages/*`, so a
plain `cd ts && npm install` picks up the build directly — no `npm link`
step needed. Bump `SDK_COMMIT` in the script if you want a newer snapshot —
re-check `ts/src/handler.ts` and `ts/src/workflows.ts` against
`packages/nexus/src/workflow-helpers.ts` if its API shape has moved since.

### 4. Regenerate TypeScript from the IDL (only needed if you edit the IDL)

```sh
nexgen ts idl/payfast.nexusrpc.yaml --output generated
```

(`nexgen` releases: https://github.com/temporalio/nexgen/releases)

### 5. Build and run

```sh
cd ts
npm run build
npm run worker            # terminal 1
npm run scenario -- idempotency   # terminal 2, or any other scenario name above
```

`worker.ts` also works against Temporal Cloud instead of a self-hosted
server — it uses `@temporalio/envconfig`, which reads `TEMPORAL_ADDRESS`/
`TEMPORAL_NAMESPACE`/`TEMPORAL_API_KEY` (same names as `deploy/.env.local`)
and turns TLS on automatically once an API key is present. Override the
task queue with `PAYFAST_TASK_QUEUE` to run a second, differently-queued
instance of this same worker — useful for testing a local code change
against Temporal Cloud without touching the Lambda deployment:

```sh
set -a; source ../deploy/.env.local; set +a
PAYFAST_TASK_QUEUE=payfast-bug-bash-local-worker npm run worker
```

Nothing routes Nexus tasks to that queue until you register an endpoint
for it too (endpoint names are 1:1 with a target task queue):

```sh
PAYFAST_NEXUS_ENDPOINT=payfast-bug-bash-local PAYFAST_TASK_QUEUE=payfast-bug-bash-local-worker ../scripts/register-endpoint.sh
```

### 6. Call it from a plain script (no caller workflow, no worker of your own)

`client/caller.ts` calls any PayFast operation directly against whatever's
currently hosting it — the local worker, or the Lambda deployment via
Temporal Cloud — using only the IDL-generated `payFast` service object
(`generated/services.ts`). This is what a caller talking to someone else's
hosted implementation of this same Nexus service would use: it doesn't
import anything from `handler.ts`/`activities.ts`/`workflows.ts`. Uses
`client.nexus.createServiceClient(...)` (Temporal's "standalone Nexus
operation" client API) rather than a caller workflow.

Connection settings come from `TEMPORAL_ADDRESS`/`TEMPORAL_NAMESPACE`/
`TEMPORAL_API_KEY` — same names as `deploy/.env.local`, loaded the same way
as `worker.ts`:

```sh
cd ts
set -a; source ../deploy/.env.local; set +a
npm run caller -- chargeOrder '{"orderId":"ord-1","amountCents":500,"cardToken":"tok_ok"}'
```

Run with no arguments to list available operations. Point `PAYFAST_NEXUS_ENDPOINT`
at a different registered endpoint (e.g. the `payfast-bug-bash-local`
endpoint from the local-worker walkthrough above) to call a different
hosted instance of the service.

## Deploy to Temporal Cloud (serverless)

`deploy/` packages `worker-lambda.ts` as an AWS Lambda container image and
runs it as a Temporal Cloud Worker Deployment — one worker, serving Nexus +
activities + the workflow-backed twin. One-time infra setup, then a 2-step
deploy loop (push image, register version). See `deploy/README.md`.

## What's verified vs. not

`scripts/link-sdk.sh`'s pinned commit (`4d5dfc5bda6e64b60242615a68aa1e3725752a9a`)
has actually been built end-to-end in this environment: `pnpm install` +
proto codegen + `tsc --build` for `common`/`proto`/`activity`/`workflow`/`client`/`nexus`/`worker`/`lambda-worker`,
plus `core-bridge`'s Rust addon compiled from source and loaded at runtime
(`require('@temporalio/worker')` works, `TemporalNexusClient.startActivity`
is present in the compiled output). `ts/`'s own code (`tsc --build`) compiles
clean against these real pre-release types — this caught and fixed two real
bugs (a `node_modules` resolution gap for `generated/`, and `Duration`-typed
timeout fields that don't accept plain `string`). The Lambda entrypoint
(`worker-lambda.ts`) loads cleanly and the workflow bundle
(`ts/scripts/bundle-workflows.js`) builds successfully.

`deploy/docker/worker-lambda.Dockerfile` has also actually been built
(`docker build --platform linux/amd64`, ~272MB final image) and run: started
locally under the Lambda Runtime Interface Emulator and invoked over HTTP.
The handler loaded, the native Rust addon initialized, and it attempted a
real gRPC connection before failing on `ConnectionRefused` (no server was
listening at the test address) — proof the whole packaging chain works
(including the `/var/sdk-typescript` relative-symlink trick the final image
relies on — visible directly in that run's stack trace), not just that it
compiles. Along the way this also caught and fixed three real Dockerfile
bugs: a missing `git submodule update` (core-bridge's proto files live in
a submodule), `pnpm --filter "...pkg"` incidentally sweeping in and failing
on unrelated workspace packages (fixed by using `tsc --build`'s own project
references instead), and `npm install --install-links` choking on the
SDK's internal `workspace:*` protocol strings (fixed by using plain
symlinked installs and working out exactly where they resolve to in the
final image).

`worker.ts` and `client/caller.ts` (both wired through `@temporalio/envconfig`
for Temporal Cloud auth) have been compiled against the real pre-release
types and smoke-tested against a bogus address — a clean connection
error, not a crash, from both the native (`worker.ts`) and pure-JS
(`caller.ts`) connection paths.

The `deploy/` scripts have been run for real against a live AWS account
and Temporal Cloud namespace (not by me — by the user, who reported the
failures back). That surfaced and fixed two more real bugs beyond what
local testing caught: `docker build`'s default provenance/SBOM attestation
manifest isn't a format `aws lambda create-function`/`update-function-code`
accepts (fixed with `--provenance=false --sbom=false`), and the
CloudFormation-created invocation role's IAM policy only covered the bare
Lambda function ARN, not the version-qualified ARN (`:1`, `:2`, ...) that
`register_version.sh` actually registers with Temporal Cloud and that
Temporal Cloud calls `GetFunction` against (fixed with a trailing wildcard
on the granted resource).

Not yet confirmed end-to-end: a scenario or the caller actually completing
a full round trip against the deployed Lambda worker (i.e. `register_version.sh`
succeeding and a real `chargeOrder` call resolving). If `@temporalio/nexus`'s
API has moved since the pinned commit, expect to adjust `handler.ts` /
`workflows.ts` accordingly.
