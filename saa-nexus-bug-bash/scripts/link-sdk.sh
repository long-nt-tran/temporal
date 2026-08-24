#!/usr/bin/env bash
# Clones temporalio/sdk-typescript at the commit this bug bash was built
# against and builds it from source — Nexus + Standalone Activity support
# (TemporalNexusClient.startActivity) is unreleased on npm, so `npm install`
# alone in ts/ won't give you a working SDK.
#
# ts/package.json already points its @temporalio/* deps at
# file:../sdk-typescript/packages/* — no `npm link` needed. Just run this
# once, then `cd ts && npm install && npm run build`.
#
# Needs: pnpm, a Rust toolchain (rustc/cargo), and protoc on PATH — same
# prerequisites as building sdk-typescript upstream.
set -euo pipefail

# Commit verified to build clean and work end-to-end with this bug bash's
# code (see ../README.md's "What's verified" section). Bump if you want a
# newer snapshot — re-check ts/src/handler.ts and ts/src/workflows.ts
# against packages/nexus/src/workflow-helpers.ts if its API shape moved.
SDK_COMMIT="4d5dfc5bda6e64b60242615a68aa1e3725752a9a"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SDK_DIR="$ROOT/sdk-typescript"

if [ ! -d "$SDK_DIR" ]; then
  git clone https://github.com/temporalio/sdk-typescript.git "$SDK_DIR"
fi
git -C "$SDK_DIR" fetch origin "$SDK_COMMIT"
git -C "$SDK_DIR" checkout "$SDK_COMMIT"
# The sdk-core submodule's own recorded commit differs per SDK_COMMIT, so
# this has to run after checkout, not just at clone time.
git -C "$SDK_DIR" submodule update --init --recursive

cd "$SDK_DIR"
pnpm install --frozen-lockfile
pnpm run gen:protos
pnpm --filter "@temporalio/proto" run build:ts
# tsc --build, not `pnpm --filter ... run build`: worker's tsconfig.json
# project-references client/workflow/activity/nexus/common (nexus in turn
# references proto), so this alone transitively builds everything we need,
# without pnpm's filter sweeping in unrelated packages (meta, test,
# contrib/*) whose own build failures would otherwise abort this step.
(cd packages/worker && npx tsc --build)
(cd packages/lambda-worker && npx tsc --build)
(cd packages/core-bridge && pnpm run build-rust)

echo
echo "==> built sdk-typescript@$SDK_COMMIT at $SDK_DIR"
echo "    next: cd ts && npm install && npm run build"
