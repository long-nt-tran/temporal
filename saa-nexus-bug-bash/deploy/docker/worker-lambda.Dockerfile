# syntax=docker/dockerfile:1.6
#
# TODO(saa-nexus-ga): @temporalio/nexus's TemporalNexusClient.startActivity
# (the whole point of a Standalone-Activity-backed Nexus operation) isn't in
# any published @temporalio/* release yet, so this builder stage clones and
# builds the pre-release SDK from source (including core-bridge's Rust
# addon) instead of a normal `npm ci`. Once it ships in a real release,
# delete everything up to "This project's own code" below and replace with
# `RUN npm ci --omit=dev` against the registry.
FROM public.ecr.aws/lambda/nodejs:22 AS builder

ARG SDK_COMMIT=4d5dfc5bda6e64b60242615a68aa1e3725752a9a

RUN dnf install -y git gcc gcc-c++ make protobuf-compiler protobuf-devel && dnf clean all
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
RUN npm install -g pnpm

WORKDIR /build
# --recurse-submodules alone isn't enough once we then check out a specific
# commit (it only fetches the submodule ref recorded at the branch tip) — the
# proto files core-bridge needs live in the sdk-core submodule, and its
# recorded commit differs per SDK_COMMIT, so re-sync submodules after checkout.
RUN git clone https://github.com/temporalio/sdk-typescript.git sdk-typescript \
    && cd sdk-typescript && git checkout "${SDK_COMMIT}" \
    && git submodule update --init --recursive

WORKDIR /build/sdk-typescript
RUN pnpm install --frozen-lockfile
RUN pnpm run gen:protos
RUN pnpm --filter "@temporalio/proto" run build:ts
# tsc --build, not `pnpm --filter ... run build`: worker's tsconfig.json
# project-references client/workflow/activity/nexus/common (nexus in turn
# references proto), so this alone transitively builds everything we need.
# The pnpm filter equivalent also sweeps in unrelated workspace packages
# (meta, test, contrib/*) whose own build failures (e.g. packages/meta
# wanting the unbuilt @temporalio/cloud) abort the whole RUN step for no
# reason relevant to us.
RUN cd packages/worker && npx tsc --build
RUN cd packages/lambda-worker && npx tsc --build
RUN cd packages/core-bridge && pnpm run build-rust-release
# Rust build artifacts (can be several GB) aren't needed past this point —
# only the compiled .node addon under packages/core-bridge/releases/ is.
RUN find packages/core-bridge -maxdepth 2 -type d -name target -exec rm -rf {} + \
    && rm -rf .git packages/core-bridge/sdk-core/.git

# This project's own code. Copied after the SDK build above so code changes
# here don't invalidate that (slow) layer.
WORKDIR /build
COPY generated ./generated
COPY ts/package.json ./ts/package.json
WORKDIR /build/ts
# Plain install, not --install-links: the file:../sdk-typescript/packages/*
# deps' own package.json declare their cross-package deps as "workspace:*",
# which only pnpm understands — npm's --install-links tries to resolve them
# as real installs and chokes on the unsupported "workspace:" protocol.
# Symlinking (the default) just needs the target directory to exist, which
# it does. See the final stage below for how these (relative!) symlinks are
# kept resolvable after copying out of this stage.
RUN npm install

# generated/*.ts imports nexus-rpc and long directly. Node module resolution
# only looks in ancestor node_modules dirs, and generated/ is a sibling of
# ts/, not a descendant — same gap hit locally, fixed the same way there
# with a node_modules symlink at the common parent.
WORKDIR /build
RUN ln -s ts/node_modules node_modules
COPY ts ./ts
WORKDIR /build/ts
RUN npm run build
RUN npm run bundle-workflows

FROM public.ecr.aws/lambda/nodejs:22

WORKDIR ${LAMBDA_TASK_ROOT}

# node_modules/@temporalio/* are relative symlinks (e.g.
# ../../../sdk-typescript/packages/nexus) created by the plain `npm install`
# above. LAMBDA_TASK_ROOT is /var/task, so copying node_modules directly
# under it means those symlinks resolve to /var/sdk-typescript — bring the
# built SDK along at exactly that path so they still work here. (Verified
# with `path.resolve` against this exact layout before relying on it.)
COPY --from=builder /build/out ./out
COPY --from=builder /build/ts/node_modules ./node_modules
COPY --from=builder /build/sdk-typescript /var/sdk-typescript

CMD ["out/ts/src/worker-lambda.handler"]
