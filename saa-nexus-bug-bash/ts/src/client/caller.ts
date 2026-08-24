// Generic PayFast caller. Deliberately independent of this repo's own
// handler.ts/activities.ts/workflows.ts — it only knows the IDL-generated
// service contract (../../generated/services, ../../generated/models) and
// the cardToken vocabulary documented in idl/CARD_TOKENS.md (part of the
// service's own contract, not an implementation detail), the same way a
// caller talking to someone else's hosted implementation of this Nexus
// service would. No caller workflow, no worker of its own: calls the
// endpoint directly via the client's standalone Nexus operation API
// (client.nexus.createServiceClient) — Temporal Cloud dispatches to
// whichever build is Current in the payfast-bug-bash Worker Deployment.
//
// Connection comes from TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE/TEMPORAL_API_KEY
// (same names as deploy/.env.local) via @temporalio/envconfig, same as
// worker.ts. Load them into your shell first:
//   set -a; source ../deploy/.env.local; set +a
//
// Usage:
//   npm run caller                                   # runs every preset below
//   npm run caller -- <preset>                        # runs just one
//   npm run caller -- --list                          # lists presets, no calls
//   npm run caller -- --raw <operation> <jsonInput> [scheduleToCloseTimeout]
import { randomUUID } from 'node:crypto';
import type { Duration } from '@temporalio/common';
import { loadClientConnectConfig } from '@temporalio/envconfig';
import { Client, Connection } from '@temporalio/client';
import { payFast } from '../../../generated/services';
import { NEXUS_ENDPOINT } from '../config';

type PayFastClient = ReturnType<Client['nexus']['createServiceClient']>;

async function call(
  nexusClient: PayFastClient,
  operation: string,
  input: unknown,
  scheduleToCloseTimeout: Duration = '30 seconds'
): Promise<any> {
  return await nexusClient.executeOperation(operation as never, input as never, {
    id: `caller-${operation}-${randomUUID()}`,
    scheduleToCloseTimeout,
  });
}

function printResult(label: string, result: unknown) {
  console.log(`  ${label}:`);
  console.log(
    JSON.stringify(result, null, 2)
      .split('\n')
      .map((line) => `    ${line}`)
      .join('\n')
  );
}

type Preset = (nexusClient: PayFastClient) => Promise<void>;

// One preset per row of idl/CARD_TOKENS.md, plus the six bug-bash scenarios
// from the top-level README's scenario map, adapted to a direct (no caller
// workflow) call. Every preset mints fresh random IDs so re-running (or
// running "all") never collides with a previous run.
const presets: Record<string, Preset> = {
  'charge-ok': async (c) => {
    printResult('chargeOrder tok_ok', await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_ok' }));
  },
  'charge-decline': async (c) => {
    printResult(
      'chargeOrder tok_decline',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_decline' })
    );
  },
  'charge-pending-review': async (c) => {
    printResult(
      'chargeOrder tok_pending_review',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_pending_review' })
    );
  },
  'charge-retry-then-ok': async (c) => {
    printResult(
      'chargeOrder tok_retry_3_then_ok',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_retry_3_then_ok' }, '60 seconds')
    );
  },
  'charge-retry-forever': async (c) => {
    printResult(
      'chargeOrder tok_retry_forever (expect a failure once retries/timeout are exhausted)',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_retry_forever' }, '15 seconds')
    );
  },
  'charge-nonretryable': async (c) => {
    printResult(
      'chargeOrder tok_fail_nonretryable (expect a failure)',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_fail_nonretryable' })
    );
  },
  'charge-timeout': async (c) => {
    printResult(
      'chargeOrder tok_timeout_20s w/ 5s scheduleToCloseTimeout (expect a timeout)',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_timeout_20s' }, '5 seconds')
    );
  },
  'charge-unknown-token': async (c) => {
    printResult(
      'chargeOrder unrecognized card token (expect status=declined, errorCode=unknown_card_token)',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_totally_made_up' })
    );
  },
  'charge-hang': async (c) => {
    printResult(
      'chargeOrder tok_hang w/ 5s scheduleToCloseTimeout (expect a timeout — see runLongTask for a controllable hang)',
      await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_hang' }, '5 seconds')
    );
  },

  // Scenario 3: failure/timeout semantics, SAA-backed vs workflow-backed.
  'charge-via-workflow-diff': async (c) => {
    const orderId = `ord-${randomUUID()}`;
    const input = { orderId, amountCents: 500, cardToken: 'tok_fail_nonretryable' };
    const [saa, wf] = await Promise.allSettled([call(c, 'chargeOrder', input), call(c, 'chargeOrderViaWorkflow', input)]);
    printResult('chargeOrder (SAA)', saa.status === 'fulfilled' ? saa.value : { error: saa.reason?.message });
    printResult('chargeOrderViaWorkflow', wf.status === 'fulfilled' ? wf.value : { error: wf.reason?.message });
  },

  // Scenario 1: idempotency under retry — two concurrent calls, one orderId.
  'idempotent-retry': async (c) => {
    const orderId = `ord-${randomUUID()}`;
    const input = { orderId, amountCents: 1000, cardToken: 'tok_retry_3_then_ok' };
    const [a, b] = await Promise.allSettled([
      call(c, 'chargeOrder', input, '60 seconds'),
      call(c, 'chargeOrder', input, '60 seconds'),
    ]);
    const aResult = a.status === 'fulfilled' ? a.value : { error: a.reason?.message };
    const bResult = b.status === 'fulfilled' ? b.value : { error: b.reason?.message };
    printResult('call A (shared orderId)', aResult);
    printResult('call B (shared orderId)', bResult);
    console.log(
      aResult?.chargeId && aResult.chargeId === bResult?.chargeId
        ? '  => Same chargeId: the two calls converged on one Activity Execution.'
        : '  => Different chargeId (or one failed): the two calls did NOT dedupe.'
    );
  },

  // Scenario 5: circuit breaker — repeated always-fails-retryably calls.
  'circuit-breaker': async (c) => {
    for (let i = 0; i < 8; i++) {
      const startedAt = Date.now();
      try {
        const result = await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_retry_forever' }, '10 seconds');
        console.log(`  call ${i + 1}/8: ${Date.now() - startedAt}ms, ok, result=${JSON.stringify(result)}`);
      } catch (err) {
        console.log(`  call ${i + 1}/8: ${Date.now() - startedAt}ms, error=${(err as Error).message}`);
      }
    }
  },

  // Scenario 5, take 2b: one long-lived operation, so the backing activity's
  // own retry policy has room to accumulate 5+ attempts *within a single
  // operation* (rather than 5+ separate failed operations, as in the burst
  // preset below) before scheduleToCloseTimeout would naturally end it. If a
  // breaker cuts off further server-side retry/dispatch after N consecutive
  // retryable failures, this should fail well before 90s; if not, it just
  // rides out the full window like every other tok_retry_forever case.
  'circuit-breaker-long': async (c) => {
    const startedAt = Date.now();
    try {
      const result = await call(
        c,
        'chargeOrder',
        { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_retry_forever' },
        '90 seconds'
      );
      console.log(`  resolved after ${Date.now() - startedAt}ms: ${JSON.stringify(result)}`);
    } catch (err) {
      console.log(`  failed after ${Date.now() - startedAt}ms: ${(err as Error).message}`);
    }
  },

  // Scenario 5, take 3: every preset above injects chaos *inside the backing
  // activity* — that only exercises the activity's own retry policy, never
  // the Nexus dispatch layer, so it can't prove anything about a
  // caller->endpoint circuit breaker either way. tok_handler_retryable_error
  // fails synchronously in the *handler's* start() (see handler.ts) with a
  // nexus-rpc HandlerError of a type that's retryable by default — a real
  // Nexus dispatch-level retryable error. Each call should fail near-instantly
  // (no activity, no timeout wait), so 10 sequential calls run in well under
  // a second if nothing throttles them — any slowdown, error-message change,
  // or the final healthy probe failing would be direct evidence of a breaker.
  'circuit-breaker-handler-error': async (c) => {
    for (let i = 0; i < 10; i++) {
      const startedAt = Date.now();
      try {
        const result = await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_handler_retryable_error' }, '10 seconds');
        console.log(`  call ${i + 1}/10: ${Date.now() - startedAt}ms, unexpectedly ok, result=${JSON.stringify(result)}`);
      } catch (err) {
        console.log(`  call ${i + 1}/10: ${Date.now() - startedAt}ms, error=${(err as Error).message}`);
      }
    }
    const startedAt = Date.now();
    try {
      const result = await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_ok' }, '10 seconds');
      console.log(`  post-burst healthy probe: ${Date.now() - startedAt}ms, ok, chargeId=${result?.chargeId}`);
    } catch (err) {
      console.log(`  post-burst healthy probe: ${Date.now() - startedAt}ms, error=${(err as Error).message}`);
    }
  },

  // Scenario 5, take 2: the plain "circuit-breaker" preset above is 8
  // *sequential* calls, each just riding out its own 10s timeout — that
  // proves nothing about whether a breaker tripped. This one (a) times a
  // healthy baseline call, (b) fires a concurrent burst of always-fails
  // calls to concentrate failures in a tight window, then (c) immediately
  // times a fresh healthy call again. If a breaker is blocking *all*
  // traffic from this caller to this endpoint (not just retrying the bad
  // token), the post-burst probe should fail fast or be visibly slower
  // than the baseline instead of succeeding at baseline speed.
  'circuit-breaker-burst': async (c) => {
    const probe = async (label: string) => {
      const startedAt = Date.now();
      try {
        const result = await call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_ok' }, '10 seconds');
        console.log(`  ${label}: ${Date.now() - startedAt}ms, ok, chargeId=${result?.chargeId}`);
      } catch (err) {
        console.log(`  ${label}: ${Date.now() - startedAt}ms, error=${(err as Error).message}`);
      }
    };

    await probe('baseline probe (tok_ok, before burst)');

    const BURST_SIZE = 20;
    console.log(`  firing ${BURST_SIZE} concurrent tok_retry_forever calls...`);
    const burstStartedAt = Date.now();
    const burst = await Promise.allSettled(
      Array.from({ length: BURST_SIZE }, () =>
        call(c, 'chargeOrder', { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_retry_forever' }, '5 seconds')
      )
    );
    const failures = burst.filter((r) => r.status === 'rejected').length;
    console.log(
      `  burst done in ${Date.now() - burstStartedAt}ms: ${failures}/${BURST_SIZE} failed as expected, ${BURST_SIZE - failures} unexpectedly succeeded`
    );

    await probe('post-burst probe (tok_ok, immediately after)');
  },

  'subscription-derived': async (c) => {
    printResult(
      'startSubscription idStrategy=derived maxCycles=3 (idempotent cycle IDs)',
      await call(
        c,
        'startSubscription',
        { subId: `sub-${randomUUID()}`, cycleN: 0, amountCents: 500, cardToken: 'tok_ok', idStrategy: 'derived', maxCycles: 3 },
        '60 seconds'
      )
    );
  },
  'subscription-random': async (c) => {
    printResult(
      'startSubscription idStrategy=random maxCycles=3 (watch for duplicate cycles downstream)',
      await call(
        c,
        'startSubscription',
        { subId: `sub-${randomUUID()}`, cycleN: 0, amountCents: 500, cardToken: 'tok_ok', idStrategy: 'random', maxCycles: 3 },
        '60 seconds'
      )
    );
  },

  'refund-after-charge': async (c) => {
    const orderId = `ord-${randomUUID()}`;
    const charge = await call(c, 'chargeOrder', { orderId, amountCents: 1000, cardToken: 'tok_ok' });
    printResult('chargeOrder', charge);
    printResult('refundCharge', await call(c, 'refundCharge', { chargeId: charge.chargeId }));
  },

  // Scenario 6: token/link handling — rehydrate a handle from a bare chargeId.
  'get-charge-after-charge': async (c) => {
    const orderId = `ord-${randomUUID()}`;
    const charge = await call(c, 'chargeOrder', { orderId, amountCents: 1000, cardToken: 'tok_ok' });
    printResult('chargeOrder', charge);
    printResult('getCharge (rehydrated from nothing but chargeId)', await call(c, 'getCharge', { chargeId: charge.chargeId }));
  },

  'long-task-short': async (c) => {
    printResult(
      'runLongTask (5s, cooperative default)',
      await call(c, 'runLongTask', { taskId: `task-${randomUUID()}`, durationSeconds: 5, heartbeatIntervalSeconds: 1 }, '30 seconds')
    );
  },

  // --- chargeOrder-only scenario battery -----------------------------------
  // Everything below targets chargeOrder exclusively. Some endpoints (e.g. a
  // third party hosting only the operation they need) may not implement
  // startSubscription/refundCharge/getCharge/runLongTask/chargeOrderViaWorkflow
  // at all, so this battery re-covers the 6 bug-bash scenarios using nothing
  // but chargeOrder, so it stays runnable against any opaque endpoint that
  // implements just this one operation. Scenarios 1, 3, and 5 are already
  // chargeOrder-only above (idempotent-retry; charge-nonretryable/
  // charge-timeout/charge-retry-forever/charge-hang; circuit-breaker*) — not
  // duplicated here. Scenario 4 (worker restart mid-activity) is an
  // operational drill on whoever hosts the handler, not something a caller
  // can exercise from the outside, so it has no entry here either. That
  // leaves scenario 2 (uncooperative cancellation) and scenario 6 (token/link
  // handling), covered below via startOperation + handle.cancel()/getHandle,
  // instead of runLongTask/getCharge.

  // Scenario 2: cooperative cancellation. tok_hang loops until cancelled;
  // cancelBehavior: 'cooperative' (the default) means the activity should
  // notice the cancel request and exit promptly as CANCELLED.
  'portable-cancel-cooperative': async (c) => {
    const handle = await c.startOperation(
      'chargeOrder',
      { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_hang', cancelBehavior: 'cooperative' },
      { id: `caller-chargeOrder-${randomUUID()}`, scheduleToCloseTimeout: '30 seconds' }
    );
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const startedAt = Date.now();
    await handle.cancel('portable-cancel-cooperative preset');
    try {
      const result = await handle.result();
      console.log(`  unexpectedly completed ${Date.now() - startedAt}ms after cancel: ${JSON.stringify(result)}`);
    } catch (err) {
      console.log(`  cancelled after ${Date.now() - startedAt}ms as expected: ${(err as Error).message}`);
    }
  },

  // Scenario 2: uncooperative cancellation. Same tok_hang, but
  // cancelBehavior: 'ignore_cancel_entirely' means the activity never checks
  // for cancellation and runs to completion (or the scheduleToCloseTimeout)
  // regardless. This is the scenario's actual point: cancel() succeeds in
  // *requesting* cancellation, but the operation should NOT resolve as
  // cancelled promptly — it should instead ride out the timeout, unlike the
  // cooperative preset above.
  'portable-cancel-uncooperative': async (c) => {
    const handle = await c.startOperation(
      'chargeOrder',
      { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_hang', cancelBehavior: 'ignore_cancel_entirely' },
      { id: `caller-chargeOrder-${randomUUID()}`, scheduleToCloseTimeout: '10 seconds' }
    );
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const startedAt = Date.now();
    await handle.cancel('portable-cancel-uncooperative preset');
    try {
      const result = await handle.result();
      console.log(`  completed ${Date.now() - startedAt}ms after cancel (ignored it, as expected): ${JSON.stringify(result)}`);
    } catch (err) {
      console.log(`  failed ${Date.now() - startedAt}ms after cancel: ${(err as Error).message}`);
    }
  },

  // Scenario 6: token/link handling. Instead of a custom getCharge lookup
  // (not portable to endpoints that don't implement it), this rehydrates a
  // *fresh* handle from nothing but the caller-supplied operation ID — the
  // same id a real caller would persist as its own "link" back to the
  // operation — via the namespace-wide client.nexus.getHandle(), proving the
  // token/id round-trip works using only chargeOrder.
  'portable-token-rehydrate': async (c) => {
    const operationId = `caller-chargeOrder-${randomUUID()}`;
    const handle = await c.startOperation(
      'chargeOrder',
      { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_ok' },
      { id: operationId, scheduleToCloseTimeout: '30 seconds' }
    );
    console.log(`  started operationId=${handle.operationId}`);
    const rehydrated = handle.client.getHandle(handle.operationId);
    printResult('result via rehydrated handle (fresh reference, same operationId)', await rehydrated.result());
  },
};

async function runPreset(name: string, preset: Preset, nexusClient: PayFastClient) {
  console.log(`\n=== ${name} ===`);
  try {
    await preset(nexusClient);
  } catch (err) {
    console.log(`  error: ${(err as Error).message}`);
  }
}

function printUsage() {
  console.error('Usage:');
  console.error('  npm run caller                       # runs every preset');
  console.error('  npm run caller -- <preset>');
  console.error('  npm run caller -- --list');
  console.error('  npm run caller -- --raw <operation> <jsonInput> [scheduleToCloseTimeout]');
  console.error(`\nPresets:\n  ${Object.keys(presets).join('\n  ')}`);
  console.error(`\nRaw operations: ${Object.keys(payFast.operations).join(', ')}`);
}

async function main() {
  const [arg1, arg2, arg3, arg4] = process.argv.slice(2);

  if (arg1 === '--list' || arg1 === '--help') {
    printUsage();
    return;
  }

  const { connectionOptions, namespace } = loadClientConnectConfig();
  const connection = await Connection.connect(connectionOptions);
  const client = new Client({ connection, namespace });
  const nexusClient = client.nexus.createServiceClient({ endpoint: NEXUS_ENDPOINT, service: payFast });

  try {
    if (arg1 === '--raw') {
      const [operation, jsonInput, scheduleToCloseTimeout] = [arg2, arg3, arg4];
      if (!operation || !(operation in payFast.operations)) {
        if (operation) console.error(`Unknown operation "${operation}".\n`);
        printUsage();
        process.exit(1);
      }
      const result = await call(
        nexusClient,
        operation,
        jsonInput ? JSON.parse(jsonInput) : {},
        (scheduleToCloseTimeout ?? '30 seconds') as Duration
      );
      console.log(JSON.stringify(result, null, 2));
      return;
    }

    if (!arg1) {
      for (const [name, preset] of Object.entries(presets)) {
        await runPreset(name, preset, nexusClient);
      }
      return;
    }

    const preset = presets[arg1];
    if (!preset) {
      console.error(`Unknown preset "${arg1}".\n`);
      printUsage();
      process.exit(1);
    }
    await runPreset(arg1, preset, nexusClient);
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
