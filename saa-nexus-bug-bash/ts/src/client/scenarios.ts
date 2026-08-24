import { randomUUID } from 'node:crypto';
import { Client, Connection } from '@temporalio/client';
import { NAMESPACE, SERVER_ADDRESS, TASK_QUEUE } from '../config';
import { getOperationToken, requestCancel, runPayfastOperation, type DriverInput, type DriverResult } from '../workflows';

async function startDriver(client: Client, request: DriverInput, workflowId = `driver-${randomUUID()}`) {
  return await client.workflow.start(runPayfastOperation, {
    taskQueue: TASK_QUEUE,
    workflowId,
    args: [request],
  });
}

function printResult(label: string, result: DriverResult) {
  console.log(`\n--- ${label} ---`);
  console.log(JSON.stringify(result, null, 2));
}

// Scenario 1: idempotency under retry. Two concurrent chargeOrder calls sharing one orderId
// should converge on a single Activity Execution; watch idConflictPolicy behavior in handler.ts.
async function scenarioIdempotency(client: Client) {
  const orderId = `ord-${randomUUID()}`;
  const request = (): DriverInput => ({
    operation: 'chargeOrder',
    input: { orderId, amountCents: 1000, cardToken: 'tok_retry_3_then_ok' },
    scheduleToCloseTimeout: '30 seconds',
  });

  const [a, b] = await Promise.all([startDriver(client, request()), startDriver(client, request())]);
  const [resultA, resultB] = await Promise.all([a.result(), b.result()]);
  printResult('chargeOrder call A (shared orderId)', resultA);
  printResult('chargeOrder call B (shared orderId)', resultB);
  console.log(
    (resultA.output as any)?.chargeId === (resultB.output as any)?.chargeId
      ? '=> Same chargeId: the two calls converged on one Activity Execution.'
      : '=> Different chargeId: the two calls did NOT dedupe.'
  );

  console.log('\nNow with a fresh random orderId per call (the "mint fresh per attempt" bug case):');
  const distinct = await startDriver(client, {
    operation: 'chargeOrder',
    input: { orderId: `ord-${randomUUID()}`, amountCents: 1000, cardToken: 'tok_ok' },
    scheduleToCloseTimeout: '30 seconds',
  });
  printResult('chargeOrder call with fresh orderId', await distinct.result());
}

// Scenario 2: uncooperative cancellation. Same drill against a cooperative and an
// ignore_cancel_entirely runLongTask.
async function scenarioCancellation(client: Client) {
  for (const cancelBehavior of ['cooperative', 'ignore_cancel_entirely'] as const) {
    const handle = await startDriver(client, {
      operation: 'runLongTask',
      input: { taskId: `task-${randomUUID()}`, durationSeconds: 30, heartbeatIntervalSeconds: 2, cancelBehavior },
      scheduleToCloseTimeout: '60 seconds',
      cancellationType: 'WAIT_CANCELLATION_COMPLETED',
    });
    await new Promise((resolve) => setTimeout(resolve, 3000));
    await handle.signal(requestCancel);
    printResult(`runLongTask cancelBehavior=${cancelBehavior}`, await handle.result());
  }
}

// Scenario 3: failure/timeout semantics, SAA-backed vs workflow-backed, same chaos token.
async function scenarioFailureSemantics(client: Client) {
  for (const cardToken of ['tok_fail_nonretryable', 'tok_timeout_20s']) {
    const orderId = `ord-${randomUUID()}`;
    const [saa, wf] = await Promise.all([
      startDriver(client, {
        operation: 'chargeOrder',
        input: { orderId, amountCents: 500, cardToken },
        scheduleToCloseTimeout: '10 seconds',
      }),
      startDriver(client, {
        operation: 'chargeOrderViaWorkflow',
        input: { orderId, amountCents: 500, cardToken },
        scheduleToCloseTimeout: '10 seconds',
      }),
    ]);
    const [saaResult, wfResult] = await Promise.all([saa.result(), wf.result()]);
    printResult(`chargeOrder (SAA) cardToken=${cardToken}`, saaResult);
    printResult(`chargeOrderViaWorkflow cardToken=${cardToken}`, wfResult);
  }
}

// Scenario 4: worker restart mid-activity. This harness only starts the long task and prints
// the driver workflowId; restart the worker process yourself, then re-run with `status <id>`.
async function scenarioWorkerRestart(client: Client) {
  const workflowId = `driver-restart-${randomUUID()}`;
  await startDriver(
    client,
    {
      operation: 'runLongTask',
      input: { taskId: `task-${randomUUID()}`, durationSeconds: 600, heartbeatIntervalSeconds: 5 },
      scheduleToCloseTimeout: '15 minutes',
    },
    workflowId
  );
  console.log(`Started 10-minute heartbeating runLongTask as driver workflow "${workflowId}".`);
  console.log('Now restart the worker process (Ctrl+C, then `npm run worker` again) and confirm it resumes.');
  console.log(`Check on it later with:  npm run scenario -- status ${workflowId}`);
}

// Scenario 5: circuit breaker. Fire five-plus concurrent chargeOrder calls that always fail
// retryably against the same endpoint; later calls should start failing fast once it trips.
async function scenarioCircuitBreaker(client: Client) {
  const attempts = 8;
  for (let i = 0; i < attempts; i++) {
    const startedAt = Date.now();
    const handle = await startDriver(client, {
      operation: 'chargeOrder',
      input: { orderId: `ord-${randomUUID()}`, amountCents: 100, cardToken: 'tok_retry_forever' },
      scheduleToCloseTimeout: '10 seconds',
    });
    const result = await handle.result();
    console.log(`call ${i + 1}/${attempts}: ${Date.now() - startedAt}ms, ok=${result.ok}, error=${result.errorMessage ?? '-'}`);
  }
}

// Scenario 6: token and link handling. Capture the Nexus operation token mid-flight via query,
// and separately rehydrate the same charge from nothing but its business chargeId via getCharge.
async function scenarioTokenAndLinks(client: Client) {
  const orderId = `ord-${randomUUID()}`;
  const handle = await startDriver(client, {
    operation: 'chargeOrder',
    input: { orderId, amountCents: 250, cardToken: 'tok_timeout_5s' },
    scheduleToCloseTimeout: '30 seconds',
  });
  await new Promise((resolve) => setTimeout(resolve, 1500));
  const token = await handle.query(getOperationToken);
  console.log(`Nexus operation token captured mid-flight: ${token ?? '(not set yet — operation resolved synchronously or too fast to observe)'}`);
  const driverResult = await handle.result();
  printResult('chargeOrder result', driverResult);

  const chargeId = `act-${orderId}`;
  const rehydrated = await startDriver(client, {
    operation: 'getCharge',
    input: { chargeId },
    scheduleToCloseTimeout: '10 seconds',
  });
  printResult(`getCharge({ chargeId: "${chargeId}" }) — rehydrated with no stored token`, await rehydrated.result());
}

async function status(client: Client, workflowId: string) {
  printResult(`status of ${workflowId}`, await client.workflow.getHandle<typeof runPayfastOperation>(workflowId).result());
}

const scenarios: Record<string, (client: Client) => Promise<void>> = {
  idempotency: scenarioIdempotency,
  cancellation: scenarioCancellation,
  'failure-semantics': scenarioFailureSemantics,
  'worker-restart': scenarioWorkerRestart,
  'circuit-breaker': scenarioCircuitBreaker,
  'token-and-links': scenarioTokenAndLinks,
};

async function main() {
  const [name, ...rest] = process.argv.slice(2);
  const connection = await Connection.connect({ address: SERVER_ADDRESS });
  const client = new Client({ connection, namespace: NAMESPACE });
  try {
    if (name === 'status') {
      await status(client, rest[0]);
      return;
    }
    const scenario = scenarios[name];
    if (!scenario) {
      console.error(`Usage: npm run scenario -- <${[...Object.keys(scenarios), 'status <workflowId>'].join(' | ')}>`);
      process.exit(1);
    }
    await scenario(client);
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
