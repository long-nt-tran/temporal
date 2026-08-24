import { activityInfo, heartbeat, log, sleep } from '@temporalio/activity';
import { ApplicationFailure } from '@temporalio/common';
import type {
  ChargeHandle,
  ChargeRequest,
  ChargeResult,
  LongTaskRequest,
  LongTaskResult,
  RefundResult,
  SubscriptionRequest,
} from '../../generated/models';
import { getClient } from './client-singleton';
import { interpretCardToken } from './chaos';
import { TASK_QUEUE } from './config';

type CancelBehavior = NonNullable<ChargeRequest['cancelBehavior']>;

/**
 * Runs for up to `totalSeconds`, heartbeating every `heartbeatIntervalSeconds`, obeying
 * `cancelBehavior`. `totalSeconds === undefined` means "run until cancelled" (tok_hang /
 * runLongTask's hang drill).
 */
async function runCancellable(
  totalSeconds: number | undefined,
  heartbeatIntervalSeconds: number,
  cancelBehavior: CancelBehavior
): Promise<'completed' | 'cancelled'> {
  const tickMs = Math.max(1, heartbeatIntervalSeconds) * 1000;
  const deadline = totalSeconds === undefined ? undefined : Date.now() + totalSeconds * 1000;

  for (;;) {
    if (deadline !== undefined && Date.now() >= deadline) {
      return 'completed';
    }
    switch (cancelBehavior) {
      case 'cooperative':
        try {
          await sleep(tickMs);
        } catch (err) {
          log.info('cooperative activity observed cancellation, exiting', { error: (err as Error).message });
          return 'cancelled';
        }
        heartbeat();
        break;
      case 'heartbeat_ignore_cancel':
        await new Promise((resolve) => setTimeout(resolve, tickMs));
        heartbeat();
        break;
      case 'ignore_cancel_entirely':
        await new Promise((resolve) => setTimeout(resolve, tickMs));
        break;
    }
  }
}

/**
 * Backs both `chargeOrder` (started as a Standalone Activity) and `chargeOrderViaWorkflow`
 * (started as a normal workflow-scheduled Activity via proxyActivities) — the exact same
 * function either way. `backing` is inferred from `workflowExecution`: the SDK only sets it
 * when a Workflow scheduled this activity.
 */
export async function runCharge(request: ChargeRequest): Promise<ChargeResult> {
  const info = activityInfo();
  const backing: ChargeResult['backing'] = info.workflowExecution ? 'workflow' : 'standalone_activity';
  const outcome = interpretCardToken(request.cardToken, info.attempt);
  const cancelBehavior = request.cancelBehavior ?? 'cooperative';

  switch (outcome.kind) {
    case 'succeed':
      return {
        chargeId: info.activityId,
        status: outcome.status,
        amountCents: request.amountCents,
        backing,
        attemptCount: info.attempt,
        ...(outcome.errorCode ? { errorCode: outcome.errorCode } : {}),
      };
    case 'fail':
      // Not `(cond ? A.retryable : A.nonRetryable)(...)`: that extracts the
      // static method as a bare reference, detaching it from ApplicationFailure.
      // Both methods do `return new this(...)`, so with `this` unbound the
      // call throws "this is not a constructor" instead of constructing a
      // failure — which then defaults to nonRetryable: false (retryable),
      // silently turning every intended non-retryable failure into a
      // retried one.
      throw outcome.retryable
        ? ApplicationFailure.retryable(outcome.message, outcome.errorCode)
        : ApplicationFailure.nonRetryable(outcome.message, outcome.errorCode);
    case 'hang':
    case 'sleep_then_succeed': {
      const seconds = outcome.kind === 'sleep_then_succeed' ? outcome.seconds : undefined;
      const result = await runCancellable(seconds, 1, cancelBehavior);
      if (result === 'cancelled') {
        throw ApplicationFailure.nonRetryable('charge cancelled', 'cancelled');
      }
      return {
        chargeId: info.activityId,
        status: 'succeeded',
        amountCents: request.amountCents,
        backing,
        attemptCount: info.attempt,
      };
    }
  }
}

/** Backs `refundCharge`. Always Standalone-Activity-backed. */
export async function runRefund(handle: ChargeHandle): Promise<RefundResult> {
  log.info('refunding charge', { chargeId: handle.chargeId });
  return { refundId: `refund-${handle.chargeId}`, status: 'succeeded' };
}

/**
 * One billing cycle of `startSubscription`. On success, if there are cycles left, starts the
 * next cycle itself as a new Standalone Activity — the "self-renewing chain" of SAAs. Uses a
 * plain Client (see client-singleton.ts) the same way any external process would; there is no
 * parent workflow to schedule the next cycle for us.
 */
export async function runSubscriptionCycle(request: SubscriptionRequest): Promise<ChargeResult> {
  const info = activityInfo();
  const outcome = interpretCardToken(request.cardToken, info.attempt);

  if (outcome.kind === 'fail') {
    throw outcome.retryable
      ? ApplicationFailure.retryable(outcome.message, outcome.errorCode)
      : ApplicationFailure.nonRetryable(outcome.message, outcome.errorCode);
  }

  const status = outcome.kind === 'succeed' ? outcome.status : 'succeeded';
  const result: ChargeResult = {
    chargeId: info.activityId,
    status,
    amountCents: request.amountCents,
    backing: 'standalone_activity',
    attemptCount: info.attempt,
  };

  if (status === 'succeeded' && request.cycleN + 1 < request.maxCycles) {
    const nextCycleN = request.cycleN + 1;
    const next: SubscriptionRequest = { ...request, cycleN: nextCycleN };
    const nextActivityId =
      request.idStrategy === 'derived'
        ? `sub-${request.subId}-cycle-${nextCycleN}`
        : `sub-${request.subId}-cycle-${randomId()}`;

    const client = await getClient();
    await client.activity.start('runSubscriptionCycle', {
      id: nextActivityId,
      taskQueue: TASK_QUEUE,
      args: [next],
    });
  }

  return result;
}

/** Backs `runLongTask`. Always Standalone-Activity-backed. Purpose-built for scenarios 2 and 4. */
export async function runLongTaskActivity(request: LongTaskRequest): Promise<LongTaskResult> {
  const start = Date.now();
  const status = await runCancellable(
    request.durationSeconds,
    request.heartbeatIntervalSeconds,
    request.cancelBehavior ?? 'cooperative'
  );
  const elapsedSeconds = Math.round((Date.now() - start) / 1000);
  return { taskId: request.taskId, status, elapsedSeconds };
}

function randomId(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}
