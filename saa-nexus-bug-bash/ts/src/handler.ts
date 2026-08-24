import { randomUUID } from 'node:crypto';
import * as nexus from 'nexus-rpc';
import * as temporalNexus from '@temporalio/nexus';
import { payFast } from '../../generated/services';
import type {
  ChargeHandle,
  ChargeRequest,
  ChargeResult,
  LongTaskRequest,
  LongTaskResult,
  RefundResult,
  SubscriptionRequest,
} from '../../generated/models';
import { chargeOrderWorkflow } from './workflows';

/**
 * PayFast's Nexus service handler. Wires each operation to one of two backings:
 *
 * - chargeOrder, startSubscription, refundCharge, runLongTask: Standalone Activity Execution
 *   (no parent workflow), via `TemporalNexusClient.startActivity`.
 * - chargeOrderViaWorkflow: a Workflow run, via `TemporalNexusClient.startWorkflow`. Runs the
 *   exact same activity code as chargeOrder — see workflows.ts.
 * - getCharge: a synchronous lookup that rehydrates a handle from nothing but the business
 *   chargeId (see the chargeId conventions below) and waits for its outcome.
 */

// chargeId conventions used across the handler so getCharge can dispatch on a bare string:
//   act-<orderId>              charge started by chargeOrder (Standalone Activity)
//   wf-<orderId>                charge started by chargeOrderViaWorkflow (Workflow run)
//   sub-<subId>-cycle-<n|rand>  one subscription billing cycle (see activities.ts)

export const payFastHandler = nexus.serviceHandler(payFast, {
  chargeOrder: new temporalNexus.TemporalOperationHandler<ChargeRequest, ChargeResult>({
    start: async (_ctx, client, input) => {
      // Handler-level chaos, distinct from the activity-level chaos in
      // chaos.ts: this fails *before* ever starting a backing activity, by
      // throwing synchronously from start() itself — a Nexus dispatch-level
      // failure, not an execution-level one. nexus-rpc's HandlerError
      // classifies INTERNAL/UNAVAILABLE/RESOURCE_EXHAUSTED/UPSTREAM_TIMEOUT/
      // REQUEST_TIMEOUT as retryable by default (see
      // nexus-rpc/src/common/handler-error.ts) — this is the "5 consecutive
      // retryable errors" a caller->endpoint circuit breaker would plausibly
      // count, unlike an activity retrying internally inside a long-running
      // backing execution (every other chaos token in this handler).
      if (input.cardToken === 'tok_handler_retryable_error') {
        throw new nexus.HandlerError('INTERNAL', 'simulated handler-level dispatch failure (chaos)');
      }
      return await client.startActivity('runCharge', {
        id: `act-${input.orderId}`,
        args: [input],
        scheduleToCloseTimeout: '2 minutes',
        heartbeatTimeout: '10 seconds',
        retry: { maximumAttempts: 6 },
      });
    },
  }),

  chargeOrderViaWorkflow: new temporalNexus.TemporalOperationHandler<ChargeRequest, ChargeResult>({
    start: async (_ctx, client, input) => {
      return await client.startWorkflow(chargeOrderWorkflow, {
        workflowId: `wf-${input.orderId}`,
        args: [input],
      });
    },
  }),

  startSubscription: new temporalNexus.TemporalOperationHandler<SubscriptionRequest, ChargeResult>({
    start: async (_ctx, client, input) => {
      const activityId =
        input.idStrategy === 'derived' ? `sub-${input.subId}-cycle-${input.cycleN}` : `sub-${input.subId}-cycle-${randomUUID()}`;
      return await client.startActivity('runSubscriptionCycle', {
        id: activityId,
        args: [input],
        scheduleToCloseTimeout: '2 minutes',
        retry: { maximumAttempts: 6 },
      });
    },
  }),

  getCharge: async (_ctx, input: ChargeHandle): Promise<ChargeResult> => {
    const client = temporalNexus.getClient();
    try {
      if (input.chargeId.startsWith('wf-')) {
        return await client.workflow.getHandle<typeof chargeOrderWorkflow>(input.chargeId).result();
      }
      return await client.activity.getHandle<ChargeResult>(input.chargeId).result();
    } catch (err) {
      return lookupFailure(input.chargeId, err);
    }
  },

  refundCharge: new temporalNexus.TemporalOperationHandler<ChargeHandle, RefundResult>({
    start: async (_ctx, client, input) => {
      return await client.startActivity('runRefund', {
        id: `refund-${input.chargeId}`,
        args: [input],
        scheduleToCloseTimeout: '1 minute',
      });
    },
  }),

  runLongTask: new temporalNexus.TemporalOperationHandler<LongTaskRequest, LongTaskResult>({
    start: async (_ctx, client, input) => {
      return await client.startActivity('runLongTaskActivity', {
        id: `task-${input.taskId}`,
        args: [input],
        scheduleToCloseTimeout: (input.durationSeconds + 300) * 1000,
        heartbeatTimeout: input.heartbeatIntervalSeconds * 3 * 1000,
      });
    },
  }),
});

function lookupFailure(chargeId: string, err: unknown): ChargeResult {
  return {
    chargeId,
    status: 'failed',
    amountCents: 0,
    backing: chargeId.startsWith('wf-') ? 'workflow' : 'standalone_activity',
    errorCode: 'lookup_failed',
    errorMessage: err instanceof Error ? err.message : String(err),
  };
}
