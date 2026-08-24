import {
  CancellationScope,
  createNexusServiceClient,
  defineQuery,
  defineSignal,
  log,
  NexusOperationCancellationType,
  proxyActivities,
  setHandler,
} from '@temporalio/workflow';
import type { Duration } from '@temporalio/common';
import type * as activities from './activities';
import { payFast } from '../../generated/services';
import type { ChargeRequest, ChargeResult } from '../../generated/models';

// Not imported from config.ts: workflow code runs in a deterministic sandbox and shouldn't
// pull in a module that reads process.env at load time.
const NEXUS_ENDPOINT = 'payfast-bug-bash';

const { runCharge } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 5 },
});

/**
 * Workflow-backed twin of `chargeOrder`. Runs the exact same activity code, just scheduled by
 * a Workflow instead of started as a Standalone Activity — see chargeOrder vs
 * chargeOrderViaWorkflow in the IDL for the scenario this exists to support.
 */
export async function chargeOrderWorkflow(request: ChargeRequest): Promise<ChargeResult> {
  return await runCharge(request);
}

// --- Generic scenario-driver workflow, used by the client harness (src/client/scenarios.ts) ---
// Invokes any PayFast operation as a caller Workflow (the tested "SAA-via-Nexus" path: a
// Workflow schedules the Nexus operation; the handler may back it with a Standalone Activity).
// Exposes the Nexus operation token via query and supports mid-flight cancellation via signal,
// so a single reusable workflow can drive scenarios 1, 2, 3, 5, and 6.

export const getOperationToken = defineQuery<string | undefined>('getOperationToken');
export const requestCancel = defineSignal('requestCancel');

export interface DriverInput {
  operation: keyof typeof payFast.operations & string;
  input: unknown;
  scheduleToCloseTimeout?: Duration;
  cancellationType?: keyof typeof NexusOperationCancellationType;
}

export interface DriverResult {
  ok: boolean;
  output?: unknown;
  errorMessage?: string;
  token?: string;
}

export async function runPayfastOperation(request: DriverInput): Promise<DriverResult> {
  let token: string | undefined;
  setHandler(getOperationToken, () => token);

  const nexusClient = createNexusServiceClient({ endpoint: NEXUS_ENDPOINT, service: payFast });

  return await CancellationScope.cancellable(async () => {
    setHandler(requestCancel, () => {
      CancellationScope.current().cancel();
    });

    try {
      const handle = await nexusClient.startOperation(request.operation, request.input as never, {
        scheduleToCloseTimeout: request.scheduleToCloseTimeout,
        cancellationType: request.cancellationType
          ? NexusOperationCancellationType[request.cancellationType]
          : undefined,
      });
      token = handle.token;
      const output = await handle.result();
      return { ok: true, output, token };
    } catch (err) {
      log.warn('payfast operation failed', { operation: request.operation, error: (err as Error).message });
      return { ok: false, errorMessage: (err as Error).message, token };
    }
  });
}
