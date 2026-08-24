// AWS Lambda entrypoint for PayFast. One worker, serving Nexus + activities +
// the workflow-backed chargeOrderViaWorkflow twin — see handler.ts.
//
// Separate from worker.ts (the local-dev, long-polling entrypoint): this file
// always runs as a Lambda, via @temporalio/lambda-worker's runWorker.
//
// Env vars:
//   WORKER_DEPLOYMENT_NAME    required — Temporal Worker Deployment name
//   WORKER_BUILD_ID           default "dev" — set to the deploy script's build-id
//   NEXUS_TASK_QUEUE          default TASK_QUEUE from config.ts
//   TEMPORAL_API_KEY_SECRET_ARN   optional — Secrets Manager ARN holding the
//     Temporal Cloud API key. Resolved into TEMPORAL_API_KEY before runWorker
//     builds its connection config: @temporalio/envconfig only enables TLS
//     when TEMPORAL_API_KEY is set, so this must happen first.
//   plus the usual TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE read by envconfig.
import * as path from 'node:path';
import type { Context } from 'aws-lambda';
import { runWorker } from '@temporalio/lambda-worker';
import * as activities from './activities';
import { payFastHandler } from './handler';
import { TASK_QUEUE } from './config';

console.log(`payfast worker-lambda module loading (build ${process.env.WORKER_BUILD_ID ?? 'dev'})`);

const deploymentName = process.env.WORKER_DEPLOYMENT_NAME;
if (!deploymentName) {
  throw new Error('WORKER_DEPLOYMENT_NAME env var not set');
}

async function resolveApiKey(): Promise<void> {
  const secretArn = process.env.TEMPORAL_API_KEY_SECRET_ARN;
  if (secretArn && !process.env.TEMPORAL_API_KEY) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
    const resp = await new SecretsManagerClient({}).send(new GetSecretValueCommand({ SecretId: secretArn }));
    if (!resp.SecretString) {
      throw new Error(`secret ${secretArn} has no SecretString`);
    }
    process.env.TEMPORAL_API_KEY = resp.SecretString;
  }
}

const initPromise = resolveApiKey().then(() =>
  runWorker({ deploymentName, buildId: process.env.WORKER_BUILD_ID ?? 'dev' }, (config) => {
    config.workerOptions.taskQueue = process.env.NEXUS_TASK_QUEUE ?? TASK_QUEUE;
    config.workerOptions.activities = activities;
    // Compiled worker-lambda.js lives at out/ts/src/worker-lambda.js;
    // bundle-workflows.js writes the bundle to out/workflow-bundle.js
    // (two levels up: src -> ts -> out), not one.
    config.workerOptions.workflowBundle = { codePath: path.join(__dirname, '..', '..', 'workflow-bundle.js') };
    config.workerOptions.nexusServices = [payFastHandler];
  })
);

export const handler = async (event: unknown, context: Context) => (await initPromise)(event, context);

