// Local-dev worker entrypoint (long-polling). Connects to a self-hosted
// server by default; set TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE/TEMPORAL_API_KEY
// (same names as deploy/.env.local) to point it at Temporal Cloud instead —
// loadClientConnectConfig() picks those up automatically and turns TLS on
// once an API key is present. Override the task queue with PAYFAST_TASK_QUEUE
// to run a second, differently-queued instance of this same worker (e.g.
// against Temporal Cloud, alongside the Lambda-hosted one).
import { loadClientConnectConfig } from '@temporalio/envconfig';
import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { NAMESPACE, SERVER_ADDRESS, TASK_QUEUE } from './config';
import { payFastHandler } from './handler';

async function main() {
  const { connectionOptions, namespace } = loadClientConnectConfig();
  const connection = await NativeConnection.connect({
    ...connectionOptions,
    address: connectionOptions.address ?? SERVER_ADDRESS,
  });
  const effectiveNamespace = namespace ?? NAMESPACE;
  try {
    const worker = await Worker.create({
      connection,
      namespace: effectiveNamespace,
      taskQueue: TASK_QUEUE,
      workflowsPath: require.resolve('./workflows'),
      activities,
      nexusServices: [payFastHandler],
    });
    console.log(`PayFast worker polling task queue "${TASK_QUEUE}" in namespace "${effectiveNamespace}"`);
    await worker.run();
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
