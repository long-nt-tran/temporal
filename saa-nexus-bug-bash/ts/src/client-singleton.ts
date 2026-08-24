import { Client, Connection } from '@temporalio/client';
import { loadClientConnectConfig } from '@temporalio/envconfig';
import { NAMESPACE, SERVER_ADDRESS } from './config';

// Standalone activities (runSubscriptionCycle in particular) start further Standalone
// Activities directly from within an activity, using a plain Client held by the worker
// process — the same way any external process would. Lazily created and cached per
// Node process. Uses envconfig (TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE/TEMPORAL_API_KEY, same
// as worker.ts) so this works against Temporal Cloud, not just a local self-hosted server.
let clientPromise: Promise<Client> | undefined;

export async function getClient(): Promise<Client> {
  if (!clientPromise) {
    const { connectionOptions, namespace } = loadClientConnectConfig();
    clientPromise = Connection.connect({
      ...connectionOptions,
      address: connectionOptions.address ?? SERVER_ADDRESS,
    }).then((connection) => new Client({ connection, namespace: namespace ?? NAMESPACE }));
  }
  return clientPromise;
}
