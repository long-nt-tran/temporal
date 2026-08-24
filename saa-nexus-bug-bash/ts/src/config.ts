// Shared constants for the worker, activities, and client-side scripts.

export const NAMESPACE = process.env.PAYFAST_NAMESPACE ?? 'default';
export const TASK_QUEUE = process.env.PAYFAST_TASK_QUEUE ?? 'payfast-bug-bash';
export const NEXUS_ENDPOINT = process.env.PAYFAST_NEXUS_ENDPOINT ?? 'payfast-bug-bash';
export const SERVER_ADDRESS = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
