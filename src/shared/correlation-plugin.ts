import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';

const CORRELATION_HEADER = 'x-correlation-id';

/**
 * Passed as Fastify's `genReqId` option (app.ts) rather than implemented as
 * an onRequest hook rebinding request.log: Fastify constructs request.log
 * (and reply's own log reference) from request.id *before* any onRequest
 * hook runs, so a hook-based rebind never reaches Fastify's own built-in
 * "incoming request"/"request completed" log lines — only genReqId does.
 * Reuses an inbound x-correlation-id header when present (so a caller can
 * thread its own trace through us) and generates one otherwise. Since
 * request.id becomes this value, every log line for the request — Fastify's
 * own and anything a route logs via request.log — carries it as `reqId`
 * for free, and it's also what routes.ts passes as the Job's correlationId.
 */
export function generateCorrelationId(req: IncomingMessage): string {
  const header = req.headers[CORRELATION_HEADER];

  return typeof header === 'string' && header.length > 0 ? header : randomUUID();
}
