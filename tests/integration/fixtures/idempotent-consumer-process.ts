/**
 * Standalone consumer process, spawned (and killed) by the rebalance and
 * graceful-shutdown tests — those need to simulate a real crash/SIGTERM,
 * which isn't possible against the Jest process itself. Reuses the same
 * production createConsumer + withIdempotency wrappers real consumers use,
 * so what's being proven is the real crash-safety property (offset commits
 * only after the handler resolves), not a synthetic stand-in for it.
 *
 * Talks to the parent test over stdout as newline-delimited JSON: one line
 * per lifecycle event (`ready`, `processing-start`, `processing-done`).
 * Configured entirely via env vars (see the FIXTURE_* reads below) so the
 * parent test can spawn it with `child_process.spawn` + `env`.
 */
import { createPrismaClient } from '../../../src/db/prisma-client';
import { createConsumer } from '../../../src/messaging/consumer';
import { withIdempotency } from '../../../src/messaging/idempotent-consumer';
import { createKafkaClient } from '../../../src/messaging/kafka-client';

function emit(event: Record<string, unknown>): void {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

async function main(): Promise<void> {
  const brokers = (process.env['FIXTURE_KAFKA_BROKERS'] ?? '').split(',');
  const topic = process.env['FIXTURE_TOPIC'];
  const groupId = process.env['FIXTURE_GROUP_ID'];
  const databaseUrl = process.env['FIXTURE_DATABASE_URL'];
  const delayMs = Number(process.env['FIXTURE_DELAY_MS'] ?? '500');
  const sessionTimeout = process.env['FIXTURE_SESSION_TIMEOUT_MS'] ? Number(process.env['FIXTURE_SESSION_TIMEOUT_MS']) : undefined;
  const heartbeatInterval = process.env['FIXTURE_HEARTBEAT_INTERVAL_MS']
    ? Number(process.env['FIXTURE_HEARTBEAT_INTERVAL_MS'])
    : undefined;

  if (!topic || !groupId || !databaseUrl) {
    throw new Error('FIXTURE_TOPIC, FIXTURE_GROUP_ID, and FIXTURE_DATABASE_URL are required');
  }

  const prisma = createPrismaClient(databaseUrl);
  const kafka = createKafkaClient({ brokers, clientId: `fixture-${process.pid}` });

  await createConsumer(kafka, {
    topic,
    groupId,
    fromBeginning: true,
    autoCommit: false,
    sessionTimeout,
    heartbeatInterval,
    handler: withIdempotency({ prisma, consumerName: groupId }, async (payload) => {
      const body = JSON.parse(payload.message.value?.toString() ?? '{}') as { eventId: string };

      emit({ event: 'processing-start', eventId: body.eventId, pid: process.pid });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      emit({ event: 'processing-done', eventId: body.eventId, pid: process.pid });
    }),
  });

  emit({ event: 'ready', pid: process.pid });
}

main().catch((error: unknown) => {
  emit({ event: 'error', message: error instanceof Error ? error.message : String(error) });
  process.exit(1);
});
