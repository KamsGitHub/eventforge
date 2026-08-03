import type { Consumer, Kafka, Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { jobFailedEventSchemaV1 } from '@/contracts/events/job-failed.event';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import type { PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';
import { publish } from '@/messaging/producer';
import type { Logger } from '@/shared/logger';

import { RETRY_TIER_TOPICS, tierForAttempt } from '@/contracts/retry-tier';

import { publishDeadLetter, publishMalformedMessageToDeadLetter } from './dead-letter';

const JOBS_FAILED_TOPIC = 'jobs.failed';
const CONSUMER_GROUP_ID = 'eventforge.execution.retry-router';

export interface RetryRouterOptions {
  readonly kafka: Kafka;
  readonly producer: Producer;
  readonly prisma: PrismaClient;
  readonly retryTierDelaysMs: Readonly<Record<1 | 2 | 3, number>>;
  readonly logger?: Logger;
}

/**
 * Consumes every JobFailed event and decides what happens next: re-dispatch
 * to the appropriate jobs.retry-N topic (with `notBefore` set to honor that
 * tier's delay), or route to jobs.dead-letter once `attempt >= maxAttempts`.
 * Never touches the Job row (execution module rule) — Job.status still only
 * ever changes via the jobs module's own consumers reacting to these events.
 */
export async function startRetryRouter(options: RetryRouterOptions): Promise<Consumer> {
  const { producer, retryTierDelaysMs } = options;

  return createConsumer(options.kafka, {
    topic: JOBS_FAILED_TOPIC,
    groupId: CONSUMER_GROUP_ID,
    autoCommit: false,
    logger: options.logger,
    handler: withIdempotency({ prisma: options.prisma, consumerName: CONSUMER_GROUP_ID }, async (msg, _tx, body) => {
      const parsed = jobFailedEventSchemaV1.safeParse(body);

      if (!parsed.success) {
        await publishMalformedMessageToDeadLetter(producer, body, parsed.error.message, msg.logger);
        return;
      }

      const envelope = parsed.data;
      const jobId = envelope.aggregateId;
      const { attempt, maxAttempts, type, payload, error, failureHistory } = envelope.payload;

      if (attempt >= maxAttempts) {
        await publishDeadLetter(
          producer,
          {
            jobId,
            correlationId: envelope.correlationId,
            causationId: envelope.eventId,
            type,
            payload,
            attempt,
            maxAttempts,
            error,
            failureHistory,
          },
          msg.logger,
        );
        return;
      }

      const tier = tierForAttempt(attempt);
      const notBefore = new Date(Date.now() + retryTierDelaysMs[tier]).toISOString();

      const retried = createEnvelope({
        eventType: JOB_REQUESTED_EVENT_TYPE,
        aggregateId: jobId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
        notBefore,
        payload: { type, payload, maxAttempts, attempt: attempt + 1, failureHistory },
      });

      await publish(producer, { topic: RETRY_TIER_TOPICS[tier], key: jobId, value: retried, logger: msg.logger });
    }),
  });
}
