import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';

import { RETRY_TIER_TOPICS, type RetryTier } from '@/contracts/retry-tier';
import type { PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';

import type { ExecuteJobService } from '../application/execute-job.service';
import type { CancellationChecker } from '../domain/cancellation-checker.port';
import { processJobRequestedBody } from './process-job-requested';

const HEARTBEAT_INTERVAL_MS = 5_000;

export interface RetryTierConsumerOptions {
  readonly kafka: Kafka;
  readonly producer: Producer;
  readonly executeJobService: ExecuteJobService;
  readonly prisma: PrismaClient;
  readonly tier: RetryTier;
  readonly isJobCancelled?: CancellationChecker;
}

/**
 * One consumer per retry tier — each tier has its own delay, and a message
 * isn't dispatched until its `notBefore` has passed. The wait happens
 * *before* entering the idempotency wrapper's transaction (never inside a
 * Prisma $transaction, which has a short default timeout) and periodically
 * calls the payload's `heartbeat()` so KafkaJS doesn't consider this consumer
 * dead and trigger a rebalance during a long (tier-3) wait.
 */
export async function startRetryTierConsumer(options: RetryTierConsumerOptions): Promise<Consumer> {
  const { producer, executeJobService, tier, prisma, isJobCancelled } = options;
  const topic = RETRY_TIER_TOPICS[tier];
  const groupId = `eventforge.execution.retry-tier-${tier}-consumer`;

  const idempotentHandler = withIdempotency({ prisma, consumerName: groupId }, async (_payload, _tx, body) => {
    await processJobRequestedBody(body, { producer, executeJobService, isJobCancelled });
  });

  return createConsumer(options.kafka, {
    topic,
    groupId,
    autoCommit: false,
    handler: async (payload) => {
      await waitUntilDue(payload);
      await idempotentHandler(payload);
    },
  });
}

async function waitUntilDue(payload: EachMessagePayload): Promise<void> {
  const notBefore = extractNotBefore(payload.message.value?.toString());

  if (notBefore === null) {
    return;
  }

  const deadline = new Date(notBefore).getTime();

  while (Date.now() < deadline) {
    const remaining = deadline - Date.now();
    await sleep(Math.min(remaining, HEARTBEAT_INTERVAL_MS));
    await payload.heartbeat();
  }
}

function extractNotBefore(rawValue: string | undefined): string | null {
  if (!rawValue) {
    return null;
  }

  const body: unknown = JSON.parse(rawValue);
  const notBefore = (body as { notBefore?: unknown }).notBefore;

  return typeof notBefore === 'string' ? notBefore : null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
