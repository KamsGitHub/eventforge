import type { Consumer, Kafka } from 'kafkajs';

import { jobDeadLetteredEventSchemaV1 } from '@/contracts/events/job-dead-lettered.event';
import { JOBS_DEAD_LETTER_TOPIC } from '@/contracts/topics';
import type { PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';

import type { JobRepository } from '../domain/job-repository.port';

const CONSUMER_GROUP_ID = 'eventforge.jobs.dead-letter-consumer';

export interface DeadLetterConsumerOptions {
  readonly kafka: Kafka;
  readonly jobRepository: JobRepository;
  readonly prisma: PrismaClient;
}

/**
 * Maps a JobDeadLettered event (retries exhausted, or a message the
 * execution side couldn't even parse) to the FAILED -> DEAD_LETTERED
 * transition. A poison message's synthetic aggregateId won't match a real
 * job, so `findById` returning null is expected and handled as a no-op —
 * same pattern job-status.consumer.ts already uses.
 */
export async function startDeadLetterConsumer(options: DeadLetterConsumerOptions): Promise<Consumer> {
  const { jobRepository } = options;

  return createConsumer(options.kafka, {
    topic: JOBS_DEAD_LETTER_TOPIC,
    groupId: CONSUMER_GROUP_ID,
    autoCommit: false,
    handler: withIdempotency({ prisma: options.prisma, consumerName: CONSUMER_GROUP_ID }, async (_payload, tx, body) => {
      const envelope = jobDeadLetteredEventSchemaV1.parse(body);
      const job = await jobRepository.findById(envelope.aggregateId, tx);

      if (!job) {
        return;
      }

      await jobRepository.update(job.transitionTo('DEAD_LETTERED', { error: envelope.payload.error }), tx);
    }),
  });
}
