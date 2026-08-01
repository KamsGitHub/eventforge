import type { Consumer, Kafka } from 'kafkajs';

import { jobCancelledEventSchemaV1 } from '@/contracts/events/job-cancelled.event';
import { jobCompletedEventSchemaV1 } from '@/contracts/events/job-completed.event';
import { jobFailedEventSchemaV1 } from '@/contracts/events/job-failed.event';
import { jobRequestedEventSchemaV1 } from '@/contracts/events/job-requested.event';
import { jobStartedEventSchemaV1 } from '@/contracts/events/job-started.event';
import { RETRY_TIER_TOPICS } from '@/contracts/retry-tier';
import { JOBS_CANCELLED_TOPIC } from '@/contracts/topics';
import type { PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';

import type { JobRepository } from '../domain/job-repository.port';

const JOBS_STARTED_TOPIC = 'jobs.started';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';
const RETRY_TOPICS = Object.values(RETRY_TIER_TOPICS);
const CONSUMER_GROUP_ID = 'eventforge.jobs.job-status-consumer';

export interface JobStatusConsumerOptions {
  readonly kafka: Kafka;
  readonly jobRepository: JobRepository;
  readonly prisma: PrismaClient;
}

/**
 * The jobs module's own status consumer — the only writer of Job.status.
 * Execution (and later services) only ever publish events; they never
 * update the Job row directly.
 */
export async function startJobStatusConsumer(options: JobStatusConsumerOptions): Promise<Consumer> {
  const { jobRepository } = options;

  return createConsumer(options.kafka, {
    topic: [JOBS_STARTED_TOPIC, JOBS_COMPLETED_TOPIC, JOBS_FAILED_TOPIC, JOBS_CANCELLED_TOPIC, ...RETRY_TOPICS],
    groupId: CONSUMER_GROUP_ID,
    autoCommit: false,
    handler: withIdempotency({ prisma: options.prisma, consumerName: CONSUMER_GROUP_ID }, async ({ topic }, tx, body) => {
      const aggregateId = (body as { aggregateId?: string }).aggregateId;
      const job = aggregateId ? await jobRepository.findById(aggregateId, tx) : null;

      if (!job) {
        return;
      }

      if (RETRY_TOPICS.includes(topic)) {
        // A retry-tier dispatch means the job is being re-queued for
        // execution — same FAILED -> QUEUED transition as a manual retry
        // (M12), just automatically triggered.
        jobRequestedEventSchemaV1.parse(body);
        await jobRepository.update(job.transitionTo('QUEUED'), tx);
        return;
      }

      switch (topic) {
        case JOBS_STARTED_TOPIC: {
          jobStartedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('RUNNING'), tx);
          break;
        }
        case JOBS_COMPLETED_TOPIC: {
          const envelope = jobCompletedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('SUCCEEDED', { result: envelope.payload.result }), tx);
          break;
        }
        case JOBS_FAILED_TOPIC: {
          const envelope = jobFailedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('FAILED', { error: envelope.payload.error }), tx);
          break;
        }
        case JOBS_CANCELLED_TOPIC: {
          // The cooperative RUNNING cancellation path (Milestone 12):
          // execution observed cancelRequested and stopped itself.
          jobCancelledEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('CANCELLED'), tx);
          break;
        }
      }
    }),
  });
}
