import type { Consumer, Kafka } from 'kafkajs';

import { jobCompletedEventSchemaV1 } from '@/contracts/events/job-completed.event';
import { jobFailedEventSchemaV1 } from '@/contracts/events/job-failed.event';
import { jobStartedEventSchemaV1 } from '@/contracts/events/job-started.event';
import { createConsumer } from '@/messaging/consumer';

import type { JobRepository } from '../domain/job-repository.port';

const JOBS_STARTED_TOPIC = 'jobs.started';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';
const CONSUMER_GROUP_ID = 'eventforge.jobs.job-status-consumer';

export interface JobStatusConsumerOptions {
  readonly kafka: Kafka;
  readonly jobRepository: JobRepository;
}

/**
 * The jobs module's own status consumer — the only writer of Job.status.
 * Execution (and later services) only ever publish events; they never
 * update the Job row directly.
 */
export async function startJobStatusConsumer(options: JobStatusConsumerOptions): Promise<Consumer> {
  const { jobRepository } = options;

  return createConsumer(options.kafka, {
    topic: [JOBS_STARTED_TOPIC, JOBS_COMPLETED_TOPIC, JOBS_FAILED_TOPIC],
    groupId: CONSUMER_GROUP_ID,
    handler: async ({ topic, message }) => {
      const body: unknown = JSON.parse(message.value?.toString() ?? '{}');
      const aggregateId = (body as { aggregateId?: string }).aggregateId;
      const job = aggregateId ? await jobRepository.findById(aggregateId) : null;

      if (!job) {
        return;
      }

      switch (topic) {
        case JOBS_STARTED_TOPIC: {
          jobStartedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('RUNNING'));
          break;
        }
        case JOBS_COMPLETED_TOPIC: {
          const envelope = jobCompletedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('SUCCEEDED', { result: envelope.payload.result }));
          break;
        }
        case JOBS_FAILED_TOPIC: {
          const envelope = jobFailedEventSchemaV1.parse(body);
          await jobRepository.update(job.transitionTo('FAILED', { error: envelope.payload.error }));
          break;
        }
      }
    },
  });
}
