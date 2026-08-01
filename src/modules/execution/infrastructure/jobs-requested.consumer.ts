import type { Consumer, Kafka, Producer } from 'kafkajs';

import type { PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';

import type { ExecuteJobService } from '../application/execute-job.service';
import type { CancellationChecker } from '../domain/cancellation-checker.port';
import { processJobRequestedBody } from './process-job-requested';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const CONSUMER_GROUP_ID = 'eventforge.execution.job-requested-consumer';

export interface JobsRequestedConsumerOptions {
  readonly kafka: Kafka;
  readonly producer: Producer;
  readonly executeJobService: ExecuteJobService;
  readonly prisma: PrismaClient;
  readonly isJobCancelled?: CancellationChecker;
}

export async function startJobsRequestedConsumer(options: JobsRequestedConsumerOptions): Promise<Consumer> {
  const { producer, executeJobService, isJobCancelled } = options;

  return createConsumer(options.kafka, {
    topic: JOBS_REQUESTED_TOPIC,
    groupId: CONSUMER_GROUP_ID,
    autoCommit: false,
    handler: withIdempotency({ prisma: options.prisma, consumerName: CONSUMER_GROUP_ID }, async (_payload, _tx, body) => {
      await processJobRequestedBody(body, { producer, executeJobService, isJobCancelled });
    }),
  });
}
