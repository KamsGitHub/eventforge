import type { Consumer, Kafka, Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_COMPLETED_EVENT_TYPE, JOB_COMPLETED_SCHEMA_VERSION } from '@/contracts/events/job-completed.event';
import { JOB_FAILED_EVENT_TYPE, JOB_FAILED_SCHEMA_VERSION } from '@/contracts/events/job-failed.event';
import { jobRequestedEventSchemaV1 } from '@/contracts/events/job-requested.event';
import { JOB_STARTED_EVENT_TYPE, JOB_STARTED_SCHEMA_VERSION } from '@/contracts/events/job-started.event';
import { createConsumer } from '@/messaging/consumer';
import { publish } from '@/messaging/producer';

import type { ExecuteJobService } from '../application/execute-job.service';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const JOBS_STARTED_TOPIC = 'jobs.started';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';
const CONSUMER_GROUP_ID = 'eventforge.execution.job-requested-consumer';

/** No retries yet (Milestone 8+) — every attempt handled here is attempt 1. */
const FIRST_ATTEMPT = 1;

export interface JobsRequestedConsumerOptions {
  readonly kafka: Kafka;
  readonly producer: Producer;
  readonly executeJobService: ExecuteJobService;
}

export async function startJobsRequestedConsumer(options: JobsRequestedConsumerOptions): Promise<Consumer> {
  const { producer, executeJobService } = options;

  return createConsumer(options.kafka, {
    topic: JOBS_REQUESTED_TOPIC,
    groupId: CONSUMER_GROUP_ID,
    handler: async ({ message }) => {
      const envelope = jobRequestedEventSchemaV1.parse(JSON.parse(message.value?.toString() ?? '{}'));
      const jobId = envelope.aggregateId;

      const started = createEnvelope({
        eventType: JOB_STARTED_EVENT_TYPE,
        aggregateId: jobId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        schemaVersion: JOB_STARTED_SCHEMA_VERSION,
        payload: { attempt: FIRST_ATTEMPT },
      });

      await publish(producer, { topic: JOBS_STARTED_TOPIC, key: jobId, value: started });

      try {
        const result = await executeJobService.execute(envelope.payload.type, envelope.payload.payload);

        const completed = createEnvelope({
          eventType: JOB_COMPLETED_EVENT_TYPE,
          aggregateId: jobId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          schemaVersion: JOB_COMPLETED_SCHEMA_VERSION,
          payload: { result },
        });

        await publish(producer, { topic: JOBS_COMPLETED_TOPIC, key: jobId, value: completed });
      } catch (error) {
        const failed = createEnvelope({
          eventType: JOB_FAILED_EVENT_TYPE,
          aggregateId: jobId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          schemaVersion: JOB_FAILED_SCHEMA_VERSION,
          payload: { attempt: FIRST_ATTEMPT, error: error instanceof Error ? error.message : String(error) },
        });

        await publish(producer, { topic: JOBS_FAILED_TOPIC, key: jobId, value: failed });
      }
    },
  });
}
