import { randomUUID } from 'node:crypto';

import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { jobCompletedEventSchemaV1 } from '@/contracts/events/job-completed.event';
import { jobFailedEventSchemaV1 } from '@/contracts/events/job-failed.event';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import {
  GENERATE_REPORT_JOB_TYPE,
  GenerateReportHandler,
} from '@/modules/execution/domain/handlers/generate-report.handler';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';

import { sharedDatabaseUrl, sharedKafkaBrokers } from '../setup';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';

describe('jobs-requested consumer (real broker via Testcontainers)', () => {
  let kafka: Kafka;
  let producer: Producer;
  let jobsRequestedConsumer: Consumer;
  let prisma: PrismaClient;

  beforeAll(async () => {
    prisma = createPrismaClient(sharedDatabaseUrl());
    kafka = createKafkaClient({ brokers: sharedKafkaBrokers(), clientId: 'eventforge-test' });

    producer = createProducer(kafka);
    await producer.connect();

    const executeJobService = new ExecuteJobService(new Map([[GENERATE_REPORT_JOB_TYPE, new GenerateReportHandler()]]));
    jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma });
  }, 60_000);

  afterAll(async () => {
    await jobsRequestedConsumer?.disconnect();
    await producer?.disconnect();
    await prisma?.$disconnect();
  }, 30_000);

  /**
   * jobs.completed/jobs.failed are the real, fixed production topics — now
   * shared across the whole test run (see tests/integration/setup.ts), not
   * fresh per file. `fromBeginning: true` + "resolve on the first message"
   * would pick up a stale message from an earlier file's tests sharing the
   * same topic, so this filters for the specific job this test created —
   * same pattern retry-and-dead-letter.e2e.test.ts already uses.
   */
  async function consumeMatching(
    topic: string,
    groupId: string,
    predicate: (body: unknown) => boolean,
  ): Promise<{ consumer: Consumer; received: Promise<EachMessagePayload> }> {
    let resolveMessage!: (payload: EachMessagePayload) => void;
    const received = new Promise<EachMessagePayload>((resolve) => {
      resolveMessage = resolve;
    });

    const consumer = await createConsumer(kafka, {
      topic,
      groupId,
      fromBeginning: true,
      handler: (payload) => {
        const body: unknown = JSON.parse(payload.message.value?.toString() ?? '{}');

        if (predicate(body)) {
          resolveMessage(payload);
        }

        return Promise.resolve();
      },
    });

    return { consumer, received };
  }

  it('consumes JobRequested and publishes JobCompleted on success', async () => {
    const aggregateId = `job-success-${randomUUID()}`;
    const { consumer, received } = await consumeMatching(
      JOBS_COMPLETED_TOPIC,
      `test.jobs-completed-consumer.${randomUUID()}`,
      (body) => (body as { aggregateId?: string }).aggregateId === aggregateId,
    );

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId,
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: { reportName: 'Q1 Revenue' }, maxAttempts: 3, attempt: 1, failureHistory: [] },
    });

    try {
      await publish(producer, { topic: JOBS_REQUESTED_TOPIC, key: requested.aggregateId, value: requested });

      const message = await received;
      const parsed = jobCompletedEventSchemaV1.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

      expect(parsed.aggregateId).toBe(aggregateId);
      expect(parsed.correlationId).toBe(requested.correlationId);
      expect(parsed.causationId).toBe(requested.eventId);
      expect(parsed.payload.result).toMatchObject({ reportName: 'Q1 Revenue' });
    } finally {
      // On the shared broker this group/consumer outlives a single test —
      // an assertion throwing above must not skip this, or the leaked
      // consumer keeps fetching after Jest tears this file's environment
      // down (surfaces as a "require after teardown" error much later).
      await consumer.disconnect();
    }
  }, 120_000);

  it('consumes JobRequested and publishes JobFailed when the handler rejects', async () => {
    const aggregateId = `job-failure-${randomUUID()}`;
    const { consumer, received } = await consumeMatching(
      JOBS_FAILED_TOPIC,
      `test.jobs-failed-consumer.${randomUUID()}`,
      (body) => (body as { aggregateId?: string }).aggregateId === aggregateId,
    );

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId,
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      // Missing reportName — GenerateReportHandler rejects on invalid payload.
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: {}, maxAttempts: 3, attempt: 1, failureHistory: [] },
    });

    try {
      await publish(producer, { topic: JOBS_REQUESTED_TOPIC, key: requested.aggregateId, value: requested });

      const message = await received;
      const parsed = jobFailedEventSchemaV1.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

      expect(parsed.aggregateId).toBe(aggregateId);
      expect(parsed.correlationId).toBe(requested.correlationId);
      expect(parsed.causationId).toBe(requested.eventId);
      expect(parsed.payload.attempt).toBe(1);
    } finally {
      await consumer.disconnect();
    }
  }, 120_000);
});
