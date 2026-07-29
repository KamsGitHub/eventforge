import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { Wait } from 'testcontainers';

import { createEnvelope } from '@/contracts/envelope';
import { jobCompletedEventSchemaV1 } from '@/contracts/events/job-completed.event';
import { jobFailedEventSchemaV1 } from '@/contracts/events/job-failed.event';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import {
  GENERATE_REPORT_JOB_TYPE,
  GenerateReportHandler,
} from '@/modules/execution/domain/handlers/generate-report.handler';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';

describe('jobs-requested consumer (real broker via Testcontainers)', () => {
  // Same Confluent-image + explicit-readiness-wait workaround as the
  // Milestone 6 foundation test — see CLAUDE.md for why.
  let container: StartedKafkaContainer;
  let kafka: Kafka;
  let producer: Producer;
  let jobsRequestedConsumer: Consumer;

  beforeAll(async () => {
    container = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
      .withKraft()
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();

    kafka = createKafkaClient({
      brokers: [`${container.getHost()}:${container.getMappedPort(9093)}`],
      clientId: 'eventforge-test',
    });

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [JOBS_REQUESTED_TOPIC, JOBS_COMPLETED_TOPIC, JOBS_FAILED_TOPIC].map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
    });
    await admin.disconnect();

    producer = createProducer(kafka);
    await producer.connect();

    const executeJobService = new ExecuteJobService(new Map([[GENERATE_REPORT_JOB_TYPE, new GenerateReportHandler()]]));
    jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService });
  }, 120_000);

  afterAll(async () => {
    await jobsRequestedConsumer?.disconnect();
    await producer?.disconnect();
    await container?.stop();
  }, 120_000);

  async function consumeOne(
    topic: string,
    groupId: string,
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
        resolveMessage(payload);
        return Promise.resolve();
      },
    });

    return { consumer, received };
  }

  it('consumes JobRequested and publishes JobCompleted on success', async () => {
    const { consumer, received } = await consumeOne(JOBS_COMPLETED_TOPIC, 'test.jobs-completed-consumer');

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId: 'job-success-1',
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: { reportName: 'Q1 Revenue' }, maxAttempts: 3 },
    });

    await publish(producer, { topic: JOBS_REQUESTED_TOPIC, key: requested.aggregateId, value: requested });

    const message = await received;
    const parsed = jobCompletedEventSchemaV1.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

    expect(parsed.aggregateId).toBe('job-success-1');
    expect(parsed.correlationId).toBe(requested.correlationId);
    expect(parsed.causationId).toBe(requested.eventId);
    expect(parsed.payload.result).toMatchObject({ reportName: 'Q1 Revenue' });

    await consumer.disconnect();
  }, 120_000);

  it('consumes JobRequested and publishes JobFailed when the handler rejects', async () => {
    const { consumer, received } = await consumeOne(JOBS_FAILED_TOPIC, 'test.jobs-failed-consumer');

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId: 'job-failure-1',
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      // Missing reportName — GenerateReportHandler rejects on invalid payload.
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: {}, maxAttempts: 3 },
    });

    await publish(producer, { topic: JOBS_REQUESTED_TOPIC, key: requested.aggregateId, value: requested });

    const message = await received;
    const parsed = jobFailedEventSchemaV1.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

    expect(parsed.aggregateId).toBe('job-failure-1');
    expect(parsed.correlationId).toBe(requested.correlationId);
    expect(parsed.causationId).toBe(requested.eventId);
    expect(parsed.payload.attempt).toBe(1);

    await consumer.disconnect();
  }, 120_000);
});
