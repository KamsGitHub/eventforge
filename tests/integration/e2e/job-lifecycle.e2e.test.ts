import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { Consumer, Kafka, Producer } from 'kafkajs';
import { Wait } from 'testcontainers';

import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import {
  GENERATE_REPORT_JOB_TYPE,
  GenerateReportHandler,
} from '@/modules/execution/domain/handlers/generate-report.handler';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';
import type { JobResponse } from '@/modules/jobs/api/schemas';
import { JobService } from '@/modules/jobs/application/job.service';
import { createJobOutboxPublishedHandler } from '@/modules/jobs/infrastructure/job-outbox-published.handler';
import { startJobStatusConsumer } from '@/modules/jobs/infrastructure/job-status.consumer';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';
import { OutboxPublisher } from '@/outbox/outbox-publisher';

import { createTestDatabase, type TestDatabase } from '../helpers/postgres-test-db';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const JOBS_STARTED_TOPIC = 'jobs.started';
const JOBS_COMPLETED_TOPIC = 'jobs.completed';
const JOBS_FAILED_TOPIC = 'jobs.failed';
const JOBS_RETRY_1_TOPIC = 'jobs.retry-1';
const JOBS_RETRY_2_TOPIC = 'jobs.retry-2';
const JOBS_RETRY_3_TOPIC = 'jobs.retry-3';
const JOBS_DEAD_LETTER_TOPIC = 'jobs.dead-letter';
const JOBS_CANCELLED_TOPIC = 'jobs.cancelled';

async function pollUntilTerminal(app: ReturnType<typeof buildApp>, jobId: string): Promise<JobResponse> {
  const terminal = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'DEAD_LETTERED']);
  const deadline = Date.now() + 30_000;

  for (;;) {
    const response = await app.inject({ method: 'GET', url: `/api/jobs/${jobId}` });
    const body = response.json<JobResponse>();

    if (terminal.has(body.status)) {
      return body;
    }

    if (Date.now() > deadline) {
      throw new Error(`job ${jobId} did not reach a terminal state in time (last status: ${body.status})`);
    }

    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

describe('job lifecycle end-to-end (real Kafka + Postgres via Testcontainers)', () => {
  let dbContainer: TestDatabase;
  let kafkaContainer: StartedKafkaContainer;
  let prisma: PrismaClient;
  let kafka: Kafka;
  let producer: Producer;
  let jobsRequestedConsumer: Consumer;
  let jobStatusConsumer: Consumer;
  let outboxPublisher: OutboxPublisher;
  let app: ReturnType<typeof buildApp>;

  beforeAll(async () => {
    dbContainer = await createTestDatabase();
    prisma = createPrismaClient(dbContainer.connectionUri);

    kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
      .withKraft()
      // Same Confluent-image + explicit-readiness-wait workaround used by
      // the other Kafka Testcontainers suites — see CLAUDE.md.
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();

    kafka = createKafkaClient({
      brokers: [`${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`],
      clientId: 'eventforge-test',
    });

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        JOBS_REQUESTED_TOPIC,
        JOBS_STARTED_TOPIC,
        JOBS_COMPLETED_TOPIC,
        JOBS_FAILED_TOPIC,
        JOBS_RETRY_1_TOPIC,
        JOBS_RETRY_2_TOPIC,
        JOBS_RETRY_3_TOPIC,
        JOBS_DEAD_LETTER_TOPIC,
        JOBS_CANCELLED_TOPIC,
      ].map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
    });
    await admin.disconnect();

    producer = createProducer(kafka);
    await producer.connect();

    const jobRepository = new PrismaJobRepository(prisma);
    const jobService = new JobService(jobRepository);

    const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: dbContainer.connectionUri });
    app = buildApp({ config, jobService });

    const executeJobService = new ExecuteJobService(new Map([[GENERATE_REPORT_JOB_TYPE, new GenerateReportHandler()]]));
    jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma });
    jobStatusConsumer = await startJobStatusConsumer({ kafka, jobRepository, prisma });

    outboxPublisher = new OutboxPublisher({
      prisma,
      producer,
      pollIntervalMs: 200,
      onPublished: createJobOutboxPublishedHandler(jobRepository),
    });
    outboxPublisher.start();
  }, 120_000);

  afterAll(async () => {
    outboxPublisher?.stop();
    await app?.close();
    await jobsRequestedConsumer?.disconnect();
    await jobStatusConsumer?.disconnect();
    await producer?.disconnect();
    await prisma?.$disconnect();
    await kafkaContainer?.stop();
    await dbContainer?.container.stop();
  }, 120_000);

  afterEach(async () => {
    await prisma.job.deleteMany();
    await prisma.outboxEvent.deleteMany();
    await prisma.processedEvent.deleteMany();
  });

  it('runs a job through the full spec workflow: PENDING -> QUEUED -> RUNNING -> SUCCEEDED', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: { reportName: 'Q1 Revenue' } },
    });

    expect(created.statusCode).toBe(201);
    const job = created.json<JobResponse>();
    expect(job.status).toBe('PENDING');

    const final = await pollUntilTerminal(app, job.id);

    expect(final.status).toBe('SUCCEEDED');
    expect(final.result).toMatchObject({ reportName: 'Q1 Revenue' });
    expect(final.startedAt).toEqual(expect.any(String));
    expect(final.completedAt).toEqual(expect.any(String));
  }, 120_000);

  it('runs a failing job through the full spec workflow, ending in FAILED', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      // Missing reportName — GenerateReportHandler rejects on invalid payload.
      payload: { type: GENERATE_REPORT_JOB_TYPE, payload: {} },
    });

    expect(created.statusCode).toBe(201);
    const job = created.json<JobResponse>();

    const final = await pollUntilTerminal(app, job.id);

    expect(final.status).toBe('FAILED');
    expect(final.error).toEqual(expect.any(String));
    expect(final.startedAt).toEqual(expect.any(String));
  }, 120_000);
});
