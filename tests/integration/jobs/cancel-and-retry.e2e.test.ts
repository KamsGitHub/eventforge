import type { Consumer, Kafka, Producer } from 'kafkajs';

import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import type { JobExecutionContext, JobHandler } from '@/modules/execution/domain/job-handler.port';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';
import { startRetryRouter } from '@/modules/execution/infrastructure/retry-router';
import { startRetryTierConsumer } from '@/modules/execution/infrastructure/retry-tier.consumer';
import type { JobResponse } from '@/modules/jobs/api/schemas';
import { JobService } from '@/modules/jobs/application/job.service';
import { startDeadLetterConsumer } from '@/modules/jobs/infrastructure/dead-letter.consumer';
import { createJobOutboxPublishedHandler } from '@/modules/jobs/infrastructure/job-outbox-published.handler';
import { startJobStatusConsumer } from '@/modules/jobs/infrastructure/job-status.consumer';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';
import { OutboxPublisher } from '@/outbox/outbox-publisher';

import { sharedDatabaseUrl, sharedKafkaBrokers } from '../setup';

const TOGGLEABLE_JOB_TYPE = 'TOGGLEABLE';
const TICKING_JOB_TYPE = 'TICKING';
const TICK_MS = 100;
const MAX_TICKS = 30;

/** Fails or succeeds depending on a flag the test flips between phases. */
class ToggleableHandler implements JobHandler {
  shouldFail = true;

  execute(): Promise<unknown> {
    return this.shouldFail ? Promise.reject(new Error('handler currently set to fail')) : Promise.resolve({ done: true });
  }
}

/** Simulates real long-running work by polling isCancelled() between ticks. */
class TickingHandler implements JobHandler {
  async execute(_payload: unknown, context: JobExecutionContext): Promise<unknown> {
    for (let tick = 0; tick < MAX_TICKS; tick++) {
      if (await context.isCancelled()) {
        throw new Error('cooperative cancellation observed mid-run');
      }

      await new Promise((resolve) => setTimeout(resolve, TICK_MS));
    }

    return { done: true };
  }
}

async function pollUntil(
  app: ReturnType<typeof buildApp>,
  jobId: string,
  predicate: (job: JobResponse) => boolean,
  timeoutMs = 30_000,
): Promise<JobResponse> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const response = await app.inject({ method: 'GET', url: `/api/jobs/${jobId}` });
    const body = response.json<JobResponse>();

    if (predicate(body)) {
      return body;
    }

    if (Date.now() > deadline) {
      throw new Error(`job ${jobId} never satisfied the predicate in time (last status: ${body.status})`);
    }

    await new Promise((resolve) => setTimeout(resolve, 150));
  }
}

describe('cancellation and manual retry end-to-end (real Kafka + Postgres via Testcontainers)', () => {
  let prisma: PrismaClient;
  let kafka: Kafka;
  let producer: Producer;
  let jobRepository: PrismaJobRepository;
  let app: ReturnType<typeof buildApp>;
  let toggleableHandler: ToggleableHandler;

  let jobsRequestedConsumer: Consumer;
  let retryTier1Consumer: Consumer;
  let retryTier2Consumer: Consumer;
  let retryTier3Consumer: Consumer;
  let retryRouter: Consumer;
  let deadLetterConsumer: Consumer;
  let jobStatusConsumer: Consumer;
  let outboxPublisher: OutboxPublisher;

  beforeAll(async () => {
    prisma = createPrismaClient(sharedDatabaseUrl());
    kafka = createKafkaClient({ brokers: sharedKafkaBrokers(), clientId: 'eventforge-test' });

    producer = createProducer(kafka);
    await producer.connect();

    jobRepository = new PrismaJobRepository(prisma);
    const jobService = new JobService(jobRepository);

    const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: sharedDatabaseUrl() });
    app = buildApp({ config, jobService });

    toggleableHandler = new ToggleableHandler();
    const executeJobService = new ExecuteJobService(
      new Map<string, JobHandler>([
        [TOGGLEABLE_JOB_TYPE, toggleableHandler],
        [TICKING_JOB_TYPE, new TickingHandler()],
      ]),
    );

    async function isJobCancelled(jobId: string): Promise<boolean> {
      const job = await jobRepository.findById(jobId);

      return job !== null && (job.props.status === 'CANCELLED' || job.props.cancelRequested);
    }

    const retryTierDelaysMs = { 1: 200, 2: 300, 3: 400 };

    jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma, isJobCancelled });
    retryTier1Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 1, isJobCancelled });
    retryTier2Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 2, isJobCancelled });
    retryTier3Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 3, isJobCancelled });
    retryRouter = await startRetryRouter({ kafka, producer, prisma, retryTierDelaysMs });
    deadLetterConsumer = await startDeadLetterConsumer({ kafka, jobRepository, prisma });
    jobStatusConsumer = await startJobStatusConsumer({ kafka, jobRepository, prisma });

    outboxPublisher = new OutboxPublisher({
      prisma,
      producer,
      pollIntervalMs: 100,
      onPublished: createJobOutboxPublishedHandler(jobRepository),
    });
    outboxPublisher.start();
  }, 60_000);

  afterAll(async () => {
    outboxPublisher?.stop();
    await app?.close();
    await jobsRequestedConsumer?.disconnect();
    await retryTier1Consumer?.disconnect();
    await retryTier2Consumer?.disconnect();
    await retryTier3Consumer?.disconnect();
    await retryRouter?.disconnect();
    await deadLetterConsumer?.disconnect();
    await jobStatusConsumer?.disconnect();
    await producer?.disconnect();
    await prisma?.$disconnect();
  }, 30_000);

  afterEach(async () => {
    await prisma.job.deleteMany();
    await prisma.outboxEvent.deleteMany();
    await prisma.processedEvent.deleteMany();
  });

  it('cancels a PENDING job immediately via the API', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: TOGGLEABLE_JOB_TYPE, payload: {} },
    });
    const jobId = created.json<JobResponse>().id;

    const cancelled = await app.inject({ method: 'POST', url: `/api/jobs/${jobId}/cancel` });

    expect(cancelled.statusCode).toBe(200);
    expect(cancelled.json<JobResponse>().status).toBe('CANCELLED');
  }, 30_000);

  it('cooperatively cancels a RUNNING job: cancel is requested, the handler observes it, and the job reaches CANCELLED', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: TICKING_JOB_TYPE, payload: {} },
    });
    const jobId = created.json<JobResponse>().id;

    const running = await pollUntil(app, jobId, (job) => job.status === 'RUNNING', 60_000);
    expect(running.status).toBe('RUNNING');

    const cancelResponse = await app.inject({ method: 'POST', url: `/api/jobs/${jobId}/cancel` });
    expect(cancelResponse.statusCode).toBe(200);
    expect(cancelResponse.json<JobResponse>().cancelRequested).toBe(true);
    expect(cancelResponse.json<JobResponse>().status).toBe('RUNNING');

    const final = await pollUntil(app, jobId, (job) => job.status === 'CANCELLED', 60_000);
    expect(final.status).toBe('CANCELLED');
  }, 60_000);

  it('manually retries a job from DEAD_LETTERED and reaches SUCCEEDED once the handler is fixed', async () => {
    toggleableHandler.shouldFail = true;

    const created = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: TOGGLEABLE_JOB_TYPE, payload: {} },
    });
    const jobId = created.json<JobResponse>().id;

    const deadLettered = await pollUntil(app, jobId, (job) => job.status === 'DEAD_LETTERED', 60_000);
    expect(deadLettered.status).toBe('DEAD_LETTERED');

    // "Now-passing handler" — same job type, but it would no longer fail.
    toggleableHandler.shouldFail = false;

    const retried = await app.inject({ method: 'POST', url: `/api/jobs/${jobId}/retry` });
    expect(retried.statusCode).toBe(200);
    expect(retried.json<JobResponse>().status).toBe('QUEUED');

    const succeeded = await pollUntil(app, jobId, (job) => job.status === 'SUCCEEDED', 60_000);
    expect(succeeded.status).toBe('SUCCEEDED');
    expect(succeeded.result).toMatchObject({ done: true });
  }, 180_000);
});
