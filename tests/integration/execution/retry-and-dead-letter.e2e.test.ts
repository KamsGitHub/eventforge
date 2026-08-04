import { randomUUID } from 'node:crypto';

import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';

import { JOB_REQUESTED_EVENT_TYPE } from '@/contracts/events/job-requested.event';
import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import type { JobHandler } from '@/modules/execution/domain/job-handler.port';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';
import { startRetryRouter } from '@/modules/execution/infrastructure/retry-router';
import { startRetryTierConsumer } from '@/modules/execution/infrastructure/retry-tier.consumer';
import { JobService } from '@/modules/jobs/application/job.service';
import { Job } from '@/modules/jobs/domain/job.entity';
import { startDeadLetterConsumer } from '@/modules/jobs/infrastructure/dead-letter.consumer';
import { createJobOutboxPublishedHandler } from '@/modules/jobs/infrastructure/job-outbox-published.handler';
import { startJobStatusConsumer } from '@/modules/jobs/infrastructure/job-status.consumer';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';
import { TimeoutWatchdog } from '@/modules/jobs/infrastructure/timeout-watchdog';
import { OutboxPublisher } from '@/outbox/outbox-publisher';

import { sharedDatabaseUrl, sharedKafkaBrokers } from '../setup';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';
const JOBS_RETRY_1_TOPIC = 'jobs.retry-1';
const JOBS_RETRY_2_TOPIC = 'jobs.retry-2';
const JOBS_DEAD_LETTER_TOPIC = 'jobs.dead-letter';

const ALWAYS_FAIL_JOB_TYPE = 'ALWAYS_FAIL';

class AlwaysFailHandler implements JobHandler {
  execute(): Promise<unknown> {
    return Promise.reject(new Error('handler always fails'));
  }
}

async function pollUntil<T>(check: () => Promise<T | null>, timeoutMs: number, intervalMs = 100): Promise<T> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const result = await check();

    if (result !== null) {
      return result;
    }

    if (Date.now() > deadline) {
      throw new Error('timed out waiting for condition');
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

describe('retry tiers, dead-letter routing, and timeout watchdog (real Kafka + Postgres via Testcontainers)', () => {
  let prisma: PrismaClient;
  let kafka: Kafka;
  let producer: Producer;
  let jobRepository: PrismaJobRepository;
  let jobService: JobService;

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
    jobService = new JobService(jobRepository);

    const executeJobService = new ExecuteJobService(new Map([[ALWAYS_FAIL_JOB_TYPE, new AlwaysFailHandler()]]));

    // Short, distinct delays per tier so the progression test finishes fast
    // while still exercising all three tiers' machinery.
    const retryTierDelaysMs = { 1: 200, 2: 300, 3: 400 };

    jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma });
    retryTier1Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 1 });
    retryTier2Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 2 });
    retryTier3Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 3 });
    retryRouter = await startRetryRouter({ kafka, producer, prisma, retryTierDelaysMs });
    deadLetterConsumer = await startDeadLetterConsumer({ kafka, jobRepository, prisma });
    jobStatusConsumer = await startJobStatusConsumer({ kafka, jobRepository, prisma });

    // JobService.createJob() only inserts a Job row + an OutboxEvent row
    // (Milestone 9) — nothing publishes JobRequested to Kafka until this
    // publisher polls and sends it. Without it every job created below
    // would sit PENDING forever and every consumer above would starve.
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

  /**
   * Resolves on the first message matching `predicate` — plain "first
   * message wins" would pick up a stale message from an earlier `it` block
   * sharing the same topic (e.g. jobs.dead-letter gets a legitimate
   * exhaustion message in one test and a poison-message one in another).
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

  it('routes a job through both retry tiers and lands it in DEAD_LETTERED once maxAttempts is exhausted', async () => {
    const jobIdRef: { current: string | undefined } = { current: undefined };
    const matchesThisJob = (body: unknown): boolean => (body as { aggregateId?: string }).aggregateId === jobIdRef.current;

    const retry1 = await consumeMatching(JOBS_RETRY_1_TOPIC, `test.retry-1-observer.${randomUUID()}`, matchesThisJob);
    const retry2 = await consumeMatching(JOBS_RETRY_2_TOPIC, `test.retry-2-observer.${randomUUID()}`, matchesThisJob);

    // Job.createNew defaults maxAttempts to 3: attempt 1 fails -> tier 1,
    // attempt 2 fails -> tier 2, attempt 3 fails -> exhausted -> dead-letter.
    const job = await jobService.createJob({ type: ALWAYS_FAIL_JOB_TYPE, payload: {} });
    jobIdRef.current = job.props.id;

    const [retry1Message, retry2Message] = await Promise.all([retry1.received, retry2.received]);
    expect(JSON.parse(retry1Message.message.value?.toString() ?? '{}')).toMatchObject({ payload: { attempt: 2 } });
    expect(JSON.parse(retry2Message.message.value?.toString() ?? '{}')).toMatchObject({ payload: { attempt: 3 } });

    await retry1.consumer.disconnect();
    await retry2.consumer.disconnect();

    const final = await pollUntil(async () => {
      const current = await jobRepository.findById(job.props.id);
      return current && current.props.status === 'DEAD_LETTERED' ? current : null;
    }, 60_000);

    expect(final.props.status).toBe('DEAD_LETTERED');
    expect(final.props.attempt).toBe(3);
    expect(final.props.error).toEqual(expect.any(String));
  }, 120_000);

  it('routes a malformed jobs.requested message straight to dead-letter without blocking the partition', async () => {
    const malformedKey = `malformed-${randomUUID()}`;
    const deadLetter = await consumeMatching(
      JOBS_DEAD_LETTER_TOPIC,
      `test.dlq-observer.${randomUUID()}`,
      (body) => (body as { aggregateId?: string }).aggregateId === malformedKey,
    );

    // Missing maxAttempts/attempt/failureHistory entirely — fails
    // jobRequestedEventSchemaV1 validation.
    await publish(producer, {
      topic: JOBS_REQUESTED_TOPIC,
      key: malformedKey,
      value: { eventId: randomUUID(), eventType: JOB_REQUESTED_EVENT_TYPE, aggregateId: malformedKey, payload: { oops: true } },
    });

    const dlqMessage = await deadLetter.received;
    const parsed = JSON.parse(dlqMessage.message.value?.toString() ?? '{}') as { payload: { error: string } };
    expect(parsed.payload.error).toMatch(/malformed message/);
    await deadLetter.consumer.disconnect();

    // Prove the partition wasn't blocked: a well-formed message right after
    // the poison one still completes normally.
    const job = await jobService.createJob({ type: ALWAYS_FAIL_JOB_TYPE, payload: {} });
    const stillWorking = await pollUntil(async () => {
      const current = await jobRepository.findById(job.props.id);
      return current && current.props.status !== 'PENDING' && current.props.status !== 'QUEUED' ? current : null;
    }, 60_000);

    expect(['RUNNING', 'FAILED']).toContain(stillWorking.props.status);
  }, 120_000);

  it('publishes a JobFailed for a RUNNING job past its deadline, which then enters the retry pipeline', async () => {
    const stuckJob = Job.createNew({
      type: ALWAYS_FAIL_JOB_TYPE,
      payload: {},
      correlationId: randomUUID(),
    })
      .transitionTo('QUEUED')
      .transitionTo('RUNNING');

    // Backdate startedAt well past the watchdog's timeout so pollOnce()
    // detects it immediately instead of waiting on the real clock.
    const backdated = Job.fromProps({ ...stuckJob.props, startedAt: new Date(Date.now() - 10_000) });
    await jobRepository.create(backdated);

    const watchdog = new TimeoutWatchdog({ jobRepository, producer, timeoutMs: 2_000 });
    const publishedCount = await watchdog.pollOnce();
    expect(publishedCount).toBe(1);

    const requeued = await pollUntil(async () => {
      const current = await jobRepository.findById(backdated.props.id);
      return current && current.props.status === 'QUEUED' ? current : null;
    }, 60_000);

    expect(requeued.props.status).toBe('QUEUED');
    expect(requeued.props.error).toBeNull();
  }, 120_000);
});
