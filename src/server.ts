import 'dotenv/config';

import type { Consumer } from 'kafkajs';

import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { prisma } from '@/db/prisma';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer } from '@/messaging/producer';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import {
  GENERATE_REPORT_JOB_TYPE,
  GenerateReportHandler,
} from '@/modules/execution/domain/handlers/generate-report.handler';
import { startJobsRequestedConsumer } from '@/modules/execution/infrastructure/jobs-requested.consumer';
import { startRetryRouter } from '@/modules/execution/infrastructure/retry-router';
import { startRetryTierConsumer } from '@/modules/execution/infrastructure/retry-tier.consumer';
import { JobService } from '@/modules/jobs/application/job.service';
import { startDeadLetterConsumer } from '@/modules/jobs/infrastructure/dead-letter.consumer';
import { createJobOutboxPublishedHandler } from '@/modules/jobs/infrastructure/job-outbox-published.handler';
import { startJobStatusConsumer } from '@/modules/jobs/infrastructure/job-status.consumer';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';
import { TimeoutWatchdog } from '@/modules/jobs/infrastructure/timeout-watchdog';
import { OutboxPublisher } from '@/outbox/outbox-publisher';

const config = loadConfig();
const jobRepository = new PrismaJobRepository(prisma);
const jobService = new JobService(jobRepository);

const app = buildApp({ config, jobService });

const kafka = createKafkaClient({ brokers: config.kafkaBrokers, clientId: config.kafkaClientId });
const producer = createProducer(kafka);
const executeJobService = new ExecuteJobService(new Map([[GENERATE_REPORT_JOB_TYPE, new GenerateReportHandler()]]));

// Bridges execution's cancellation checks to the jobs module's own
// repository without execution ever importing jobs' domain/infrastructure
// directly — the composition root is allowed to reach across modules, the
// modules themselves are not. Covers both cancellation paths: a job
// cancelled immediately from PENDING/QUEUED is already status CANCELLED by
// the time this is checked, while a RUNNING job stays RUNNING with only
// cancelRequested set — a handler's cooperative mid-execution check needs
// this to catch that second case too, not just the first.
async function isJobCancelled(jobId: string): Promise<boolean> {
  const job = await jobRepository.findById(jobId);

  return job !== null && (job.props.status === 'CANCELLED' || job.props.cancelRequested);
}

const outboxPublisher = new OutboxPublisher({
  prisma,
  producer,
  onPublished: createJobOutboxPublishedHandler(jobRepository),
});

const timeoutWatchdog = new TimeoutWatchdog({
  jobRepository,
  producer,
  timeoutMs: config.jobTimeoutMs,
  pollIntervalMs: config.timeoutWatchdogPollIntervalMs,
});

let jobsRequestedConsumer: Consumer | undefined;
let jobStatusConsumer: Consumer | undefined;
let retryRouter: Consumer | undefined;
let retryTier1Consumer: Consumer | undefined;
let retryTier2Consumer: Consumer | undefined;
let retryTier3Consumer: Consumer | undefined;
let deadLetterConsumer: Consumer | undefined;

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  app.log.info({ signal }, 'shutting down');

  try {
    timeoutWatchdog.stop();
    outboxPublisher.stop();
    await jobsRequestedConsumer?.disconnect();
    await retryTier1Consumer?.disconnect();
    await retryTier2Consumer?.disconnect();
    await retryTier3Consumer?.disconnect();
    await retryRouter?.disconnect();
    await deadLetterConsumer?.disconnect();
    await jobStatusConsumer?.disconnect();
    await producer.disconnect();
    await app.close();
    process.exit(0);
  } catch (error) {
    app.log.error({ error }, 'error during shutdown');
    process.exit(1);
  }
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

async function start(): Promise<void> {
  await producer.connect();
  jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma, isJobCancelled });
  retryTier1Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 1, isJobCancelled });
  retryTier2Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 2, isJobCancelled });
  retryTier3Consumer = await startRetryTierConsumer({ kafka, producer, executeJobService, prisma, tier: 3, isJobCancelled });
  retryRouter = await startRetryRouter({ kafka, producer, prisma, retryTierDelaysMs: config.retryTierDelaysMs });
  deadLetterConsumer = await startDeadLetterConsumer({ kafka, jobRepository, prisma });
  jobStatusConsumer = await startJobStatusConsumer({ kafka, jobRepository, prisma });
  outboxPublisher.start();
  timeoutWatchdog.start();

  const address = await app.listen({ port: app.config.port, host: '0.0.0.0' });
  app.log.info({ address }, 'server listening');
}

start().catch((error: unknown) => {
  app.log.error({ error }, 'failed to start server');
  process.exit(1);
});
