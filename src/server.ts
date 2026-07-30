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
import { JobService } from '@/modules/jobs/application/job.service';
import { createJobOutboxPublishedHandler } from '@/modules/jobs/infrastructure/job-outbox-published.handler';
import { startJobStatusConsumer } from '@/modules/jobs/infrastructure/job-status.consumer';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';
import { OutboxPublisher } from '@/outbox/outbox-publisher';

const config = loadConfig();
const jobRepository = new PrismaJobRepository(prisma);
const jobService = new JobService(jobRepository);

const app = buildApp({ config, jobService });

const kafka = createKafkaClient({ brokers: config.kafkaBrokers, clientId: config.kafkaClientId });
const producer = createProducer(kafka);
const executeJobService = new ExecuteJobService(new Map([[GENERATE_REPORT_JOB_TYPE, new GenerateReportHandler()]]));

const outboxPublisher = new OutboxPublisher({
  prisma,
  producer,
  onPublished: createJobOutboxPublishedHandler(jobRepository),
});

let jobsRequestedConsumer: Consumer | undefined;
let jobStatusConsumer: Consumer | undefined;

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  app.log.info({ signal }, 'shutting down');

  try {
    outboxPublisher.stop();
    await jobsRequestedConsumer?.disconnect();
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
  jobsRequestedConsumer = await startJobsRequestedConsumer({ kafka, producer, executeJobService, prisma });
  jobStatusConsumer = await startJobStatusConsumer({ kafka, jobRepository, prisma });
  outboxPublisher.start();

  const address = await app.listen({ port: app.config.port, host: '0.0.0.0' });
  app.log.info({ address }, 'server listening');
}

start().catch((error: unknown) => {
  app.log.error({ error }, 'failed to start server');
  process.exit(1);
});
