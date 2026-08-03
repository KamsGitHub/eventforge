import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { JobService } from '@/modules/jobs/application/job.service';
import type { JobResponse } from '@/modules/jobs/api/schemas';

import { createCapturingLogger } from '../helpers/capturing-logger';
import { InMemoryJobRepository } from './fakes/in-memory-job.repository';

describe('correlationId propagation (Milestone 13)', () => {
  it('carries one correlationId from the HTTP access log into the outbox row it creates', async () => {
    const config = loadConfig({ LOG_LEVEL: 'info', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' });
    const repository = new InMemoryJobRepository();
    const jobService = new JobService(repository);
    const { logger, lines } = createCapturingLogger();
    const app = buildApp({ config, jobService, logger });

    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: 'GENERATE_REPORT', payload: { reportId: 1 } },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json<JobResponse>();
    const correlationId = body.correlationId;

    // The outbox row (what the outbox publisher would ship to Kafka)
    // carries the same correlationId the HTTP request generated.
    expect(repository.outboxEvents).toHaveLength(1);
    const outboxPayload = repository.outboxEvents[0]?.payload as { correlationId?: string };
    expect(outboxPayload.correlationId).toBe(correlationId);

    // Fastify's own "request completed" access log line carries the
    // correlationId as reqId — genReqId (src/shared/correlation-plugin.ts)
    // makes Fastify's request id *be* the correlationId, so every log line
    // for the request (including Fastify's built-in ones) has it for free.
    const accessLogLine = lines().find((line) => line['reqId'] === correlationId && line['msg'] === 'request completed');
    expect(accessLogLine).toBeDefined();

    await app.close();
  });

  it('reuses an inbound x-correlation-id header instead of generating a new one', async () => {
    const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' });
    const repository = new InMemoryJobRepository();
    const jobService = new JobService(repository);
    const app = buildApp({ config, jobService });

    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      headers: { 'x-correlation-id': 'caller-supplied-id' },
      payload: { type: 'GENERATE_REPORT', payload: {} },
    });

    const body = response.json<JobResponse>();
    expect(body.correlationId).toBe('caller-supplied-id');

    await app.close();
  });
});
