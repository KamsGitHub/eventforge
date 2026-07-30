import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { JobService } from '@/modules/jobs/application/job.service';

import { createFakeProducer } from './jobs/fakes/fake-producer';
import { InMemoryJobRepository } from './jobs/fakes/in-memory-job.repository';

describe('GET /health', () => {
  it('returns 200 with an ok status', async () => {
    const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' });
    const jobService = new JobService(new InMemoryJobRepository(), createFakeProducer().producer);
    const app = buildApp({ config, jobService });

    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: 'ok' });

    await app.close();
  });
});
