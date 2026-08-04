import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { JobService } from '@/modules/jobs/application/job.service';

import { InMemoryJobRepository } from '../jobs/fakes/in-memory-job.repository';

function buildTestApp() {
  const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' });
  const jobService = new JobService(new InMemoryJobRepository());

  return buildApp({ config, jobService });
}

describe('GET /metrics', () => {
  it('exposes the core prom-client metrics in Prometheus text format', async () => {
    const app = buildTestApp();

    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.body).toContain('# HELP job_duration_seconds');
    expect(response.body).toContain('# HELP jobs_total');
    expect(response.body).toContain('# HELP retry_total');
    expect(response.body).toContain('# HELP dead_letter_total');
    expect(response.body).toContain('# HELP outbox_backlog');
  });
});
