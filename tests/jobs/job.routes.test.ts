import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';
import { JobService } from '@/modules/jobs/application/job.service';
import type { JobResponse, ListJobsResponseSchema } from '@/modules/jobs/api/schemas';
import type { z } from 'zod';

import { createFakeProducer } from './fakes/fake-producer';
import { InMemoryJobRepository } from './fakes/in-memory-job.repository';

type ListJobsResponse = z.infer<typeof ListJobsResponseSchema>;
type ErrorBody = { error: string };

function buildTestApp() {
  const config = loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' });
  const repository = new InMemoryJobRepository();
  const jobService = new JobService(repository, createFakeProducer().producer);
  const app = buildApp({ config, jobService });

  return { app, repository };
}

describe('POST /api/jobs', () => {
  it('creates a job and returns 201 with the job representation', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: 'GENERATE_REPORT', payload: { reportId: 42 } },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json<JobResponse>();
    expect(body.status).toBe('QUEUED');
    expect(body.type).toBe('GENERATE_REPORT');
    expect(body.payload).toEqual({ reportId: 42 });
    expect(body.id).toEqual(expect.any(String));
    expect(body.correlationId).toEqual(expect.any(String));

    await app.close();
  });

  it('returns 400 when type is missing', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { payload: {} },
    });

    expect(response.statusCode).toBe(400);

    await app.close();
  });

  it('returns 400 when payload is not an object', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { type: 'GENERATE_REPORT', payload: 'not-an-object' },
    });

    expect(response.statusCode).toBe(400);

    await app.close();
  });

  it('returns the original job (not a new row) when the same Idempotency-Key is replayed', async () => {
    const { app } = buildTestApp();

    const first = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      headers: { 'idempotency-key': 'idem-1' },
      payload: { type: 'GENERATE_REPORT', payload: { attempt: 1 } },
    });
    const second = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      headers: { 'idempotency-key': 'idem-1' },
      payload: { type: 'GENERATE_REPORT', payload: { attempt: 2 } },
    });

    expect(first.statusCode).toBe(201);
    expect(second.statusCode).toBe(201);
    expect(second.json<JobResponse>().id).toBe(first.json<JobResponse>().id);
    expect(second.json<JobResponse>().payload).toEqual({ attempt: 1 });

    await app.close();
  });

  it('creates independent jobs when no Idempotency-Key is given', async () => {
    const { app } = buildTestApp();

    const first = await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'X', payload: {} } });
    const second = await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'X', payload: {} } });

    expect(first.json<JobResponse>().id).not.toBe(second.json<JobResponse>().id);

    await app.close();
  });
});

describe('GET /api/jobs/:jobId', () => {
  it('returns the job when it exists', async () => {
    const { app } = buildTestApp();

    const created = await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'X', payload: {} } });
    const jobId = created.json<JobResponse>().id;

    const response = await app.inject({ method: 'GET', url: `/api/jobs/${jobId}` });

    expect(response.statusCode).toBe(200);
    expect(response.json<JobResponse>().id).toBe(jobId);

    await app.close();
  });

  it('returns 404 for an unknown job id', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({
      method: 'GET',
      url: '/api/jobs/00000000-0000-0000-0000-000000000000',
    });

    expect(response.statusCode).toBe(404);
    const body = response.json<ErrorBody>();
    expect(typeof body.error).toBe('string');

    await app.close();
  });

  it('returns 400 for a malformed job id', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({ method: 'GET', url: '/api/jobs/not-a-uuid' });

    expect(response.statusCode).toBe(400);

    await app.close();
  });
});

describe('GET /api/jobs', () => {
  it('lists created jobs, most recent first', async () => {
    const { app } = buildTestApp();

    await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'A', payload: {} } });
    await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'B', payload: {} } });

    const response = await app.inject({ method: 'GET', url: '/api/jobs' });

    expect(response.statusCode).toBe(200);
    expect(response.json<ListJobsResponse>().jobs).toHaveLength(2);

    await app.close();
  });

  it('respects the limit query parameter', async () => {
    const { app } = buildTestApp();

    await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'A', payload: {} } });
    await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'B', payload: {} } });

    const response = await app.inject({ method: 'GET', url: '/api/jobs?limit=1' });

    expect(response.json<ListJobsResponse>().jobs).toHaveLength(1);

    await app.close();
  });

  it('filters by status', async () => {
    const { app, repository } = buildTestApp();

    const created = await app.inject({ method: 'POST', url: '/api/jobs', payload: { type: 'A', payload: {} } });
    const job = await repository.findById(created.json<JobResponse>().id);
    if (job) {
      await repository.update(job.transitionTo('RUNNING'));
    }

    const queued = await app.inject({ method: 'GET', url: '/api/jobs?status=QUEUED' });
    const running = await app.inject({ method: 'GET', url: '/api/jobs?status=RUNNING' });

    expect(queued.json<ListJobsResponse>().jobs).toHaveLength(0);
    expect(running.json<ListJobsResponse>().jobs).toHaveLength(1);

    await app.close();
  });

  it('returns 400 for an invalid status filter', async () => {
    const { app } = buildTestApp();

    const response = await app.inject({ method: 'GET', url: '/api/jobs?status=NOT_A_STATUS' });

    expect(response.statusCode).toBe(400);

    await app.close();
  });
});
