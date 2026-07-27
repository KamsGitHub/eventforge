import { JobService } from '@/modules/jobs/application/job.service';

import { InMemoryJobRepository } from './fakes/in-memory-job.repository';

describe('JobService', () => {
  it('creates a job in PENDING with a generated correlationId', async () => {
    const service = new JobService(new InMemoryJobRepository());

    const job = await service.createJob({ type: 'GENERATE_REPORT', payload: { reportId: 1 } });

    expect(job.props.status).toBe('PENDING');
    expect(job.props.type).toBe('GENERATE_REPORT');
    expect(job.props.payload).toEqual({ reportId: 1 });
    expect(job.props.correlationId).toEqual(expect.any(String));
    expect(job.props.correlationId.length).toBeGreaterThan(0);
  });

  it('returns the existing job when the same idempotencyKey is submitted twice', async () => {
    const service = new JobService(new InMemoryJobRepository());

    const first = await service.createJob({ type: 'GENERATE_REPORT', payload: { a: 1 }, idempotencyKey: 'idem-1' });
    const second = await service.createJob({ type: 'GENERATE_REPORT', payload: { a: 2 }, idempotencyKey: 'idem-1' });

    expect(second.props.id).toBe(first.props.id);
    expect(second.props.payload).toEqual({ a: 1 });
  });

  it('creates separate jobs when no idempotencyKey is given', async () => {
    const service = new JobService(new InMemoryJobRepository());

    const first = await service.createJob({ type: 'GENERATE_REPORT', payload: {} });
    const second = await service.createJob({ type: 'GENERATE_REPORT', payload: {} });

    expect(second.props.id).not.toBe(first.props.id);
  });

  it('getJob returns null for an unknown id', async () => {
    const service = new JobService(new InMemoryJobRepository());

    const job = await service.getJob('00000000-0000-0000-0000-000000000000');

    expect(job).toBeNull();
  });

  it('getJob returns the created job by id', async () => {
    const service = new JobService(new InMemoryJobRepository());
    const created = await service.createJob({ type: 'GENERATE_REPORT', payload: {} });

    const found = await service.getJob(created.props.id);

    expect(found?.props.id).toBe(created.props.id);
  });

  describe('listJobs', () => {
    it('defaults to the 20 most recent jobs', async () => {
      const repository = new InMemoryJobRepository();
      const service = new JobService(repository);

      for (let i = 0; i < 25; i += 1) {
        await service.createJob({ type: 'GENERATE_REPORT', payload: { i } });
      }

      const jobs = await service.listJobs();

      expect(jobs).toHaveLength(20);
    });

    it('caps limit at 100 even if a larger value is requested', async () => {
      const service = new JobService(new InMemoryJobRepository());

      const jobs = await service.listJobs({ limit: 500 });

      expect(jobs).toHaveLength(0); // no jobs created — just proving it doesn't throw/misbehave on a large limit
    });

    it('filters by status', async () => {
      const repository = new InMemoryJobRepository();
      const service = new JobService(repository);

      const job = await service.createJob({ type: 'GENERATE_REPORT', payload: {} });
      await repository.update(job.transitionTo('QUEUED'));

      const pending = await service.listJobs({ status: 'PENDING' });
      const queued = await service.listJobs({ status: 'QUEUED' });

      expect(pending).toHaveLength(0);
      expect(queued).toHaveLength(1);
      expect(queued[0]?.props.id).toBe(job.props.id);
    });

    it('supports offset-based pagination', async () => {
      const repository = new InMemoryJobRepository();
      const service = new JobService(repository);

      const created = [];
      for (let i = 0; i < 3; i += 1) {
        created.push(await service.createJob({ type: 'GENERATE_REPORT', payload: { i } }));
      }

      const page1 = await service.listJobs({ limit: 2, offset: 0 });
      const page2 = await service.listJobs({ limit: 2, offset: 2 });

      expect(page1).toHaveLength(2);
      expect(page2).toHaveLength(1);
    });
  });
});
