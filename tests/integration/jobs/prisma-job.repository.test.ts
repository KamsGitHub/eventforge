import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { JobVersionConflictError } from '@/modules/jobs/domain/errors';
import { Job } from '@/modules/jobs/domain/job.entity';
import { PrismaJobRepository } from '@/modules/jobs/infrastructure/prisma-job.repository';

import { sharedDatabaseUrl } from '../setup';

describe('PrismaJobRepository (real Postgres via Testcontainers)', () => {
  let prisma: PrismaClient;
  let repository: PrismaJobRepository;

  beforeAll(() => {
    prisma = createPrismaClient(sharedDatabaseUrl());
    repository = new PrismaJobRepository(prisma);
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  afterEach(async () => {
    await prisma.job.deleteMany();
    await prisma.outboxEvent.deleteMany();
  });

  it('creates a job and finds it by id', async () => {
    const job = Job.createNew({ type: 'GENERATE_REPORT', payload: { reportId: 1 }, correlationId: 'corr-1' });

    const created = await repository.create(job);
    const found = await repository.findById(job.props.id);

    expect(created.props.id).toBe(job.props.id);
    expect(found).not.toBeNull();
    expect(found?.props.status).toBe('PENDING');
    expect(found?.props.payload).toEqual({ reportId: 1 });
    expect(found?.props.correlationId).toBe('corr-1');
    expect(found?.props.version).toBe(0);
  });

  it('returns null for an unknown id', async () => {
    const found = await repository.findById('00000000-0000-0000-0000-000000000000');

    expect(found).toBeNull();
  });

  it('persists a state transition and increments the version', async () => {
    const job = Job.createNew({ type: 'GENERATE_REPORT', payload: {}, correlationId: 'corr-1' });
    await repository.create(job);

    const queued = job.transitionTo('QUEUED');
    const updated = await repository.update(queued);

    expect(updated.props.status).toBe('QUEUED');
    expect(updated.props.version).toBe(1);

    const reloaded = await repository.findById(job.props.id);

    expect(reloaded?.props.status).toBe('QUEUED');
    expect(reloaded?.props.version).toBe(1);
  });

  it('rejects a stale-version update (optimistic concurrency)', async () => {
    const job = Job.createNew({ type: 'GENERATE_REPORT', payload: {}, correlationId: 'corr-1' });
    await repository.create(job);

    // Two independent "readers" load the same version-0 snapshot.
    const readerA = await repository.findById(job.props.id);
    const readerB = await repository.findById(job.props.id);
    if (!readerA || !readerB) throw new Error('expected job to exist');

    // Reader A writes first — succeeds and bumps the version to 1.
    await repository.update(readerA.transitionTo('QUEUED'));

    // Reader B still holds the stale version-0 snapshot; its write must be rejected.
    await expect(repository.update(readerB.transitionTo('CANCELLED'))).rejects.toBeInstanceOf(JobVersionConflictError);

    // The row reflects reader A's write, not reader B's rejected one.
    const final = await repository.findById(job.props.id);

    expect(final?.props.status).toBe('QUEUED');
    expect(final?.props.version).toBe(1);
  });

  it('enforces the idempotencyKey unique constraint at the database level', async () => {
    const first = Job.createNew({
      type: 'GENERATE_REPORT',
      payload: {},
      correlationId: 'corr-1',
      idempotencyKey: 'idem-1',
    });
    const second = Job.createNew({
      type: 'GENERATE_REPORT',
      payload: {},
      correlationId: 'corr-2',
      idempotencyKey: 'idem-1',
    });

    await repository.create(first);

    await expect(repository.create(second)).rejects.toThrow();
  });

  describe('createWithOutboxEvent', () => {
    it('inserts the job row and the outbox event in one transaction', async () => {
      const job = Job.createNew({ type: 'GENERATE_REPORT', payload: { reportId: 1 }, correlationId: 'corr-1' });

      const created = await repository.createWithOutboxEvent(job, {
        aggregateId: job.props.id,
        eventType: 'JobRequested',
        topic: 'jobs.requested',
        messageKey: job.props.id,
        payload: { hello: 'world' },
      });

      expect(created.props.status).toBe('PENDING');

      const outboxRows = await prisma.outboxEvent.findMany({ where: { aggregateId: job.props.id } });
      expect(outboxRows).toHaveLength(1);
      expect(outboxRows[0]?.topic).toBe('jobs.requested');
      expect(outboxRows[0]?.eventType).toBe('JobRequested');
      expect(outboxRows[0]?.publishedAt).toBeNull();
      expect(outboxRows[0]?.payload).toEqual({ hello: 'world' });
    });

    it('rolls back the outbox insert when the job insert fails (idempotencyKey conflict)', async () => {
      const first = Job.createNew({
        type: 'GENERATE_REPORT',
        payload: {},
        correlationId: 'corr-1',
        idempotencyKey: 'idem-outbox-1',
      });
      await repository.create(first);

      const second = Job.createNew({
        type: 'GENERATE_REPORT',
        payload: {},
        correlationId: 'corr-2',
        idempotencyKey: 'idem-outbox-1',
      });

      await expect(
        repository.createWithOutboxEvent(second, {
          aggregateId: second.props.id,
          eventType: 'JobRequested',
          topic: 'jobs.requested',
          messageKey: second.props.id,
          payload: {},
        }),
      ).rejects.toThrow();

      const outboxRows = await prisma.outboxEvent.findMany({ where: { aggregateId: second.props.id } });
      expect(outboxRows).toHaveLength(0);
    });
  });
});
