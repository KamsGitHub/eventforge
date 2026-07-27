import { IllegalJobStateTransitionError } from '@/modules/jobs/domain/errors';
import { Job, type JobProps, type JobStatus } from '@/modules/jobs/domain/job.entity';

const ALL_STATUSES: readonly JobStatus[] = [
  'PENDING',
  'QUEUED',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'DEAD_LETTERED',
  'CANCELLED',
];

// Independently authored expectation table (not imported from the entity)
// so this test actually proves the state machine's behavior instead of
// checking the implementation's own map against itself.
const ALLOWED: Readonly<Record<JobStatus, readonly JobStatus[]>> = {
  PENDING: ['QUEUED', 'CANCELLED'],
  QUEUED: ['RUNNING', 'CANCELLED'],
  RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED'],
  SUCCEEDED: [],
  FAILED: ['QUEUED', 'DEAD_LETTERED'],
  DEAD_LETTERED: ['QUEUED'],
  CANCELLED: [],
};

function buildJob(status: JobStatus, overrides: Partial<JobProps> = {}): Job {
  const now = new Date('2026-01-01T00:00:00.000Z');

  return Job.fromProps({
    id: 'job-1',
    type: 'GENERATE_REPORT',
    status,
    payload: { foo: 'bar' },
    result: null,
    error: null,
    attempt: 0,
    maxAttempts: 3,
    idempotencyKey: null,
    correlationId: 'corr-1',
    version: 0,
    createdAt: now,
    updatedAt: now,
    startedAt: null,
    completedAt: null,
    ...overrides,
  });
}

describe('Job entity', () => {
  describe('createNew', () => {
    it('creates a job in PENDING with sensible defaults', () => {
      const job = Job.createNew({ type: 'GENERATE_REPORT', payload: { a: 1 }, correlationId: 'corr-1' });

      expect(job.props.status).toBe('PENDING');
      expect(job.props.attempt).toBe(0);
      expect(job.props.maxAttempts).toBe(3);
      expect(job.props.version).toBe(0);
      expect(job.props.idempotencyKey).toBeNull();
      expect(job.props.result).toBeNull();
      expect(job.props.error).toBeNull();
      expect(job.props.id).toEqual(expect.any(String));
    });

    it('honors an explicit idempotencyKey and maxAttempts', () => {
      const job = Job.createNew({
        type: 'GENERATE_REPORT',
        payload: {},
        correlationId: 'corr-1',
        idempotencyKey: 'idem-1',
        maxAttempts: 5,
      });

      expect(job.props.idempotencyKey).toBe('idem-1');
      expect(job.props.maxAttempts).toBe(5);
    });
  });

  describe('transitionTo — full state matrix', () => {
    for (const from of ALL_STATUSES) {
      for (const to of ALL_STATUSES) {
        const isLegal = ALLOWED[from].includes(to);

        if (isLegal) {
          it(`allows ${from} -> ${to}`, () => {
            const job = buildJob(from);

            const next = job.transitionTo(to);

            expect(next.props.status).toBe(to);
          });
        } else {
          it(`rejects ${from} -> ${to}`, () => {
            const job = buildJob(from);

            expect(() => job.transitionTo(to)).toThrow(IllegalJobStateTransitionError);
          });
        }
      }
    }
  });

  describe('transitionTo — side effects', () => {
    it('sets startedAt and increments attempt on transition to RUNNING', () => {
      const job = buildJob('QUEUED', { attempt: 0, startedAt: null });

      const running = job.transitionTo('RUNNING');

      expect(running.props.attempt).toBe(1);
      expect(running.props.startedAt).toBeInstanceOf(Date);
    });

    it('sets completedAt and stores the result on transition to SUCCEEDED', () => {
      const job = buildJob('RUNNING');

      const succeeded = job.transitionTo('SUCCEEDED', { result: { ok: true } });

      expect(succeeded.props.completedAt).toBeInstanceOf(Date);
      expect(succeeded.props.result).toEqual({ ok: true });
    });

    it('sets error on transition to FAILED without marking it completed', () => {
      const job = buildJob('RUNNING');

      const failed = job.transitionTo('FAILED', { error: 'boom' });

      expect(failed.props.error).toBe('boom');
      expect(failed.props.completedAt).toBeNull();
    });

    it('sets completedAt on transition to DEAD_LETTERED', () => {
      const job = buildJob('FAILED', { error: 'boom' });

      const dead = job.transitionTo('DEAD_LETTERED');

      expect(dead.props.completedAt).toBeInstanceOf(Date);
      expect(dead.props.error).toBe('boom');
    });

    it('clears error and completedAt when retried back to QUEUED from FAILED', () => {
      const job = buildJob('FAILED', { error: 'boom', completedAt: new Date() });

      const requeued = job.transitionTo('QUEUED');

      expect(requeued.props.error).toBeNull();
      expect(requeued.props.completedAt).toBeNull();
    });

    it('clears completedAt when retried back to QUEUED from DEAD_LETTERED', () => {
      const job = buildJob('DEAD_LETTERED', { error: 'boom', completedAt: new Date() });

      const requeued = job.transitionTo('QUEUED');

      expect(requeued.props.error).toBeNull();
      expect(requeued.props.completedAt).toBeNull();
    });

    it('sets completedAt on transition to CANCELLED', () => {
      const job = buildJob('RUNNING');

      const cancelled = job.transitionTo('CANCELLED');

      expect(cancelled.props.completedAt).toBeInstanceOf(Date);
    });

    it('does not mutate the original job (immutability)', () => {
      const job = buildJob('PENDING');

      const next = job.transitionTo('QUEUED');

      expect(job.props.status).toBe('PENDING');
      expect(next.props.status).toBe('QUEUED');
      expect(next).not.toBe(job);
    });

    it('throws IllegalJobStateTransitionError with the from/to states attached', () => {
      const job = buildJob('SUCCEEDED');

      try {
        job.transitionTo('QUEUED');
        throw new Error('expected transitionTo to throw');
      } catch (error) {
        expect(error).toBeInstanceOf(IllegalJobStateTransitionError);
        expect((error as IllegalJobStateTransitionError).from).toBe('SUCCEEDED');
        expect((error as IllegalJobStateTransitionError).to).toBe('QUEUED');
      }
    });
  });
});
