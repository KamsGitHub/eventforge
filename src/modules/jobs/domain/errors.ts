import type { JobStatus } from './job-status';

export class IllegalJobStateTransitionError extends Error {
  constructor(
    public readonly from: JobStatus,
    public readonly to: JobStatus,
  ) {
    super(`Illegal job state transition: ${from} -> ${to}`);
    this.name = 'IllegalJobStateTransitionError';
  }
}

export class JobVersionConflictError extends Error {
  constructor(
    public readonly jobId: string,
    public readonly expectedVersion: number,
  ) {
    super(`Job ${jobId} was modified concurrently (expected version ${expectedVersion})`);
    this.name = 'JobVersionConflictError';
  }
}

/**
 * Thrown when a job submission's Idempotency-Key collides with an existing
 * job. This is an HTTP-level duplicate-submission concern, distinct from
 * Kafka consumer idempotency (Milestone 10) — different mechanism,
 * different table, don't conflate them.
 */
export class DuplicateJobSubmissionError extends Error {
  constructor(public readonly idempotencyKey: string) {
    super(`A job with idempotencyKey "${idempotencyKey}" already exists`);
    this.name = 'DuplicateJobSubmissionError';
  }
}
