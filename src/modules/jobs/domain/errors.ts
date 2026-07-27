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
