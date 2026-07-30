// NewOutboxEvent lives under src/outbox (cross-cutting technical code, not a
// module) rather than in domain — importing it here is the same kind of
// dependency as importing a shared type, not a module-boundary violation.
import type { NewOutboxEvent } from '@/outbox/outbox.types';

import type { Job } from './job.entity';
import type { JobStatus } from './job-status';

export interface JobListFilter {
  readonly status?: JobStatus;
  readonly limit: number;
  readonly offset: number;
}

export interface JobRepository {
  create(job: Job): Promise<Job>;

  /**
   * Inserts `job` and `outboxEvent` in a single transaction — the row and
   * its delivery guarantee either both land or neither does. Used instead
   * of `create()` whenever the caller needs at-least-once delivery of an
   * event tied to the job's creation.
   */
  createWithOutboxEvent(job: Job, outboxEvent: NewOutboxEvent): Promise<Job>;

  findById(id: string): Promise<Job | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<Job | null>;
  list(filter: JobListFilter): Promise<Job[]>;

  /**
   * Persists `job`, enforcing optimistic concurrency against the version the
   * job was loaded with. Throws JobVersionConflictError if another writer
   * has updated the row since this job was read.
   */
  update(job: Job): Promise<Job>;
}
