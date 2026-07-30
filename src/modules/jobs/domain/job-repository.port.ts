// NewOutboxEvent lives under src/outbox (cross-cutting technical code, not a
// module) rather than in domain — importing it here is the same kind of
// dependency as importing a shared type, not a module-boundary violation.
import type { NewOutboxEvent } from '@/outbox/outbox.types';

// Same reasoning as the NewOutboxEvent import above: Prisma/PrismaClient
// live under src/db (cross-cutting), not a module. findById/update accept
// an optional transaction client so the idempotent-consumer wrapper
// (src/messaging) can make a Job read/write commit atomically with its own
// ProcessedEvent insert — without this, a crash between the two could mark
// an event processed whose job change never actually landed.
import type { Prisma, PrismaClient } from '@/db/prisma-client';

import type { Job } from './job.entity';
import type { JobStatus } from './job-status';

/** A plain client for a one-off call, or a transaction client to join the caller's own transaction. */
export type JobRepositoryClient = PrismaClient | Prisma.TransactionClient;

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

  findById(id: string, client?: JobRepositoryClient): Promise<Job | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<Job | null>;
  list(filter: JobListFilter): Promise<Job[]>;

  /**
   * Persists `job`, enforcing optimistic concurrency against the version the
   * job was loaded with. Throws JobVersionConflictError if another writer
   * has updated the row since this job was read. Pass a transaction client
   * to make this commit atomically with other writes in that transaction.
   */
  update(job: Job, client?: JobRepositoryClient): Promise<Job>;
}
