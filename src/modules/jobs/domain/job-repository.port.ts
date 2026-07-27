import type { Job } from './job.entity';
import type { JobStatus } from './job-status';

export interface JobListFilter {
  readonly status?: JobStatus;
  readonly limit: number;
  readonly offset: number;
}

export interface JobRepository {
  create(job: Job): Promise<Job>;
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
