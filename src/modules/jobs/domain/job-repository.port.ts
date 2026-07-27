import type { Job } from './job.entity';

export interface JobRepository {
  create(job: Job): Promise<Job>;
  findById(id: string): Promise<Job | null>;

  /**
   * Persists `job`, enforcing optimistic concurrency against the version the
   * job was loaded with. Throws JobVersionConflictError if another writer
   * has updated the row since this job was read.
   */
  update(job: Job): Promise<Job>;
}
