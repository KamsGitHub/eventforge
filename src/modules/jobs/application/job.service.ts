import { randomUUID } from 'node:crypto';

import { DuplicateJobSubmissionError } from '../domain/errors';
import { Job } from '../domain/job.entity';
import type { JobRepository } from '../domain/job-repository.port';
import type { JobStatus } from '../domain/job-status';

export interface CreateJobInput {
  readonly type: string;
  readonly payload: unknown;
  readonly idempotencyKey?: string | undefined;
}

export interface ListJobsInput {
  readonly status?: JobStatus | undefined;
  readonly limit?: number | undefined;
  readonly offset?: number | undefined;
}

const DEFAULT_LIST_LIMIT = 20;
const MAX_LIST_LIMIT = 100;

export class JobService {
  constructor(private readonly jobs: JobRepository) {}

  /**
   * Kafka isn't wired up until Milestone 9 (transactional outbox); jobs
   * created here are persisted and stay PENDING — nothing consumes them
   * yet. That's expected, not a bug.
   */
  async createJob(input: CreateJobInput): Promise<Job> {
    const job = Job.createNew({
      type: input.type,
      payload: input.payload,
      correlationId: randomUUID(),
      idempotencyKey: input.idempotencyKey ?? null,
    });

    try {
      return await this.jobs.create(job);
    } catch (error) {
      if (error instanceof DuplicateJobSubmissionError) {
        const existing = await this.jobs.findByIdempotencyKey(error.idempotencyKey);

        if (existing) {
          return existing;
        }
      }

      throw error;
    }
  }

  async getJob(id: string): Promise<Job | null> {
    return this.jobs.findById(id);
  }

  async listJobs(input: ListJobsInput = {}): Promise<Job[]> {
    const limit = Math.min(input.limit ?? DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT);
    const offset = input.offset ?? 0;

    return this.jobs.list({ status: input.status, limit, offset });
  }
}
