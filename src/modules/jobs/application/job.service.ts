import { randomUUID } from 'node:crypto';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import type { NewOutboxEvent } from '@/outbox/outbox.types';

import { DuplicateJobSubmissionError } from '../domain/errors';
import { Job } from '../domain/job.entity';
import type { JobRepository } from '../domain/job-repository.port';
import type { JobStatus } from '../domain/job-status';

const JOBS_REQUESTED_TOPIC = 'jobs.requested';

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
   * Job row and JobRequested outbox event are inserted in one transaction
   * (JobRepository.createWithOutboxEvent) — a crash right after this call
   * can't lose the event the way a direct producer.send could. The job
   * stays PENDING until the outbox publisher actually confirms delivery
   * and transitions it to QUEUED; that's the publisher's job, not this
   * one's.
   */
  async createJob(input: CreateJobInput): Promise<Job> {
    const job = Job.createNew({
      type: input.type,
      payload: input.payload,
      correlationId: randomUUID(),
      idempotencyKey: input.idempotencyKey ?? null,
    });

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId: job.props.id,
      correlationId: job.props.correlationId,
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      payload: { type: job.props.type, payload: job.props.payload, maxAttempts: job.props.maxAttempts },
    });

    const outboxEvent: NewOutboxEvent = {
      aggregateId: job.props.id,
      eventType: JOB_REQUESTED_EVENT_TYPE,
      topic: JOBS_REQUESTED_TOPIC,
      messageKey: job.props.id,
      payload: requested,
    };

    try {
      return await this.jobs.createWithOutboxEvent(job, outboxEvent);
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
