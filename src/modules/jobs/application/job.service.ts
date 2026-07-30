import { randomUUID } from 'node:crypto';

import type { Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import { publish } from '@/messaging/producer';

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
  constructor(
    private readonly jobs: JobRepository,
    private readonly producer: Producer,
  ) {}

  /**
   * Publishes JobRequested via a direct producer.send — there's no outbox
   * yet (Milestone 9), so a crash between the row insert and this publish
   * still loses the event. That gap is closed at M9, not here.
   */
  async createJob(input: CreateJobInput): Promise<Job> {
    const job = Job.createNew({
      type: input.type,
      payload: input.payload,
      correlationId: randomUUID(),
      idempotencyKey: input.idempotencyKey ?? null,
    });

    let created: Job;
    try {
      created = await this.jobs.create(job);
    } catch (error) {
      if (error instanceof DuplicateJobSubmissionError) {
        const existing = await this.jobs.findByIdempotencyKey(error.idempotencyKey);

        if (existing) {
          return existing;
        }
      }

      throw error;
    }

    const requested = createEnvelope({
      eventType: JOB_REQUESTED_EVENT_TYPE,
      aggregateId: created.props.id,
      correlationId: created.props.correlationId,
      schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
      payload: { type: created.props.type, payload: created.props.payload, maxAttempts: created.props.maxAttempts },
    });

    await publish(this.producer, { topic: JOBS_REQUESTED_TOPIC, key: created.props.id, value: requested });

    return this.jobs.update(created.transitionTo('QUEUED'));
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
