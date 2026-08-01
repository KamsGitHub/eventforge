import { DuplicateJobSubmissionError, JobVersionConflictError } from '@/modules/jobs/domain/errors';
import { Job } from '@/modules/jobs/domain/job.entity';
import type { JobListFilter, JobRepository } from '@/modules/jobs/domain/job-repository.port';
import type { NewOutboxEvent } from '@/outbox/outbox.types';

/** In-memory JobRepository test double — no database involved. */
export class InMemoryJobRepository implements JobRepository {
  private readonly jobsById = new Map<string, Job>();

  /** Outbox events "inserted" via createWithOutboxEvent — for tests to assert on, no real outbox involved. */
  readonly outboxEvents: NewOutboxEvent[] = [];

  create(job: Job): Promise<Job> {
    if (job.props.idempotencyKey && this.findByIdempotencyKeySync(job.props.idempotencyKey)) {
      return Promise.reject(new DuplicateJobSubmissionError(job.props.idempotencyKey));
    }

    this.jobsById.set(job.props.id, job);

    return Promise.resolve(job);
  }

  async createWithOutboxEvent(job: Job, outboxEvent: NewOutboxEvent): Promise<Job> {
    const created = await this.create(job);
    this.outboxEvents.push(outboxEvent);

    return created;
  }

  findById(id: string): Promise<Job | null> {
    return Promise.resolve(this.jobsById.get(id) ?? null);
  }

  findByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    return Promise.resolve(this.findByIdempotencyKeySync(idempotencyKey));
  }

  list(filter: JobListFilter): Promise<Job[]> {
    let jobs = [...this.jobsById.values()].sort((a, b) => b.props.createdAt.getTime() - a.props.createdAt.getTime());

    if (filter.status) {
      jobs = jobs.filter((job) => job.props.status === filter.status);
    }

    return Promise.resolve(jobs.slice(filter.offset, filter.offset + filter.limit));
  }

  update(job: Job): Promise<Job> {
    const existing = this.jobsById.get(job.props.id);

    if (!existing || existing.props.version !== job.props.version) {
      return Promise.reject(new JobVersionConflictError(job.props.id, job.props.version));
    }

    const updated = Job.fromProps({ ...job.props, version: job.props.version + 1 });
    this.jobsById.set(job.props.id, updated);

    return Promise.resolve(updated);
  }

  async updateWithOutboxEvent(job: Job, outboxEvent: NewOutboxEvent): Promise<Job> {
    const updated = await this.update(job);
    this.outboxEvents.push(outboxEvent);

    return updated;
  }

  private findByIdempotencyKeySync(idempotencyKey: string): Job | null {
    for (const job of this.jobsById.values()) {
      if (job.props.idempotencyKey === idempotencyKey) {
        return job;
      }
    }

    return null;
  }
}
