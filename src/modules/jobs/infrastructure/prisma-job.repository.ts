import { Prisma, type PrismaClient, type PrismaJobRow } from '@/db/prisma-client';
import { insertOutboxEvent } from '@/outbox/outbox.repository';
import type { NewOutboxEvent } from '@/outbox/outbox.types';

import { DuplicateJobSubmissionError, JobVersionConflictError } from '../domain/errors';
import { Job, type JobProps } from '../domain/job.entity';
import type { JobListFilter, JobRepository, JobRepositoryClient } from '../domain/job-repository.port';

function toProps(row: PrismaJobRow): JobProps {
  return {
    id: row.id,
    type: row.type,
    status: row.status,
    payload: row.payload,
    result: row.result,
    error: row.error,
    attempt: row.attempt,
    maxAttempts: row.maxAttempts,
    idempotencyKey: row.idempotencyKey,
    correlationId: row.correlationId,
    cancelRequested: row.cancelRequested,
    version: row.version,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    startedAt: row.startedAt,
    completedAt: row.completedAt,
  };
}

function toCreateInput(props: JobProps): Prisma.JobCreateInput {
  return {
    id: props.id,
    type: props.type,
    status: props.status,
    payload: props.payload as Prisma.InputJsonValue,
    result: props.result === null ? undefined : props.result,
    error: props.error,
    attempt: props.attempt,
    maxAttempts: props.maxAttempts,
    idempotencyKey: props.idempotencyKey,
    correlationId: props.correlationId,
    cancelRequested: props.cancelRequested,
    version: props.version,
    createdAt: props.createdAt,
    updatedAt: props.updatedAt,
    startedAt: props.startedAt,
    completedAt: props.completedAt,
  };
}

/**
 * The only file in the jobs module allowed to import the Prisma client —
 * this is the seam the execution-service extraction (Milestone 18) cuts
 * along.
 */
export class PrismaJobRepository implements JobRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async create(job: Job): Promise<Job> {
    try {
      const row = await this.prisma.job.create({ data: toCreateInput(job.props) });

      return Job.fromProps(toProps(row));
    } catch (error) {
      // P2002 = unique constraint violation. idempotencyKey is currently the
      // only unique column besides the primary key, so this is unambiguous;
      // revisit this check if a second unique constraint is ever added.
      if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002' && job.props.idempotencyKey) {
        throw new DuplicateJobSubmissionError(job.props.idempotencyKey);
      }

      throw error;
    }
  }

  async createWithOutboxEvent(job: Job, outboxEvent: NewOutboxEvent): Promise<Job> {
    try {
      const row = await this.prisma.$transaction(async (tx) => {
        const created = await tx.job.create({ data: toCreateInput(job.props) });
        await insertOutboxEvent(tx, outboxEvent);

        return created;
      });

      return Job.fromProps(toProps(row));
    } catch (error) {
      // Same P2002 -> DuplicateJobSubmissionError translation as create().
      if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002' && job.props.idempotencyKey) {
        throw new DuplicateJobSubmissionError(job.props.idempotencyKey);
      }

      throw error;
    }
  }

  async findById(id: string, client: JobRepositoryClient = this.prisma): Promise<Job | null> {
    const row = await client.job.findUnique({ where: { id } });

    return row ? Job.fromProps(toProps(row)) : null;
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    const row = await this.prisma.job.findUnique({ where: { idempotencyKey } });

    return row ? Job.fromProps(toProps(row)) : null;
  }

  async list(filter: JobListFilter): Promise<Job[]> {
    const rows = await this.prisma.job.findMany({
      where: filter.status ? { status: filter.status } : undefined,
      orderBy: { createdAt: 'desc' },
      take: filter.limit,
      skip: filter.offset,
    });

    return rows.map((row) => Job.fromProps(toProps(row)));
  }

  async update(job: Job, client: JobRepositoryClient = this.prisma): Promise<Job> {
    return this.updateWithClient(job, client);
  }

  async updateWithOutboxEvent(job: Job, outboxEvent: NewOutboxEvent): Promise<Job> {
    return this.prisma.$transaction(async (tx) => {
      const updated = await this.updateWithClient(job, tx);
      await insertOutboxEvent(tx, outboxEvent);

      return updated;
    });
  }

  private async updateWithClient(job: Job, client: JobRepositoryClient): Promise<Job> {
    const props = job.props;

    const { count } = await client.job.updateMany({
      where: { id: props.id, version: props.version },
      data: {
        status: props.status,
        payload: props.payload as Prisma.InputJsonValue,
        result: props.result === null ? Prisma.JsonNull : (props.result as Prisma.InputJsonValue),
        error: props.error,
        attempt: props.attempt,
        maxAttempts: props.maxAttempts,
        correlationId: props.correlationId,
        cancelRequested: props.cancelRequested,
        updatedAt: props.updatedAt,
        startedAt: props.startedAt,
        completedAt: props.completedAt,
        version: { increment: 1 },
      },
    });

    if (count === 0) {
      throw new JobVersionConflictError(props.id, props.version);
    }

    const updated = await this.findById(props.id, client);

    /* istanbul ignore next -- the row cannot vanish between updateMany and findById within one request */
    if (!updated) {
      throw new Error(`Job ${props.id} disappeared immediately after a successful update`);
    }

    return updated;
  }
}
