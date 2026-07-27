import { Prisma, type PrismaClient, type PrismaJobRow } from '@/db/prisma-client';

import { JobVersionConflictError } from '../domain/errors';
import { Job, type JobProps } from '../domain/job.entity';
import type { JobRepository } from '../domain/job-repository.port';

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
    const row = await this.prisma.job.create({ data: toCreateInput(job.props) });

    return Job.fromProps(toProps(row));
  }

  async findById(id: string): Promise<Job | null> {
    const row = await this.prisma.job.findUnique({ where: { id } });

    return row ? Job.fromProps(toProps(row)) : null;
  }

  async update(job: Job): Promise<Job> {
    const props = job.props;

    const { count } = await this.prisma.job.updateMany({
      where: { id: props.id, version: props.version },
      data: {
        status: props.status,
        payload: props.payload as Prisma.InputJsonValue,
        result: props.result === null ? Prisma.JsonNull : (props.result as Prisma.InputJsonValue),
        error: props.error,
        attempt: props.attempt,
        maxAttempts: props.maxAttempts,
        correlationId: props.correlationId,
        updatedAt: props.updatedAt,
        startedAt: props.startedAt,
        completedAt: props.completedAt,
        version: { increment: 1 },
      },
    });

    if (count === 0) {
      throw new JobVersionConflictError(props.id, props.version);
    }

    const updated = await this.findById(props.id);

    /* istanbul ignore next -- the row cannot vanish between updateMany and findById within one request */
    if (!updated) {
      throw new Error(`Job ${props.id} disappeared immediately after a successful update`);
    }

    return updated;
  }
}
