import { z } from 'zod';

import type { Job } from '../domain/job.entity';

export const JobStatusSchema = z.enum([
  'PENDING',
  'QUEUED',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'DEAD_LETTERED',
  'CANCELLED',
]);

export const CreateJobBodySchema = z.object({
  type: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
});
export type CreateJobBody = z.infer<typeof CreateJobBodySchema>;

export const JobParamsSchema = z.object({
  jobId: z.uuid(),
});

export const ListJobsQuerySchema = z.object({
  status: JobStatusSchema.optional(),
  limit: z.coerce.number().int().positive().max(100).optional(),
  offset: z.coerce.number().int().nonnegative().optional(),
});

export const JobResponseSchema = z.object({
  id: z.string(),
  type: z.string(),
  status: JobStatusSchema,
  payload: z.unknown(),
  result: z.unknown().nullable(),
  error: z.string().nullable(),
  attempt: z.number(),
  maxAttempts: z.number(),
  idempotencyKey: z.string().nullable(),
  correlationId: z.string(),
  cancelRequested: z.boolean(),
  createdAt: z.string(),
  updatedAt: z.string(),
  startedAt: z.string().nullable(),
  completedAt: z.string().nullable(),
});
export type JobResponse = z.infer<typeof JobResponseSchema>;

export const ListJobsResponseSchema = z.object({
  jobs: z.array(JobResponseSchema),
});

export const ErrorResponseSchema = z.object({
  error: z.string(),
});

export function toJobResponse(job: Job): JobResponse {
  const props = job.props;

  return {
    id: props.id,
    type: props.type,
    status: props.status,
    payload: props.payload,
    result: props.result,
    error: props.error,
    attempt: props.attempt,
    maxAttempts: props.maxAttempts,
    idempotencyKey: props.idempotencyKey,
    correlationId: props.correlationId,
    cancelRequested: props.cancelRequested,
    createdAt: props.createdAt.toISOString(),
    updatedAt: props.updatedAt.toISOString(),
    startedAt: props.startedAt ? props.startedAt.toISOString() : null,
    completedAt: props.completedAt ? props.completedAt.toISOString() : null,
  };
}
