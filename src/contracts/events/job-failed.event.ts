import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';
import { failureRecordSchema } from '@/contracts/events/failure-record';

export const JOB_FAILED_EVENT_TYPE = 'JobFailed';
export const JOB_FAILED_SCHEMA_VERSION = 1;

export const jobFailedPayloadSchemaV1 = z.object({
  // The retry router (Milestone 11) must be able to re-dispatch or dead-letter
  // this job from the event alone, without reading the Job row (execution
  // never touches the Job table) — so type/payload/maxAttempts travel with
  // every failure, not just attempt/error.
  type: z.string(),
  payload: z.unknown(),
  attempt: z.number().int().positive(),
  maxAttempts: z.number().int().positive(),
  error: z.string(),
  failureHistory: z.array(failureRecordSchema),
});

export type JobFailedPayloadV1 = z.infer<typeof jobFailedPayloadSchemaV1>;

export const jobFailedEventSchemaV1 = envelopeSchema(jobFailedPayloadSchemaV1);

export type JobFailedEventV1 = z.infer<typeof jobFailedEventSchemaV1>;
