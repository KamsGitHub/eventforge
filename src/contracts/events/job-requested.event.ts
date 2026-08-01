import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';
import { failureRecordSchema } from '@/contracts/events/failure-record';

export const JOB_REQUESTED_EVENT_TYPE = 'JobRequested';
export const JOB_REQUESTED_SCHEMA_VERSION = 1;

export const jobRequestedPayloadSchemaV1 = z.object({
  type: z.string(),
  payload: z.unknown(),
  maxAttempts: z.number().int().positive(),
  /** Which attempt this dispatch represents — 1 for the original submission, incremented on each automatic retry. */
  attempt: z.number().int().positive(),
  /** Prior attempts' failures, carried forward so a later dead-letter/retry hop never needs a DB lookup. */
  failureHistory: z.array(failureRecordSchema),
});

export type JobRequestedPayloadV1 = z.infer<typeof jobRequestedPayloadSchemaV1>;

export const jobRequestedEventSchemaV1 = envelopeSchema(jobRequestedPayloadSchemaV1);

export type JobRequestedEventV1 = z.infer<typeof jobRequestedEventSchemaV1>;
