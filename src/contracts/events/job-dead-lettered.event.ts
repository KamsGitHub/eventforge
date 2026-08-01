import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';
import { failureRecordSchema } from '@/contracts/events/failure-record';

export const JOB_DEAD_LETTERED_EVENT_TYPE = 'JobDeadLettered';
export const JOB_DEAD_LETTERED_SCHEMA_VERSION = 1;

export const jobDeadLetteredPayloadSchemaV1 = z.object({
  // Deliberately looser than JobFailedPayloadV1 (nonnegative rather than
  // positive, type nullable): a poison message with no recoverable job
  // identity still needs to land here without failing this schema too.
  type: z.string().nullable(),
  payload: z.unknown(),
  attempt: z.number().int().nonnegative(),
  maxAttempts: z.number().int().nonnegative(),
  error: z.string(),
  failureHistory: z.array(failureRecordSchema),
});

export type JobDeadLetteredPayloadV1 = z.infer<typeof jobDeadLetteredPayloadSchemaV1>;

export const jobDeadLetteredEventSchemaV1 = envelopeSchema(jobDeadLetteredPayloadSchemaV1);

export type JobDeadLetteredEventV1 = z.infer<typeof jobDeadLetteredEventSchemaV1>;
