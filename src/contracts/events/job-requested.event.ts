import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';

export const JOB_REQUESTED_EVENT_TYPE = 'JobRequested';
export const JOB_REQUESTED_SCHEMA_VERSION = 1;

export const jobRequestedPayloadSchemaV1 = z.object({
  type: z.string(),
  payload: z.unknown(),
  maxAttempts: z.number().int().positive(),
});

export type JobRequestedPayloadV1 = z.infer<typeof jobRequestedPayloadSchemaV1>;

export const jobRequestedEventSchemaV1 = envelopeSchema(jobRequestedPayloadSchemaV1);

export type JobRequestedEventV1 = z.infer<typeof jobRequestedEventSchemaV1>;
