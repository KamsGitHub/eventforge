import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';

export const JOB_COMPLETED_EVENT_TYPE = 'JobCompleted';
export const JOB_COMPLETED_SCHEMA_VERSION = 1;

export const jobCompletedPayloadSchemaV1 = z.object({
  result: z.unknown(),
});

export type JobCompletedPayloadV1 = z.infer<typeof jobCompletedPayloadSchemaV1>;

export const jobCompletedEventSchemaV1 = envelopeSchema(jobCompletedPayloadSchemaV1);

export type JobCompletedEventV1 = z.infer<typeof jobCompletedEventSchemaV1>;
