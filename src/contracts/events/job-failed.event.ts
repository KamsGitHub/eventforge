import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';

export const JOB_FAILED_EVENT_TYPE = 'JobFailed';
export const JOB_FAILED_SCHEMA_VERSION = 1;

export const jobFailedPayloadSchemaV1 = z.object({
  attempt: z.number().int().positive(),
  error: z.string(),
});

export type JobFailedPayloadV1 = z.infer<typeof jobFailedPayloadSchemaV1>;

export const jobFailedEventSchemaV1 = envelopeSchema(jobFailedPayloadSchemaV1);

export type JobFailedEventV1 = z.infer<typeof jobFailedEventSchemaV1>;
