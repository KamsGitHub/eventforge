import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';

export const JOB_STARTED_EVENT_TYPE = 'JobStarted';
export const JOB_STARTED_SCHEMA_VERSION = 1;

export const jobStartedPayloadSchemaV1 = z.object({
  attempt: z.number().int().positive(),
});

export type JobStartedPayloadV1 = z.infer<typeof jobStartedPayloadSchemaV1>;

export const jobStartedEventSchemaV1 = envelopeSchema(jobStartedPayloadSchemaV1);

export type JobStartedEventV1 = z.infer<typeof jobStartedEventSchemaV1>;
