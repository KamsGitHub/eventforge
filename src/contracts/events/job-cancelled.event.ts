import { z } from 'zod';

import { envelopeSchema } from '@/contracts/envelope';

export const JOB_CANCELLED_EVENT_TYPE = 'JobCancelled';
export const JOB_CANCELLED_SCHEMA_VERSION = 1;

export const jobCancelledPayloadSchemaV1 = z.object({});

export type JobCancelledPayloadV1 = z.infer<typeof jobCancelledPayloadSchemaV1>;

export const jobCancelledEventSchemaV1 = envelopeSchema(jobCancelledPayloadSchemaV1);

export type JobCancelledEventV1 = z.infer<typeof jobCancelledEventSchemaV1>;
