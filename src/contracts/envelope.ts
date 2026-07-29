import { randomUUID } from 'node:crypto';

import { z } from 'zod';

/**
 * Every Kafka message uses this shape. Message key is always the jobId
 * (aggregateId), so all events for one job stay in the same partition and
 * are processed in order.
 */
export function envelopeSchema<PayloadSchema extends z.ZodType>(payloadSchema: PayloadSchema) {
  return z.object({
    eventId: z.uuid(),
    eventType: z.string(),
    aggregateId: z.string(),
    correlationId: z.uuid(),
    causationId: z.uuid().nullable(),
    occurredAt: z.iso.datetime(),
    schemaVersion: z.number().int().positive(),
    payload: payloadSchema,
  });
}

export type Envelope<Payload> = Readonly<{
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId: string | null;
  occurredAt: string;
  schemaVersion: number;
  payload: Payload;
}>;

export interface CreateEnvelopeInput<Payload> {
  readonly eventType: string;
  readonly aggregateId: string;
  readonly schemaVersion: number;
  readonly payload: Payload;
  /** Same value for one job's entire lifecycle; generated if this is the first event. */
  readonly correlationId?: string;
  /** eventId of the event that triggered this one, if any. */
  readonly causationId?: string | null;
}

export function createEnvelope<Payload>(input: CreateEnvelopeInput<Payload>): Envelope<Payload> {
  return {
    eventId: randomUUID(),
    eventType: input.eventType,
    aggregateId: input.aggregateId,
    correlationId: input.correlationId ?? randomUUID(),
    causationId: input.causationId ?? null,
    occurredAt: new Date().toISOString(),
    schemaVersion: input.schemaVersion,
    payload: input.payload,
  };
}
