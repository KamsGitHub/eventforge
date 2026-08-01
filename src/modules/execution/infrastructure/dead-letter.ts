import { randomUUID } from 'node:crypto';

import type { Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_DEAD_LETTERED_EVENT_TYPE, JOB_DEAD_LETTERED_SCHEMA_VERSION } from '@/contracts/events/job-dead-lettered.event';
import type { FailureRecord } from '@/contracts/events/failure-record';
import { JOBS_DEAD_LETTER_TOPIC } from '@/contracts/topics';
import { publish } from '@/messaging/producer';

export interface DeadLetterInput {
  readonly jobId: string;
  readonly correlationId?: string;
  readonly causationId?: string | null;
  readonly type: string | null;
  readonly payload: unknown;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly error: string;
  readonly failureHistory: readonly FailureRecord[];
}

/** Retries exhausted for a job whose identity/history we actually have (the normal path, driven by retry-router). */
export async function publishDeadLetter(producer: Producer, input: DeadLetterInput): Promise<void> {
  const envelope = createEnvelope({
    eventType: JOB_DEAD_LETTERED_EVENT_TYPE,
    aggregateId: input.jobId,
    correlationId: input.correlationId,
    causationId: input.causationId ?? null,
    schemaVersion: JOB_DEAD_LETTERED_SCHEMA_VERSION,
    payload: {
      type: input.type,
      payload: input.payload,
      attempt: input.attempt,
      maxAttempts: input.maxAttempts,
      error: input.error,
      failureHistory: input.failureHistory,
    },
  });

  await publish(producer, { topic: JOBS_DEAD_LETTER_TOPIC, key: input.jobId, value: envelope });
}

/**
 * A message that failed schema validation before we could even tell what job
 * it was for. Routed straight to dead-letter rather than thrown — a thrown
 * error would leave the offset uncommitted and retry forever, blocking the
 * partition on a message that can never parse successfully.
 */
export async function publishMalformedMessageToDeadLetter(producer: Producer, rawBody: unknown, reason: string): Promise<void> {
  const bodyRecord = isRecord(rawBody) ? rawBody : null;
  const aggregateId = typeof bodyRecord?.['aggregateId'] === 'string' ? bodyRecord['aggregateId'] : randomUUID();
  const nestedPayload = isRecord(bodyRecord?.['payload']) ? bodyRecord['payload'] : null;

  await publishDeadLetter(producer, {
    jobId: aggregateId,
    type: typeof nestedPayload?.['type'] === 'string' ? nestedPayload['type'] : null,
    payload: nestedPayload?.['payload'] ?? null,
    attempt: 0,
    maxAttempts: 0,
    error: `malformed message: ${reason}`,
    failureHistory: [],
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
