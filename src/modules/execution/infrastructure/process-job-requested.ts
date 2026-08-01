import type { Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_CANCELLED_EVENT_TYPE, JOB_CANCELLED_SCHEMA_VERSION } from '@/contracts/events/job-cancelled.event';
import { JOB_COMPLETED_EVENT_TYPE, JOB_COMPLETED_SCHEMA_VERSION } from '@/contracts/events/job-completed.event';
import { JOB_FAILED_EVENT_TYPE, JOB_FAILED_SCHEMA_VERSION } from '@/contracts/events/job-failed.event';
import { jobRequestedEventSchemaV1 } from '@/contracts/events/job-requested.event';
import { JOB_STARTED_EVENT_TYPE, JOB_STARTED_SCHEMA_VERSION } from '@/contracts/events/job-started.event';
import { JOBS_CANCELLED_TOPIC } from '@/contracts/topics';
import { publish } from '@/messaging/producer';

import type { ExecuteJobService } from '../application/execute-job.service';
import type { CancellationChecker } from '../domain/cancellation-checker.port';
import { publishMalformedMessageToDeadLetter } from './dead-letter';

export const JOBS_STARTED_TOPIC = 'jobs.started';
export const JOBS_COMPLETED_TOPIC = 'jobs.completed';
export const JOBS_FAILED_TOPIC = 'jobs.failed';

export interface ProcessJobRequestedDeps {
  readonly producer: Producer;
  readonly executeJobService: ExecuteJobService;
  /**
   * Optional so tests/fakes that don't care about cancellation can omit it;
   * a missing checker behaves as "never cancelled". Real wiring always
   * supplies one (see server.ts).
   */
  readonly isJobCancelled?: CancellationChecker;
}

/**
 * Shared by both the initial jobs.requested consumer and each retry-tier
 * consumer — a retried dispatch is handled identically to the original one,
 * just carrying a higher `attempt` and accumulated `failureHistory`.
 *
 * A body that fails schema validation is routed straight to dead-letter
 * instead of thrown: a thrown error would leave the offset uncommitted and
 * retry forever (KafkaJS's default redelivery), blocking the partition on a
 * message that can never parse successfully — and a malformed payload isn't
 * a "retryable" failure in the first place.
 */
export async function processJobRequestedBody(body: unknown, deps: ProcessJobRequestedDeps): Promise<void> {
  const parsed = jobRequestedEventSchemaV1.safeParse(body);

  if (!parsed.success) {
    await publishMalformedMessageToDeadLetter(deps.producer, body, parsed.error.message);
    return;
  }

  const envelope = parsed.data;
  const jobId = envelope.aggregateId;
  const { attempt, type, payload: jobPayload, maxAttempts, failureHistory } = envelope.payload;
  const isCancelled = (): Promise<boolean> => deps.isJobCancelled?.(jobId) ?? Promise.resolve(false);

  // Covers the race where a PENDING/QUEUED cancel lands after this event
  // was already produced to Kafka but before it's consumed here — the job's
  // status is already CANCELLED by the time we'd otherwise dispatch it, so
  // skip entirely rather than resurrecting it with a JobStarted/Completed.
  if (await isCancelled()) {
    return;
  }

  const started = createEnvelope({
    eventType: JOB_STARTED_EVENT_TYPE,
    aggregateId: jobId,
    correlationId: envelope.correlationId,
    causationId: envelope.eventId,
    schemaVersion: JOB_STARTED_SCHEMA_VERSION,
    payload: { attempt },
  });

  await publish(deps.producer, { topic: JOBS_STARTED_TOPIC, key: jobId, value: started });

  const context = { isCancelled };

  try {
    const result = await deps.executeJobService.execute(type, jobPayload, context);

    if (await isCancelled()) {
      await publishCancelled(deps.producer, jobId, envelope);
      return;
    }

    const completed = createEnvelope({
      eventType: JOB_COMPLETED_EVENT_TYPE,
      aggregateId: jobId,
      correlationId: envelope.correlationId,
      causationId: envelope.eventId,
      schemaVersion: JOB_COMPLETED_SCHEMA_VERSION,
      payload: { result },
    });

    await publish(deps.producer, { topic: JOBS_COMPLETED_TOPIC, key: jobId, value: completed });
  } catch (error) {
    if (await isCancelled()) {
      await publishCancelled(deps.producer, jobId, envelope);
      return;
    }

    const errorMessage = error instanceof Error ? error.message : String(error);

    const failed = createEnvelope({
      eventType: JOB_FAILED_EVENT_TYPE,
      aggregateId: jobId,
      correlationId: envelope.correlationId,
      causationId: envelope.eventId,
      schemaVersion: JOB_FAILED_SCHEMA_VERSION,
      payload: {
        type,
        payload: jobPayload,
        attempt,
        maxAttempts,
        error: errorMessage,
        failureHistory: [...failureHistory, { attempt, error: errorMessage, failedAt: new Date().toISOString() }],
      },
    });

    await publish(deps.producer, { topic: JOBS_FAILED_TOPIC, key: jobId, value: failed });
  }
}

async function publishCancelled(
  producer: Producer,
  jobId: string,
  triggeringEnvelope: { readonly eventId: string; readonly correlationId: string },
): Promise<void> {
  const cancelled = createEnvelope({
    eventType: JOB_CANCELLED_EVENT_TYPE,
    aggregateId: jobId,
    correlationId: triggeringEnvelope.correlationId,
    causationId: triggeringEnvelope.eventId,
    schemaVersion: JOB_CANCELLED_SCHEMA_VERSION,
    payload: {},
  });

  await publish(producer, { topic: JOBS_CANCELLED_TOPIC, key: jobId, value: cancelled });
}
