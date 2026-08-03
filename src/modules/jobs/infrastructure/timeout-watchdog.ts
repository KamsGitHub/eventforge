import type { Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_FAILED_EVENT_TYPE, JOB_FAILED_SCHEMA_VERSION } from '@/contracts/events/job-failed.event';
import { publish } from '@/messaging/producer';
import type { Logger } from '@/shared/logger';

import type { Job } from '../domain/job.entity';
import type { JobRepository } from '../domain/job-repository.port';

const JOBS_FAILED_TOPIC = 'jobs.failed';
const DEFAULT_POLL_INTERVAL_MS = 30_000;
const TIMEOUT_ERROR = 'timeout';
const WATCHDOG_LIST_LIMIT = 100;

export interface TimeoutWatchdogOptions {
  readonly jobRepository: JobRepository;
  readonly producer: Producer;
  /** How long a job may stay RUNNING before it's considered stuck. */
  readonly timeoutMs: number;
  readonly pollIntervalMs?: number;
  readonly logger?: Logger;
}

/**
 * Polls for RUNNING jobs whose startedAt is past the deadline and publishes
 * a JobFailed event for each — the exact same event execution would publish
 * on a real handler failure, so it flows through the same retry-router /
 * job-status-consumer pipeline (retry or dead-letter) without either of
 * those needing to know a timeout is a different kind of failure.
 *
 * Never writes Job.status directly (only jobs module's status consumer
 * does, reacting to this same JobFailed event) — this class only detects
 * and publishes.
 */
export class TimeoutWatchdog {
  private readonly flaggedJobIds = new Set<string>();
  private timer: NodeJS.Timeout | undefined;
  private polling = false;

  constructor(private readonly options: TimeoutWatchdogOptions) {}

  start(): void {
    const intervalMs = this.options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.timer = setInterval(() => void this.pollOnce(), intervalMs);
  }

  stop(): void {
    clearInterval(this.timer);
    this.timer = undefined;
  }

  /** Exposed for tests to drive deterministically instead of waiting on the timer. */
  async pollOnce(): Promise<number> {
    if (this.polling) {
      return 0;
    }

    this.polling = true;
    try {
      const deadline = new Date(Date.now() - this.options.timeoutMs);
      const runningJobs = await this.options.jobRepository.list({ status: 'RUNNING', limit: WATCHDOG_LIST_LIMIT, offset: 0 });

      const timedOut = runningJobs.filter((job) => job.props.startedAt !== null && job.props.startedAt < deadline);
      let publishedCount = 0;

      for (const job of timedOut) {
        if (this.flaggedJobIds.has(job.props.id)) {
          continue;
        }

        this.flaggedJobIds.add(job.props.id);
        await this.publishTimeoutFailure(job);
        publishedCount += 1;
      }

      const stillRunningIds = new Set(runningJobs.map((job) => job.props.id));

      for (const jobId of this.flaggedJobIds) {
        if (!stillRunningIds.has(jobId)) {
          this.flaggedJobIds.delete(jobId);
        }
      }

      return publishedCount;
    } finally {
      this.polling = false;
    }
  }

  private async publishTimeoutFailure(job: Job): Promise<void> {
    const failed = createEnvelope({
      eventType: JOB_FAILED_EVENT_TYPE,
      aggregateId: job.props.id,
      correlationId: job.props.correlationId,
      schemaVersion: JOB_FAILED_SCHEMA_VERSION,
      payload: {
        type: job.props.type,
        payload: job.props.payload,
        attempt: job.props.attempt,
        maxAttempts: job.props.maxAttempts,
        error: TIMEOUT_ERROR,
        failureHistory: [{ attempt: job.props.attempt, error: TIMEOUT_ERROR, failedAt: new Date().toISOString() }],
      },
    });

    await publish(this.options.producer, { topic: JOBS_FAILED_TOPIC, key: job.props.id, value: failed, logger: this.options.logger });
  }
}
