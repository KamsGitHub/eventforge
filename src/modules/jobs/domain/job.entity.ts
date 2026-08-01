import { randomUUID } from 'node:crypto';

import { IllegalJobStateTransitionError } from './errors';
import type { JobStatus } from './job-status';

export type { JobStatus } from './job-status';

export interface JobProps {
  readonly id: string;
  readonly type: string;
  readonly status: JobStatus;
  readonly payload: unknown;
  readonly result: unknown;
  readonly error: string | null;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly idempotencyKey: string | null;
  readonly correlationId: string;
  readonly cancelRequested: boolean;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly startedAt: Date | null;
  readonly completedAt: Date | null;
}

export interface NewJobInput {
  readonly type: string;
  readonly payload: unknown;
  readonly correlationId: string;
  readonly idempotencyKey?: string | null;
  readonly maxAttempts?: number;
}

export interface TransitionChanges {
  readonly result?: unknown;
  readonly error?: string | null;
}

/**
 * Permitted transitions. DEAD_LETTERED is semi-terminal: only a manual retry
 * (back to QUEUED) can escape it. SUCCEEDED and CANCELLED are fully terminal.
 */
const ALLOWED_TRANSITIONS: Readonly<Record<JobStatus, readonly JobStatus[]>> = {
  PENDING: ['QUEUED', 'CANCELLED'],
  QUEUED: ['RUNNING', 'CANCELLED'],
  RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED'],
  SUCCEEDED: [],
  FAILED: ['QUEUED', 'DEAD_LETTERED'],
  DEAD_LETTERED: ['QUEUED'],
  CANCELLED: [],
};

export class Job {
  private constructor(private readonly jobProps: JobProps) {}

  static createNew(input: NewJobInput): Job {
    const now = new Date();

    return new Job({
      id: randomUUID(),
      type: input.type,
      status: 'PENDING',
      payload: input.payload,
      result: null,
      error: null,
      attempt: 0,
      maxAttempts: input.maxAttempts ?? 3,
      idempotencyKey: input.idempotencyKey ?? null,
      correlationId: input.correlationId,
      cancelRequested: false,
      version: 0,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      completedAt: null,
    });
  }

  static fromProps(props: JobProps): Job {
    return new Job(props);
  }

  get props(): Readonly<JobProps> {
    return this.jobProps;
  }

  transitionTo(to: JobStatus, changes: TransitionChanges = {}): Job {
    const from = this.jobProps.status;
    const allowed = ALLOWED_TRANSITIONS[from];

    if (!allowed.includes(to)) {
      throw new IllegalJobStateTransitionError(from, to);
    }

    const now = new Date();
    const patch: Partial<JobProps> = ((): Partial<JobProps> => {
      switch (to) {
        case 'QUEUED':
          return { error: null, completedAt: null };
        case 'RUNNING':
          return { attempt: this.jobProps.attempt + 1, startedAt: now, error: null };
        case 'SUCCEEDED':
          return { result: changes.result ?? this.jobProps.result, completedAt: now };
        case 'FAILED':
          return { error: changes.error ?? this.jobProps.error };
        case 'DEAD_LETTERED':
          return { error: changes.error ?? this.jobProps.error, completedAt: now };
        case 'CANCELLED':
          return { completedAt: now, cancelRequested: false };
        case 'PENDING':
          return {};
      }
    })();

    return new Job({ ...this.jobProps, ...patch, status: to, updatedAt: now });
  }

  /**
   * Cancel is immediate from PENDING/QUEUED (a normal transition, enforced
   * the same way as any other). From RUNNING it's cooperative: the job
   * stays RUNNING with cancelRequested set, and whatever's executing it
   * must observe the flag and stop itself — real cancellation of in-flight
   * work needs a handler that polls between "ticks", which is a known
   * limitation for simulated/instant handlers, not a bug here. Every other
   * source state rejects outright rather than silently no-op'ing.
   */
  cancel(): Job {
    if (this.jobProps.status === 'PENDING' || this.jobProps.status === 'QUEUED') {
      return this.transitionTo('CANCELLED');
    }

    if (this.jobProps.status === 'RUNNING') {
      return new Job({ ...this.jobProps, cancelRequested: true, updatedAt: new Date() });
    }

    throw new IllegalJobStateTransitionError(this.jobProps.status, 'CANCELLED');
  }

  /**
   * Manual retry (user-initiated, from FAILED/DEAD_LETTERED) vs. the
   * automatic retry-topic routing (Milestone 11) are conceptually distinct
   * even though both eventually increment attempt — this only handles the
   * former. The QUEUED transition itself never touches attempt; that still
   * only happens at the next RUNNING transition, same as every other path
   * back to QUEUED.
   */
  retry(): Job {
    if (this.jobProps.status !== 'FAILED' && this.jobProps.status !== 'DEAD_LETTERED') {
      throw new IllegalJobStateTransitionError(this.jobProps.status, 'QUEUED');
    }

    return this.transitionTo('QUEUED');
  }
}
