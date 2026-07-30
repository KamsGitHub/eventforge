import { JOB_REQUESTED_EVENT_TYPE } from '@/contracts/events/job-requested.event';
import type { OutboxEventRow } from '@/outbox/outbox.types';

import type { JobRepository } from '../domain/job-repository.port';

/**
 * The jobs-module-owned reaction to the outbox publisher confirming a send —
 * the state machine's PENDING -> QUEUED transition happens here, through the
 * jobs module's own repository, rather than in the (module-agnostic) outbox
 * publisher itself. Keeps `src/outbox` reusable for other modules' outboxes
 * later without teaching it what a "Job" is.
 */
export function createJobOutboxPublishedHandler(jobRepository: JobRepository): (row: OutboxEventRow) => Promise<void> {
  return async (row) => {
    if (row.eventType !== JOB_REQUESTED_EVENT_TYPE) {
      return;
    }

    const job = await jobRepository.findById(row.aggregateId);

    if (!job || job.props.status !== 'PENDING') {
      return;
    }

    await jobRepository.update(job.transitionTo('QUEUED'));
  };
}
