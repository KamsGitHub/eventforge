import type { Producer } from 'kafkajs';

import { createEnvelope } from '@/contracts/envelope';
import { JOB_CANCELLED_EVENT_TYPE } from '@/contracts/events/job-cancelled.event';
import { JOB_COMPLETED_EVENT_TYPE } from '@/contracts/events/job-completed.event';
import { JOB_FAILED_EVENT_TYPE } from '@/contracts/events/job-failed.event';
import { JOB_REQUESTED_EVENT_TYPE, JOB_REQUESTED_SCHEMA_VERSION } from '@/contracts/events/job-requested.event';
import { JOB_STARTED_EVENT_TYPE } from '@/contracts/events/job-started.event';
import { ExecuteJobService } from '@/modules/execution/application/execute-job.service';
import type { JobHandler } from '@/modules/execution/domain/job-handler.port';
import { processJobRequestedBody } from '@/modules/execution/infrastructure/process-job-requested';

const JOB_TYPE = 'SOME_JOB';

function buildRequestedBody(overrides: Partial<Record<string, unknown>> = {}) {
  return createEnvelope({
    eventType: JOB_REQUESTED_EVENT_TYPE,
    aggregateId: 'job-1',
    schemaVersion: JOB_REQUESTED_SCHEMA_VERSION,
    payload: { type: JOB_TYPE, payload: {}, maxAttempts: 3, attempt: 1, failureHistory: [], ...overrides },
  });
}

function fakeProducer(): { producer: Producer; send: jest.Mock } {
  const send = jest.fn().mockResolvedValue([]);

  return { producer: { send } as unknown as Producer, send };
}

function sentEventTypes(send: jest.Mock): string[] {
  return (send.mock.calls as Array<[{ messages: { value: string }[] }]>).map(
    ([call]) => (JSON.parse(call.messages[0]!.value) as { eventType: string }).eventType,
  );
}

describe('processJobRequestedBody — cooperative cancellation (Milestone 12)', () => {
  it('publishes JobStarted then JobCompleted when never cancelled', async () => {
    const { producer, send } = fakeProducer();
    const handler: JobHandler = { execute: () => Promise.resolve({ ok: true }) };
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, handler]]));

    await processJobRequestedBody(buildRequestedBody(), {
      producer,
      executeJobService,
      isJobCancelled: () => Promise.resolve(false),
    });

    expect(sentEventTypes(send)).toEqual([JOB_STARTED_EVENT_TYPE, JOB_COMPLETED_EVENT_TYPE]);
  });

  it('skips dispatch entirely when the job is already cancelled before it would be picked up', async () => {
    const { producer, send } = fakeProducer();
    const execute = jest.fn().mockResolvedValue({ ok: true });
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, { execute }]]));

    await processJobRequestedBody(buildRequestedBody(), {
      producer,
      executeJobService,
      isJobCancelled: () => Promise.resolve(true),
    });

    expect(execute).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it('publishes JobCancelled instead of JobCompleted when cancellation is observed after a successful execute()', async () => {
    const { producer, send } = fakeProducer();
    const handler: JobHandler = { execute: () => Promise.resolve({ ok: true }) };
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, handler]]));
    const isJobCancelled = jest.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true);

    await processJobRequestedBody(buildRequestedBody(), { producer, executeJobService, isJobCancelled });

    expect(sentEventTypes(send)).toEqual([JOB_STARTED_EVENT_TYPE, JOB_CANCELLED_EVENT_TYPE]);
  });

  it('publishes JobCancelled instead of JobFailed when cancellation is observed after execute() rejects', async () => {
    const { producer, send } = fakeProducer();
    const handler: JobHandler = { execute: () => Promise.reject(new Error('boom')) };
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, handler]]));
    const isJobCancelled = jest.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true);

    await processJobRequestedBody(buildRequestedBody(), { producer, executeJobService, isJobCancelled });

    expect(sentEventTypes(send)).toEqual([JOB_STARTED_EVENT_TYPE, JOB_CANCELLED_EVENT_TYPE]);
  });

  it('still publishes JobFailed for a genuine failure when cancellation was never requested', async () => {
    const { producer, send } = fakeProducer();
    const handler: JobHandler = { execute: () => Promise.reject(new Error('boom')) };
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, handler]]));

    await processJobRequestedBody(buildRequestedBody(), {
      producer,
      executeJobService,
      isJobCancelled: () => Promise.resolve(false),
    });

    expect(sentEventTypes(send)).toEqual([JOB_STARTED_EVENT_TYPE, JOB_FAILED_EVENT_TYPE]);
  });

  it('defaults to never-cancelled when isJobCancelled is omitted', async () => {
    const { producer, send } = fakeProducer();
    const handler: JobHandler = { execute: () => Promise.resolve({ ok: true }) };
    const executeJobService = new ExecuteJobService(new Map([[JOB_TYPE, handler]]));

    await processJobRequestedBody(buildRequestedBody(), { producer, executeJobService });

    expect(sentEventTypes(send)).toEqual([JOB_STARTED_EVENT_TYPE, JOB_COMPLETED_EVENT_TYPE]);
  });
});
