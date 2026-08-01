import { ExecuteJobService, UnknownJobTypeError } from '@/modules/execution/application/execute-job.service';
import type { JobExecutionContext, JobHandler } from '@/modules/execution/domain/job-handler.port';

const neverCancelled: JobExecutionContext = { isCancelled: () => Promise.resolve(false) };

describe('ExecuteJobService', () => {
  it('routes to the registered handler for a known job type', async () => {
    const execute = jest.fn().mockResolvedValue({ ok: true });
    const handler: JobHandler = { execute };
    const service = new ExecuteJobService(new Map([['SOME_JOB', handler]]));

    const result = await service.execute('SOME_JOB', { input: 1 }, neverCancelled);

    expect(execute).toHaveBeenCalledWith({ input: 1 }, neverCancelled);
    expect(result).toEqual({ ok: true });
  });

  it('throws UnknownJobTypeError for an unregistered job type', async () => {
    const service = new ExecuteJobService(new Map());

    await expect(service.execute('NOPE', {}, neverCancelled)).rejects.toThrow(UnknownJobTypeError);
  });
});
