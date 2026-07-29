import { ExecuteJobService, UnknownJobTypeError } from '@/modules/execution/application/execute-job.service';
import type { JobHandler } from '@/modules/execution/domain/job-handler.port';

describe('ExecuteJobService', () => {
  it('routes to the registered handler for a known job type', async () => {
    const execute = jest.fn().mockResolvedValue({ ok: true });
    const handler: JobHandler = { execute };
    const service = new ExecuteJobService(new Map([['SOME_JOB', handler]]));

    const result = await service.execute('SOME_JOB', { input: 1 });

    expect(execute).toHaveBeenCalledWith({ input: 1 });
    expect(result).toEqual({ ok: true });
  });

  it('throws UnknownJobTypeError for an unregistered job type', async () => {
    const service = new ExecuteJobService(new Map());

    await expect(service.execute('NOPE', {})).rejects.toThrow(UnknownJobTypeError);
  });
});
