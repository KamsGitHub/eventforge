import type { JobHandler } from '../domain/job-handler.port';

export class UnknownJobTypeError extends Error {
  constructor(public readonly jobType: string) {
    super(`No handler registered for job type "${jobType}"`);
    this.name = 'UnknownJobTypeError';
  }
}

export class ExecuteJobService {
  constructor(private readonly handlers: ReadonlyMap<string, JobHandler>) {}

  async execute(jobType: string, payload: unknown): Promise<unknown> {
    const handler = this.handlers.get(jobType);

    if (!handler) {
      throw new UnknownJobTypeError(jobType);
    }

    return handler.execute(payload);
  }
}
