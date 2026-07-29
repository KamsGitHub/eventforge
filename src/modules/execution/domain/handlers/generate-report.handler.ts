import { randomUUID } from 'node:crypto';

import { z } from 'zod';

import type { JobHandler } from '../job-handler.port';

export const GENERATE_REPORT_JOB_TYPE = 'GENERATE_REPORT';

const payloadSchema = z.object({
  reportName: z.string().min(1),
});

const SIMULATED_WORK_MS = 10;

export class GenerateReportHandler implements JobHandler {
  async execute(payload: unknown): Promise<unknown> {
    const { reportName } = payloadSchema.parse(payload);

    await new Promise((resolve) => setTimeout(resolve, SIMULATED_WORK_MS));

    return {
      reportId: randomUUID(),
      reportName,
      generatedAt: new Date().toISOString(),
    };
  }
}
