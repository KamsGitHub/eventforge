import type { FastifyInstance, RawServerDefault } from 'fastify';
import type { ZodTypeProvider } from 'fastify-type-provider-zod';
import type { IncomingMessage, ServerResponse } from 'node:http';

import type { JobService } from '../application/job.service';

import {
  CreateJobBodySchema,
  ErrorResponseSchema,
  JobParamsSchema,
  JobResponseSchema,
  ListJobsQuerySchema,
  ListJobsResponseSchema,
  toJobResponse,
} from './schemas';

// The Logger generic is deliberately `any`: app.ts passes a concrete pino
// instance via `loggerInstance`, whose type doesn't structurally satisfy
// FastifyBaseLogger (a known Fastify+pino generic-variance quirk with
// route registration methods). We don't need to be precise about the
// logger type here — only about the Zod type provider.
/* eslint-disable-next-line @typescript-eslint/no-explicit-any -- see comment above */
type AnyLogger = any;
type AppInstance = FastifyInstance<
  RawServerDefault,
  IncomingMessage,
  ServerResponse<IncomingMessage>,
  AnyLogger,
  ZodTypeProvider
>;

export function registerJobRoutes(app: AppInstance, jobService: JobService): void {
  const server = app;

  server.post(
    '/api/jobs',
    {
      schema: {
        body: CreateJobBodySchema,
        response: { 201: JobResponseSchema },
      },
    },
    async (request, reply) => {
      const idempotencyKeyHeader = request.headers['idempotency-key'];
      const idempotencyKey = typeof idempotencyKeyHeader === 'string' ? idempotencyKeyHeader : undefined;

      const job = await jobService.createJob({ ...request.body, idempotencyKey });

      await reply.code(201).send(toJobResponse(job));
    },
  );

  server.get(
    '/api/jobs/:jobId',
    {
      schema: {
        params: JobParamsSchema,
        response: { 200: JobResponseSchema, 404: ErrorResponseSchema },
      },
    },
    async (request, reply) => {
      const job = await jobService.getJob(request.params.jobId);

      if (!job) {
        await reply.code(404).send({ error: `Job ${request.params.jobId} not found` });
        return;
      }

      await reply.send(toJobResponse(job));
    },
  );

  server.get(
    '/api/jobs',
    {
      schema: {
        querystring: ListJobsQuerySchema,
        response: { 200: ListJobsResponseSchema },
      },
    },
    async (request, reply) => {
      const jobs = await jobService.listJobs(request.query);

      await reply.send({ jobs: jobs.map(toJobResponse) });
    },
  );
}
