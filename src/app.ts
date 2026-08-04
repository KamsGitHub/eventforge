import Fastify from 'fastify';
import { serializerCompiler, validatorCompiler, type ZodTypeProvider } from 'fastify-type-provider-zod';
import type pino from 'pino';

import { loadConfig, type Config } from '@/config/env';
import { registerJobRoutes } from '@/modules/jobs/api/routes';
import type { JobService } from '@/modules/jobs/application/job.service';
import { generateCorrelationId } from '@/shared/correlation-plugin';
import { createLogger } from '@/shared/logger';
import { metricsRegistry } from '@/shared/metrics';

declare module 'fastify' {
  interface FastifyInstance {
    config: Config;
  }
}

export interface BuildAppDeps {
  readonly config?: Config;
  readonly jobService: JobService;
  /** Overrides the pino instance createLogger(config) would otherwise build — lets tests capture output (e.g. asserting a correlationId reaches the access log) via a custom destination stream. */
  readonly logger?: pino.Logger;
}

export function buildApp(deps: BuildAppDeps) {
  const config = deps.config ?? loadConfig();

  const app = Fastify({
    loggerInstance: deps.logger ?? createLogger(config),
    genReqId: generateCorrelationId,
  }).withTypeProvider<ZodTypeProvider>();

  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  app.decorate('config', config);

  app.get('/health', () => ({ status: 'ok' }));

  app.get('/metrics', async (_request, reply) => {
    reply.header('Content-Type', metricsRegistry.contentType);
    return metricsRegistry.metrics();
  });

  registerJobRoutes(app, deps.jobService);

  return app;
}
