import Fastify from 'fastify';
import { serializerCompiler, validatorCompiler, type ZodTypeProvider } from 'fastify-type-provider-zod';

import { loadConfig, type Config } from '@/config/env';
import { registerJobRoutes } from '@/modules/jobs/api/routes';
import type { JobService } from '@/modules/jobs/application/job.service';
import { createLogger } from '@/shared/logger';

declare module 'fastify' {
  interface FastifyInstance {
    config: Config;
  }
}

export interface BuildAppDeps {
  readonly config?: Config;
  readonly jobService: JobService;
}

export function buildApp(deps: BuildAppDeps) {
  const config = deps.config ?? loadConfig();

  const app = Fastify({
    loggerInstance: createLogger(config),
  }).withTypeProvider<ZodTypeProvider>();

  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  app.decorate('config', config);

  app.get('/health', () => ({ status: 'ok' }));

  registerJobRoutes(app, deps.jobService);

  return app;
}
