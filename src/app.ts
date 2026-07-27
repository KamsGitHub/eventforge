import Fastify from 'fastify';

import { loadConfig, type Config } from '@/config/env';
import { createLogger } from '@/shared/logger';

declare module 'fastify' {
  interface FastifyInstance {
    config: Config;
  }
}

export function buildApp(config: Config = loadConfig()) {
  const app = Fastify({
    loggerInstance: createLogger(config),
  });

  app.decorate('config', config);

  app.get('/health', () => ({ status: 'ok' }));

  return app;
}
