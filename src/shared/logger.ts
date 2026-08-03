import pino from 'pino';

import type { Config } from '@/config/env';

export function createLogger(config: Pick<Config, 'nodeEnv' | 'logLevel'>): pino.Logger {
  return pino({
    level: config.logLevel,
    transport:
      config.nodeEnv === 'development'
        ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'HH:MM:ss', ignore: 'pid,hostname' } }
        : undefined,
  });
}

/**
 * The minimal logging surface the messaging wrapper and Kafka-side modules
 * depend on — deliberately narrower than pino.Logger so callers aren't
 * exposed to the FastifyBaseLogger-vs-pino.Logger generic-variance quirk
 * documented in modules/jobs/api/routes.ts (both a real pino.Logger and
 * Fastify's request.log satisfy this structurally).
 */
export interface Logger {
  info(fields: Record<string, unknown>, msg?: string): void;
  error(fields: Record<string, unknown>, msg?: string): void;
  child(bindings: Record<string, unknown>): Logger;
}

/** Fallback for call sites that don't care about correlation logging (mainly tests constructing a handler directly). */
export const noopLogger: Logger = {
  info: () => undefined,
  error: () => undefined,
  child: () => noopLogger,
};
