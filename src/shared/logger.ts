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
