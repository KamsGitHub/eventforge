import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  DATABASE_URL: z.string().min(1, 'DATABASE_URL is required'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('eventforge'),
  RETRY_TIER_1_DELAY_MS: z.coerce.number().int().positive().default(30_000),
  RETRY_TIER_2_DELAY_MS: z.coerce.number().int().positive().default(300_000),
  RETRY_TIER_3_DELAY_MS: z.coerce.number().int().positive().default(1_800_000),
  JOB_TIMEOUT_MS: z.coerce.number().int().positive().default(300_000),
  TIMEOUT_WATCHDOG_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
});

export type Config = Readonly<{
  nodeEnv: 'development' | 'test' | 'production';
  port: number;
  logLevel: 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace' | 'silent';
  databaseUrl: string;
  kafkaBrokers: readonly string[];
  kafkaClientId: string;
  retryTierDelaysMs: Readonly<Record<1 | 2 | 3, number>>;
  jobTimeoutMs: number;
  timeoutWatchdogPollIntervalMs: number;
}>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = envSchema.safeParse(env);

  if (!parsed.success) {
    throw new Error(`Invalid environment configuration:\n${z.prettifyError(parsed.error)}`);
  }

  return {
    nodeEnv: parsed.data.NODE_ENV,
    port: parsed.data.PORT,
    logLevel: parsed.data.LOG_LEVEL,
    databaseUrl: parsed.data.DATABASE_URL,
    kafkaBrokers: parsed.data.KAFKA_BROKERS.split(',').map((broker) => broker.trim()),
    kafkaClientId: parsed.data.KAFKA_CLIENT_ID,
    retryTierDelaysMs: {
      1: parsed.data.RETRY_TIER_1_DELAY_MS,
      2: parsed.data.RETRY_TIER_2_DELAY_MS,
      3: parsed.data.RETRY_TIER_3_DELAY_MS,
    },
    jobTimeoutMs: parsed.data.JOB_TIMEOUT_MS,
    timeoutWatchdogPollIntervalMs: parsed.data.TIMEOUT_WATCHDOG_POLL_INTERVAL_MS,
  };
}
