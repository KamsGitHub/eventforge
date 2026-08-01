import { loadConfig } from '@/config/env';

const DATABASE_URL = 'postgresql://user:pass@localhost:5432/db';

describe('loadConfig', () => {
  it('applies defaults when optional env vars are missing', () => {
    const config = loadConfig({ DATABASE_URL });

    expect(config).toEqual({
      nodeEnv: 'development',
      port: 3000,
      logLevel: 'info',
      databaseUrl: DATABASE_URL,
      kafkaBrokers: ['localhost:9092'],
      kafkaClientId: 'eventforge',
      retryTierDelaysMs: { 1: 30_000, 2: 300_000, 3: 1_800_000 },
      jobTimeoutMs: 300_000,
      timeoutWatchdogPollIntervalMs: 30_000,
    });
  });

  it('reads valid overrides', () => {
    const config = loadConfig({
      NODE_ENV: 'production',
      PORT: '4000',
      LOG_LEVEL: 'warn',
      DATABASE_URL,
      KAFKA_BROKERS: 'broker-1:9092, broker-2:9092',
      KAFKA_CLIENT_ID: 'eventforge-worker',
      RETRY_TIER_1_DELAY_MS: '1000',
      RETRY_TIER_2_DELAY_MS: '2000',
      RETRY_TIER_3_DELAY_MS: '3000',
      JOB_TIMEOUT_MS: '60000',
      TIMEOUT_WATCHDOG_POLL_INTERVAL_MS: '5000',
    });

    expect(config).toEqual({
      nodeEnv: 'production',
      port: 4000,
      logLevel: 'warn',
      databaseUrl: DATABASE_URL,
      kafkaBrokers: ['broker-1:9092', 'broker-2:9092'],
      kafkaClientId: 'eventforge-worker',
      retryTierDelaysMs: { 1: 1000, 2: 2000, 3: 3000 },
      jobTimeoutMs: 60_000,
      timeoutWatchdogPollIntervalMs: 5000,
    });
  });

  it('throws a clear error on an invalid PORT', () => {
    expect(() => loadConfig({ PORT: 'not-a-number', DATABASE_URL })).toThrow(/Invalid environment configuration/);
  });

  it('throws a clear error on an invalid NODE_ENV', () => {
    expect(() => loadConfig({ NODE_ENV: 'staging', DATABASE_URL })).toThrow(/Invalid environment configuration/);
  });

  it('throws a clear error when DATABASE_URL is missing', () => {
    expect(() => loadConfig({})).toThrow(/Invalid environment configuration/);
  });
});
