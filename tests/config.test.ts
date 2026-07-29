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
    });

    expect(config).toEqual({
      nodeEnv: 'production',
      port: 4000,
      logLevel: 'warn',
      databaseUrl: DATABASE_URL,
      kafkaBrokers: ['broker-1:9092', 'broker-2:9092'],
      kafkaClientId: 'eventforge-worker',
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
