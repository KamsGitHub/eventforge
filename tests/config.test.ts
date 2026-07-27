import { loadConfig } from '@/config/env';

const DATABASE_URL = 'postgresql://user:pass@localhost:5432/db';

describe('loadConfig', () => {
  it('applies defaults when optional env vars are missing', () => {
    const config = loadConfig({ DATABASE_URL });

    expect(config).toEqual({ nodeEnv: 'development', port: 3000, logLevel: 'info', databaseUrl: DATABASE_URL });
  });

  it('reads valid overrides', () => {
    const config = loadConfig({ NODE_ENV: 'production', PORT: '4000', LOG_LEVEL: 'warn', DATABASE_URL });

    expect(config).toEqual({ nodeEnv: 'production', port: 4000, logLevel: 'warn', databaseUrl: DATABASE_URL });
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
