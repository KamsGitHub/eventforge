import { loadConfig } from '@/config/env';

describe('loadConfig', () => {
  it('applies defaults when env vars are missing', () => {
    const config = loadConfig({});

    expect(config).toEqual({ nodeEnv: 'development', port: 3000, logLevel: 'info' });
  });

  it('reads valid overrides', () => {
    const config = loadConfig({ NODE_ENV: 'production', PORT: '4000', LOG_LEVEL: 'warn' });

    expect(config).toEqual({ nodeEnv: 'production', port: 4000, logLevel: 'warn' });
  });

  it('throws a clear error on an invalid PORT', () => {
    expect(() => loadConfig({ PORT: 'not-a-number' })).toThrow(/Invalid environment configuration/);
  });

  it('throws a clear error on an invalid NODE_ENV', () => {
    expect(() => loadConfig({ NODE_ENV: 'staging' })).toThrow(/Invalid environment configuration/);
  });
});
