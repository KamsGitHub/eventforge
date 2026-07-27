import { buildApp } from '@/app';
import { loadConfig } from '@/config/env';

describe('GET /health', () => {
  it('returns 200 with an ok status', async () => {
    const app = buildApp(loadConfig({ LOG_LEVEL: 'silent', DATABASE_URL: 'postgresql://user:pass@localhost:5432/db' }));

    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: 'ok' });

    await app.close();
  });
});
