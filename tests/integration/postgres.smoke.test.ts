import { Client } from 'pg';

import { sharedDatabaseUrl } from './setup';

describe('postgres via testcontainers (infra smoke test)', () => {
  let client: Client;

  beforeAll(async () => {
    client = new Client({ connectionString: sharedDatabaseUrl() });
    await client.connect();
  });

  afterAll(async () => {
    await client?.end();
  });

  it('connects to the shared testcontainers postgres and runs a query against it', async () => {
    const result = await client.query('SELECT 1 AS ok');

    expect(result.rows[0]).toEqual({ ok: 1 });
  });
});
