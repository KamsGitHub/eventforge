import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Client } from 'pg';

describe('postgres via testcontainers (infra smoke test)', () => {
  let container: StartedPostgreSqlContainer;
  let client: Client;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine').start();
    client = new Client({ connectionString: container.getConnectionUri() });
    await client.connect();
  }, 60_000);

  afterAll(async () => {
    await client?.end();
    await container?.stop();
  }, 30_000);

  it('boots a real postgres container and runs a query against it', async () => {
    const result = await client.query('SELECT 1 AS ok');

    expect(result.rows[0]).toEqual({ ok: 1 });
  });
});
