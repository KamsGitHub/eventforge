import { execSync } from 'node:child_process';

import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';

export interface TestDatabase {
  readonly container: StartedPostgreSqlContainer;
  readonly connectionUri: string;
}

/**
 * Boots a fresh, throwaway Postgres container and applies every committed
 * migration against it via the real Prisma CLI — the same command used in
 * production — rather than hand-rolling schema setup. Hermetic: no reliance
 * on the docker-compose Postgres being up.
 */
export async function createTestDatabase(): Promise<TestDatabase> {
  const container = await new PostgreSqlContainer('postgres:16-alpine').start();
  const connectionUri = container.getConnectionUri();

  execSync('npx prisma migrate deploy', {
    env: { ...process.env, DATABASE_URL: connectionUri },
    stdio: 'pipe',
  });

  return { container, connectionUri };
}
