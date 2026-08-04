import { execSync } from 'node:child_process';

import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Wait } from 'testcontainers';

export interface StartedPostgres {
  readonly container: StartedPostgreSqlContainer;
  readonly connectionUri: string;
}

/**
 * Boots a fresh, throwaway Postgres container and applies every committed
 * migration against it via the real Prisma CLI — the same command used in
 * production — rather than hand-rolling schema setup.
 */
export async function startPostgresContainer(): Promise<StartedPostgres> {
  const container = await new PostgreSqlContainer('postgres:16-alpine').start();
  const connectionUri = container.getConnectionUri();

  execSync('npx prisma migrate deploy', {
    env: { ...process.env, DATABASE_URL: connectionUri },
    stdio: 'pipe',
  });

  return { container, connectionUri };
}

export interface StartedKafka {
  readonly container: StartedKafkaContainer;
  readonly brokers: readonly string[];
}

/**
 * Testcontainers' KafkaContainer wrapper only supports Confluent images (its
 * Kraft bootstrap script targets /etc/confluent/docker/*), unlike the
 * apache/kafka image docker-compose uses — Confluent is used here purely to
 * get a hermetic broker for tests. Its default wait strategy only waits for
 * the port to accept TCP connections, which fires while the broker is still
 * initializing and resets a real request sent that early, so this waits on
 * the broker's own "started" log line instead.
 */
export async function startKafkaContainer(): Promise<StartedKafka> {
  const container = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
    .withKraft()
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
    .start();

  return { container, brokers: [`${container.getHost()}:${container.getMappedPort(9093)}`] };
}

/**
 * Every production topic (infrastructure/kafka/create-topics.sh's list),
 * created once against the shared broker in global setup. A test that needs
 * an ad-hoc topic of its own (e.g. `_dev.ping`) still creates it locally —
 * this list only covers the ones the real consumers under test subscribe to.
 */
export const PRODUCTION_TOPICS = [
  'jobs.requested',
  'jobs.started',
  'jobs.completed',
  'jobs.failed',
  'jobs.retry-1',
  'jobs.retry-2',
  'jobs.retry-3',
  'jobs.dead-letter',
  'jobs.cancelled',
] as const;
