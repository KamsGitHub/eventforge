import type { StartedKafkaContainer } from '@testcontainers/kafka';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';

// globalSetup/globalTeardown are loaded outside Jest's normal module
// registry, so the '@/' alias (wired via moduleNameMapper) doesn't resolve
// here the way it does in regular test files — a plain relative import is
// required instead.
import { createKafkaClient } from '../../src/messaging/kafka-client';

import { PRODUCTION_TOPICS, startKafkaContainer, startPostgresContainer } from './containers';

const DATABASE_URL_ENV = 'TEST_DATABASE_URL';
const KAFKA_BROKERS_ENV = 'TEST_KAFKA_BROKERS';

/**
 * globalSetup and globalTeardown run once each, in the same parent Jest
 * process, before/after every worker — so container handles stashed on
 * `globalThis` in setup are still there for teardown to read. Connection
 * info for the workers themselves (separate processes) travels via
 * `process.env`, which workers inherit since they're spawned after
 * globalSetup completes.
 */
interface SharedContainers {
  readonly postgres: StartedPostgreSqlContainer;
  readonly kafka: StartedKafkaContainer;
}

const globalWithContainers = globalThis as typeof globalThis & { __EVENTFORGE_TEST_CONTAINERS__?: SharedContainers };

/**
 * One Postgres + one Kafka container for the entire integration test run,
 * rather than every file booting (and waiting on) its own — booting a real
 * JVM broker per file was the dominant cost of this suite. Individual test
 * files still clean up their own rows between tests (see each file's
 * afterEach) so they don't pollute each other despite sharing the database.
 */
export async function globalSetup(): Promise<void> {
  const [postgres, kafka] = await Promise.all([startPostgresContainer(), startKafkaContainer()]);

  const kafkaClient = createKafkaClient({ brokers: kafka.brokers, clientId: 'eventforge-test-setup' });
  const admin = kafkaClient.admin();
  await admin.connect();
  await admin.createTopics({
    topics: PRODUCTION_TOPICS.map((topic) => ({ topic, numPartitions: 3, replicationFactor: 1 })),
  });
  await admin.disconnect();

  globalWithContainers.__EVENTFORGE_TEST_CONTAINERS__ = { postgres: postgres.container, kafka: kafka.container };
  process.env[DATABASE_URL_ENV] = postgres.connectionUri;
  process.env[KAFKA_BROKERS_ENV] = kafka.brokers.join(',');
}

export async function globalTeardown(): Promise<void> {
  const containers = globalWithContainers.__EVENTFORGE_TEST_CONTAINERS__;

  await containers?.kafka.stop();
  await containers?.postgres.stop();
}

/** Read by individual test files' beforeAll — see globalSetup above for how these are populated. */
export function sharedDatabaseUrl(): string {
  const url = process.env[DATABASE_URL_ENV];

  if (!url) {
    throw new Error(`${DATABASE_URL_ENV} is not set — is this test running under the "integration" Jest project?`);
  }

  return url;
}

export function sharedKafkaBrokers(): string[] {
  const brokers = process.env[KAFKA_BROKERS_ENV];

  if (!brokers) {
    throw new Error(`${KAFKA_BROKERS_ENV} is not set — is this test running under the "integration" Jest project?`);
  }

  return brokers.split(',');
}
