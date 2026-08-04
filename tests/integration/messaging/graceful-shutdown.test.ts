import { randomUUID } from 'node:crypto';

import type { Kafka, Producer } from 'kafkajs';

import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';

import { spawnConsumerFixture } from '../fixtures/spawn-fixture';
import { sharedDatabaseUrl, sharedKafkaBrokers } from '../setup';

describe('graceful shutdown (real Kafka + Postgres via Testcontainers)', () => {
  let kafka: Kafka;
  let producer: Producer;
  let prisma: PrismaClient;

  beforeAll(async () => {
    kafka = createKafkaClient({ brokers: sharedKafkaBrokers(), clientId: 'eventforge-test' });
    prisma = createPrismaClient(sharedDatabaseUrl());
    producer = createProducer(kafka);
    await producer.connect();
  }, 30_000);

  afterAll(async () => {
    await producer?.disconnect();
    await prisma?.$disconnect();
  }, 30_000);

  it(
    'never commits the offset for a message that was still in flight when SIGTERM arrived, so a fresh consumer reprocesses it on restart',
    async () => {
      const topic = `_dev.shutdown-${randomUUID()}`;
      const groupId = `test.shutdown.${randomUUID()}`;

      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
      await admin.disconnect();

      const eventId = randomUUID();
      await publish(producer, { topic, key: eventId, value: { eventId, i: 0 } });

      // Deliberately no SIGTERM handler in the fixture — a real ungraceful
      // termination (the case createConsumer's autoCommit-after-handler
      // design protects against) means the process just dies mid-handler,
      // never reaching the offset-commit that only runs after the handler
      // resolves.
      const original = spawnConsumerFixture({
        FIXTURE_KAFKA_BROKERS: sharedKafkaBrokers().join(','),
        FIXTURE_DATABASE_URL: sharedDatabaseUrl(),
        FIXTURE_TOPIC: topic,
        FIXTURE_GROUP_ID: groupId,
        FIXTURE_DELAY_MS: '10000',
      });

      const exited = new Promise<void>((resolve) => original.child.once('exit', () => resolve()));

      try {
        await original.waitFor((e) => e.event === 'processing-start' && e['eventId'] === eventId, 30_000);

        original.kill('SIGTERM');
        await exited;

        expect(original.events.some((e) => e.event === 'processing-done')).toBe(false);

        const processedBeforeRestart = await prisma.processedEvent.findMany({ where: { consumerName: groupId } });
        expect(processedBeforeRestart).toHaveLength(0);

        const admin2 = kafka.admin();
        await admin2.connect();
        const offsets = await admin2.fetchOffsets({ groupId, topics: [topic] });
        await admin2.disconnect();
        // KafkaJS reports '-1' for a partition with no committed offset.
        expect(offsets[0]?.partitions[0]?.offset).toBe('-1');

        const restarted = spawnConsumerFixture({
          FIXTURE_KAFKA_BROKERS: sharedKafkaBrokers().join(','),
          FIXTURE_DATABASE_URL: sharedDatabaseUrl(),
          FIXTURE_TOPIC: topic,
          FIXTURE_GROUP_ID: groupId,
          FIXTURE_DELAY_MS: '20',
        });

        try {
          await restarted.waitFor((e) => e.event === 'processing-done' && e['eventId'] === eventId, 30_000);

          const processedAfterRestart = await prisma.processedEvent.findMany({ where: { consumerName: groupId } });
          expect(processedAfterRestart).toHaveLength(1);
        } finally {
          restarted.kill('SIGKILL');
        }
      } finally {
        original.kill('SIGKILL');
        await prisma.processedEvent.deleteMany({ where: { consumerName: groupId } });
      }
    },
    90_000,
  );
});
