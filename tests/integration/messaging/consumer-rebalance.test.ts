import { randomUUID } from 'node:crypto';

import type { Kafka, Producer } from 'kafkajs';

import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';

import { spawnConsumerFixture } from '../fixtures/spawn-fixture';
import { sharedDatabaseUrl, sharedKafkaBrokers } from '../setup';

describe('consumer rebalance (real Kafka + Postgres via Testcontainers)', () => {
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
    'cleanly hands off an in-flight partition to a second consumer instance after the first is killed, with no duplicate side effects',
    async () => {
      const topic = `_dev.rebalance-${randomUUID()}`;
      const groupId = `test.rebalance.${randomUUID()}`;

      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
      await admin.disconnect();

      // One partition, so whichever consumer owns it owns every message —
      // this is what makes "no duplicate side effects" unambiguous to check.
      const eventIds = Array.from({ length: 4 }, () => randomUUID());

      for (const [i, eventId] of eventIds.entries()) {
        await publish(producer, { topic, key: eventId, value: { eventId, i } });
      }

      // Long per-message delay + a short session timeout: gives the test a
      // window to kill consumerA while it's provably still holding message
      // 0 (offset not yet committed), and lets the broker detect the kill
      // quickly instead of waiting out Kafka's ~30s default.
      const consumerA = spawnConsumerFixture({
        FIXTURE_KAFKA_BROKERS: sharedKafkaBrokers().join(','),
        FIXTURE_DATABASE_URL: sharedDatabaseUrl(),
        FIXTURE_TOPIC: topic,
        FIXTURE_GROUP_ID: groupId,
        FIXTURE_DELAY_MS: '10000',
        FIXTURE_SESSION_TIMEOUT_MS: '6000',
        FIXTURE_HEARTBEAT_INTERVAL_MS: '1500',
      });

      try {
        const firstMessage = eventIds[0];
        await consumerA.waitFor((e) => e.event === 'processing-start' && e['eventId'] === firstMessage, 30_000);

        // Simulated crash: SIGKILL gives consumerA no chance to leave the
        // group cleanly or commit anything, exactly like a killed pod —
        // the broker only notices once heartbeats stop arriving.
        consumerA.kill('SIGKILL');

        const consumerB = spawnConsumerFixture({
          FIXTURE_KAFKA_BROKERS: sharedKafkaBrokers().join(','),
          FIXTURE_DATABASE_URL: sharedDatabaseUrl(),
          FIXTURE_TOPIC: topic,
          FIXTURE_GROUP_ID: groupId,
          FIXTURE_DELAY_MS: '20',
        });

        try {
          for (const eventId of eventIds) {
            await consumerB.waitFor((e) => e.event === 'processing-done' && e['eventId'] === eventId, 30_000);
          }

          // consumerA never got past the artificial delay on message 0, so
          // it never emitted 'processing-done' for anything — every message,
          // including the one it was mid-processing, was applied exactly
          // once, by consumerB, after the rebalance picked up the
          // never-committed offset.
          const doneEventsA = consumerA.events.filter((e) => e.event === 'processing-done');
          expect(doneEventsA).toHaveLength(0);

          const processedRows = await prisma.processedEvent.findMany({ where: { consumerName: groupId } });
          expect(processedRows).toHaveLength(eventIds.length);
          expect(new Set(processedRows.map((row) => row.eventId)).size).toBe(eventIds.length);
        } finally {
          consumerB.kill('SIGKILL');
        }
      } finally {
        consumerA.kill('SIGKILL');
        await prisma.processedEvent.deleteMany({ where: { consumerName: groupId } });
      }
    },
    90_000,
  );
});
