import { randomUUID } from 'node:crypto';

import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { Wait } from 'testcontainers';

import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { withIdempotency } from '@/messaging/idempotent-consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';

import { createTestDatabase, type TestDatabase } from '../helpers/postgres-test-db';

const CONSUMER_NAME = 'test.idempotent-consumer';
const RESTART_TOPIC = '_dev.idempotent-consumer-restart';

function buildPayload(body: unknown, offset: string): EachMessagePayload {
  return {
    topic: RESTART_TOPIC,
    partition: 0,
    message: {
      key: null,
      value: Buffer.from(JSON.stringify(body)),
      timestamp: Date.now().toString(),
      attributes: 0,
      offset,
      size: 0,
    },
    heartbeat: () => Promise.resolve(),
    pause: () => () => undefined,
  };
}

describe('withIdempotency (real Postgres via Testcontainers)', () => {
  let db: TestDatabase;
  let prisma: PrismaClient;

  beforeAll(async () => {
    db = await createTestDatabase();
    prisma = createPrismaClient(db.connectionUri);
  }, 120_000);

  afterAll(async () => {
    await prisma?.$disconnect();
    await db?.container.stop();
  }, 120_000);

  afterEach(async () => {
    await prisma.processedEvent.deleteMany();
  });

  it('applies the business change exactly once when the same event is delivered twice', async () => {
    let applyCount = 0;
    const handler = withIdempotency({ prisma, consumerName: CONSUMER_NAME }, () => {
      applyCount += 1;
      return Promise.resolve();
    });

    const body = { eventId: randomUUID(), aggregateId: 'job-duplicate' };

    await handler(buildPayload(body, '0'));
    // Redelivery of the exact same event — e.g. the offset was never
    // committed after the first delivery.
    await handler(buildPayload(body, '0'));

    expect(applyCount).toBe(1);

    const rows = await prisma.processedEvent.findMany({ where: { eventId: body.eventId } });
    expect(rows).toHaveLength(1);
  });

  it('lets only one of two concurrently racing deliveries apply the business change', async () => {
    let applyCount = 0;
    const handler = withIdempotency({ prisma, consumerName: CONSUMER_NAME }, () => {
      applyCount += 1;
      return Promise.resolve();
    });

    const body = { eventId: randomUUID(), aggregateId: 'job-concurrent' };

    // Real concurrent redelivery (e.g. a brief overlap during a rebalance)
    // racing on the real Postgres unique constraint, not a mock.
    await Promise.all([handler(buildPayload(body, '0')), handler(buildPayload(body, '0'))]);

    expect(applyCount).toBe(1);

    const rows = await prisma.processedEvent.findMany({ where: { eventId: body.eventId } });
    expect(rows).toHaveLength(1);
  });

  it('rolls back the ProcessedEvent insert when the business change throws, so a retry can re-apply it', async () => {
    let attempts = 0;
    const handler = withIdempotency({ prisma, consumerName: CONSUMER_NAME }, () => {
      attempts += 1;

      if (attempts === 1) {
        return Promise.reject(new Error('simulated business-logic failure'));
      }

      return Promise.resolve();
    });

    const body = { eventId: randomUUID(), aggregateId: 'job-failed-first-attempt' };

    await expect(handler(buildPayload(body, '0'))).rejects.toThrow('simulated business-logic failure');

    const afterFailure = await prisma.processedEvent.findMany({ where: { eventId: body.eventId } });
    expect(afterFailure).toHaveLength(0);

    await handler(buildPayload(body, '0'));

    expect(attempts).toBe(2);
    const afterRetry = await prisma.processedEvent.findMany({ where: { eventId: body.eventId } });
    expect(afterRetry).toHaveLength(1);
  });
});

describe('idempotent consumer restart (real Postgres + Kafka via Testcontainers)', () => {
  let db: TestDatabase;
  let prisma: PrismaClient;
  let kafkaContainer: StartedKafkaContainer;
  let kafka: Kafka;
  let producer: Producer;

  beforeAll(async () => {
    db = await createTestDatabase();
    prisma = createPrismaClient(db.connectionUri);

    kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
      .withKraft()
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();

    kafka = createKafkaClient({
      brokers: [`${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`],
      clientId: 'eventforge-test',
    });

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: RESTART_TOPIC, numPartitions: 1, replicationFactor: 1 }] });
    await admin.disconnect();

    producer = createProducer(kafka);
    await producer.connect();
  }, 120_000);

  afterAll(async () => {
    await producer?.disconnect();
    await prisma?.$disconnect();
    await kafkaContainer?.stop();
    await db?.container.stop();
  }, 120_000);

  afterEach(async () => {
    await prisma.processedEvent.deleteMany();
  });

  it('safely no-ops on redelivery of a message whose business change already committed before the offset did', async () => {
    let applyCount = 0;
    const handler = withIdempotency({ prisma, consumerName: CONSUMER_NAME }, () => {
      applyCount += 1;
      return Promise.resolve();
    });

    const body = { eventId: randomUUID(), aggregateId: 'job-restart' };

    // Simulates the crash: the DB transaction (business change + the
    // ProcessedEvent row) commits, but — because this call bypasses
    // createConsumer entirely — no Kafka offset is ever committed for it,
    // exactly like a process dying between the two.
    await handler(buildPayload(body, '0'));
    expect(applyCount).toBe(1);

    // "Restart": publish the same event for real and let a brand-new
    // consumer group — which has never committed an offset — pick it up
    // from the beginning, the same way a redelivery after a crash would.
    await publish(producer, { topic: RESTART_TOPIC, key: body.aggregateId, value: body });

    let resolveProcessed!: () => void;
    const processed = new Promise<void>((resolve) => {
      resolveProcessed = resolve;
    });

    const consumer = await createConsumer(kafka, {
      topic: RESTART_TOPIC,
      groupId: 'test.idempotent-consumer-restart-group',
      fromBeginning: true,
      autoCommit: false,
      handler: async (payload) => {
        await handler(payload);
        resolveProcessed();
      },
    });

    await processed;
    await consumer.disconnect();

    // Still 1: the redelivered message was recognized as already processed
    // and the business change never ran a second time.
    expect(applyCount).toBe(1);

    const rows = await prisma.processedEvent.findMany({ where: { eventId: body.eventId } });
    expect(rows).toHaveLength(1);
  }, 120_000);
});
