import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { Wait } from 'testcontainers';

import { createPrismaClient, type PrismaClient } from '@/db/prisma-client';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';
import { OutboxPublisher } from '@/outbox/outbox-publisher';
import { claimNextUnpublished, insertOutboxEvent, markPublished } from '@/outbox/outbox.repository';

import { createTestDatabase, type TestDatabase } from '../helpers/postgres-test-db';

const CONCURRENT_CLAIM_TOPIC = '_dev.outbox-concurrent-claim';
const CRASH_RECOVERY_TOPIC = '_dev.outbox-crash-recovery';

describe('OutboxPublisher (real Postgres + Kafka via Testcontainers)', () => {
  let db: TestDatabase;
  let prisma: PrismaClient;
  let kafkaContainer: StartedKafkaContainer;
  let kafka: Kafka;
  let producer: Producer;

  beforeAll(async () => {
    db = await createTestDatabase();
    prisma = createPrismaClient(db.connectionUri);

    // Same Confluent-image + explicit-readiness-wait workaround used by the
    // other Kafka Testcontainers suites — see CLAUDE.md.
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
    await admin.createTopics({
      topics: [CONCURRENT_CLAIM_TOPIC, CRASH_RECOVERY_TOPIC].map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
    });
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
    await prisma.outboxEvent.deleteMany();
  });

  async function consumeMessages(
    topic: string,
    count: number,
    groupId: string,
  ): Promise<{ consumer: Consumer; received: Promise<EachMessagePayload[]> }> {
    const messages: EachMessagePayload[] = [];
    let resolveAll!: (payloads: EachMessagePayload[]) => void;
    const received = new Promise<EachMessagePayload[]>((resolve) => {
      resolveAll = resolve;
    });

    const consumer = await createConsumer(kafka, {
      topic,
      groupId,
      fromBeginning: true,
      handler: (payload) => {
        messages.push(payload);

        if (messages.length >= count) {
          resolveAll(messages);
        }

        return Promise.resolve();
      },
    });

    return { consumer, received };
  }

  it('never lets two concurrent publisher instances claim the same row', async () => {
    const rowCount = 10;

    for (let i = 0; i < rowCount; i += 1) {
      await insertOutboxEvent(prisma, {
        aggregateId: `job-${i}`,
        eventType: 'DevTest',
        topic: CONCURRENT_CLAIM_TOPIC,
        messageKey: `job-${i}`,
        payload: { i },
      });
    }

    const { consumer, received } = await consumeMessages(
      CONCURRENT_CLAIM_TOPIC,
      rowCount,
      'test.outbox-concurrent-consumer',
    );

    const publisherA = new OutboxPublisher({ prisma, producer });
    const publisherB = new OutboxPublisher({ prisma, producer });

    // Both instances race to drain the same unpublished backlog concurrently
    // — SKIP LOCKED means each row is claimed by exactly one of them.
    await Promise.all([publisherA.pollOnce(), publisherB.pollOnce()]);

    const messages = await received;
    await consumer.disconnect();

    // Not 2x rowCount — proves no row was claimed (and sent) by both instances.
    expect(messages).toHaveLength(rowCount);

    const rows = await prisma.outboxEvent.findMany();
    expect(rows).toHaveLength(rowCount);
    expect(rows.every((row) => row.publishedAt !== null)).toBe(true);
    expect(rows.every((row) => row.publishingAttempts === 0)).toBe(true);
  }, 120_000);

  it('re-sends an event after a crash between the Kafka send and the publishedAt commit', async () => {
    const aggregateId = 'crash-test-job';

    await insertOutboxEvent(prisma, {
      aggregateId,
      eventType: 'DevTest',
      topic: CRASH_RECOVERY_TOPIC,
      messageKey: aggregateId,
      payload: { crash: true },
    });

    const { consumer, received } = await consumeMessages(CRASH_RECOVERY_TOPIC, 2, 'test.outbox-crash-consumer');

    // Simulate a process crash: claim + send exactly like the publisher
    // would, then abort the transaction instead of committing the
    // publishedAt update. The Kafka send already happened and can't be
    // undone by a DB rollback — that's the whole point of this test.
    await expect(
      prisma.$transaction(async (tx) => {
        const row = await claimNextUnpublished(tx);

        if (!row) {
          throw new Error('expected an unpublished row to claim');
        }

        await publish(producer, { topic: row.topic, key: row.messageKey, value: row.payload });
        await markPublished(tx, row.id);

        throw new Error('simulated crash before commit');
      }),
    ).rejects.toThrow('simulated crash before commit');

    const stillUnpublished = await prisma.outboxEvent.findFirst({ where: { aggregateId } });
    expect(stillUnpublished?.publishedAt).toBeNull();

    // The real publisher claims the still-unpublished row and re-sends it.
    const publisher = new OutboxPublisher({ prisma, producer });
    const publishedCount = await publisher.pollOnce();

    expect(publishedCount).toBe(1);

    const messages = await received;
    await consumer.disconnect();

    // Two deliveries for one logical event — at-least-once, not exactly-once.
    expect(messages).toHaveLength(2);

    const finalRow = await prisma.outboxEvent.findFirst({ where: { aggregateId } });
    expect(finalRow?.publishedAt).not.toBeNull();
  }, 120_000);
});
