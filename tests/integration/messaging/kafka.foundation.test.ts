import type { EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { z } from 'zod';

import { createEnvelope, envelopeSchema } from '@/contracts/envelope';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';

import { createCapturingLogger } from '../../helpers/capturing-logger';
import { sharedKafkaBrokers } from '../setup';

const PING_TOPIC = '_dev.ping';
const pingEnvelopeSchema = envelopeSchema(z.object({ message: z.string() }));

// This file has occasionally hung/timed out under load — both locally after
// a long run of back-to-back Testcontainers suites, and on GitHub's 2-vCPU
// runner once this ran as the ~8th Kafka-heavy file in one shared-broker
// session. Root cause looks like genuine broker/CPU contention (a new
// consumer group's join taking unusually long), not a bug: it's hit either
// of the two tests below at different times, and passes reliably in
// isolation. Retrying automates the project's own existing "rerun before
// assuming a real regression" policy rather than papering over a real bug.
jest.retryTimes(2, { logErrorsBeforeRetry: true });

describe('Kafka producer/consumer foundation (real broker via Testcontainers)', () => {
  beforeAll(async () => {
    const kafka = createKafkaClient({ brokers: sharedKafkaBrokers(), clientId: 'eventforge-test-setup' });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: PING_TOPIC, numPartitions: 1, replicationFactor: 1 }] });
    await admin.disconnect();
  }, 60_000);

  // Each test gets its own Kafka client (own connection pool), rather than
  // sharing one across both `it`s. This test consistently hung on CI/locally
  // — always the *second* test, never the first — which pointed at a race
  // between the previous test's `consumer.disconnect()` resolving at the JS
  // level and the broker connection it used actually being torn down before
  // the next test's client tried to reuse a pooled connection on the same
  // `Kafka` instance. A dedicated client per test removes that possibility
  // outright instead of chasing the exact kafkajs-internal timing.
  async function withKafkaClient<T>(fn: (kafka: Kafka, producer: Producer) => Promise<T>): Promise<T> {
    const kafka = createKafkaClient({ brokers: sharedKafkaBrokers(), clientId: `eventforge-test-${process.hrtime.bigint()}` });
    const producer = createProducer(kafka);
    await producer.connect();

    try {
      return await fn(kafka, producer);
    } finally {
      await producer.disconnect();
    }
  }

  it(
    'produces and consumes a real message end-to-end, round-tripping the envelope through zod',
    () =>
      withKafkaClient(async (kafka, producer) => {
        const envelope = createEnvelope({
          eventType: 'DevPing',
          aggregateId: 'ping-1',
          schemaVersion: 1,
          payload: { message: 'hello' },
        });

        let resolveReceived!: (payload: EachMessagePayload) => void;
        const received = new Promise<EachMessagePayload>((resolve) => {
          resolveReceived = resolve;
        });

        const consumer = await createConsumer(kafka, {
          topic: PING_TOPIC,
          groupId: 'eventforge.dev.ping-consumer',
          handler: (payload) => {
            resolveReceived(payload);
            return Promise.resolve();
          },
        });

        try {
          await publish(producer, { topic: PING_TOPIC, key: envelope.aggregateId, value: envelope });

          const message = await received;
          const parsed = pingEnvelopeSchema.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

          expect(parsed.eventType).toBe('DevPing');
          expect(parsed.aggregateId).toBe('ping-1');
          expect(parsed.payload).toEqual({ message: 'hello' });
        } finally {
          await consumer.disconnect();
        }
      }),
    60_000,
  );

  it(
    'logs correlation fields automatically on publish and on consume, without the caller doing any logging itself (Milestone 13)',
    () =>
      withKafkaClient(async (kafka, producer) => {
        const envelope = createEnvelope({
          eventType: 'DevPing',
          aggregateId: 'ping-2',
          schemaVersion: 1,
          payload: { message: 'hello again' },
        });

        const producerLog = createCapturingLogger();
        const consumerLog = createCapturingLogger();

        let resolveReceived!: () => void;
        const received = new Promise<void>((resolve) => {
          resolveReceived = resolve;
        });

        const consumer = await createConsumer(kafka, {
          topic: PING_TOPIC,
          groupId: 'eventforge.dev.ping-consumer-logging',
          logger: consumerLog.logger,
          handler: () => {
            resolveReceived();
            return Promise.resolve();
          },
        });

        try {
          await publish(producer, { topic: PING_TOPIC, key: envelope.aggregateId, value: envelope, logger: producerLog.logger });
          await received;

          const publishedLine = producerLog.lines().find((line) => line['correlationId'] === envelope.correlationId);
          expect(publishedLine).toMatchObject({
            msg: 'event published',
            topic: PING_TOPIC,
            eventId: envelope.eventId,
            eventType: 'DevPing',
            aggregateId: 'ping-2',
          });

          const consumedLine = consumerLog.lines().find((line) => line['correlationId'] === envelope.correlationId);
          expect(consumedLine).toMatchObject({
            msg: 'event consumed',
            topic: PING_TOPIC,
            eventId: envelope.eventId,
            eventType: 'DevPing',
            aggregateId: 'ping-2',
          });
        } finally {
          await consumer.disconnect();
        }
      }),
    60_000,
  );
});
