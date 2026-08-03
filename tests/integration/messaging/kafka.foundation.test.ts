import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { Wait } from 'testcontainers';
import { z } from 'zod';

import { createEnvelope, envelopeSchema } from '@/contracts/envelope';
import { createConsumer } from '@/messaging/consumer';
import { createKafkaClient } from '@/messaging/kafka-client';
import { createProducer, publish } from '@/messaging/producer';

import { createCapturingLogger } from '../../helpers/capturing-logger';

const PING_TOPIC = '_dev.ping';
const pingEnvelopeSchema = envelopeSchema(z.object({ message: z.string() }));

describe('Kafka producer/consumer foundation (real broker via Testcontainers)', () => {
  // Testcontainers' KafkaContainer wrapper only supports Confluent images
  // (its Kraft bootstrap dance targets /etc/confluent/docker/*), unlike the
  // apache/kafka image used by local docker-compose — a Confluent image is
  // used here purely to get a hermetic broker for this test.
  let container: StartedKafkaContainer;
  let kafka: Kafka;
  let producer: Producer;
  let consumer: Consumer | undefined;

  beforeAll(async () => {
    container = await new KafkaContainer('confluentinc/cp-kafka:7.5.0')
      .withKraft()
      // The library's default wait strategy only waits for the port to
      // accept TCP connections, which fires while the broker is still
      // initializing and resets any real request sent that early — wait for
      // its own "started" log line instead so it's actually ready to serve.
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();
    kafka = createKafkaClient({
      brokers: [`${container.getHost()}:${container.getMappedPort(9093)}`],
      clientId: 'eventforge-test',
    });

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: PING_TOPIC, numPartitions: 1, replicationFactor: 1 }] });
    await admin.disconnect();

    producer = createProducer(kafka);
    await producer.connect();
  }, 120_000);

  afterAll(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
    await container?.stop();
  }, 30_000);

  it('produces and consumes a real message end-to-end, round-tripping the envelope through zod', async () => {
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

    consumer = await createConsumer(kafka, {
      topic: PING_TOPIC,
      groupId: 'eventforge.dev.ping-consumer',
      handler: (payload) => {
        resolveReceived(payload);
        return Promise.resolve();
      },
    });

    await publish(producer, { topic: PING_TOPIC, key: envelope.aggregateId, value: envelope });

    const message = await received;
    const parsed = pingEnvelopeSchema.parse(JSON.parse(message.message.value?.toString() ?? '{}'));

    expect(parsed.eventType).toBe('DevPing');
    expect(parsed.aggregateId).toBe('ping-1');
    expect(parsed.payload).toEqual({ message: 'hello' });

    await consumer.disconnect();
    consumer = undefined;
  }, 120_000);

  it('logs correlation fields automatically on publish and on consume, without the caller doing any logging itself (Milestone 13)', async () => {
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

    consumer = await createConsumer(kafka, {
      topic: PING_TOPIC,
      groupId: 'eventforge.dev.ping-consumer-logging',
      logger: consumerLog.logger,
      handler: () => {
        resolveReceived();
        return Promise.resolve();
      },
    });

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

    await consumer.disconnect();
    consumer = undefined;
  }, 120_000);
});
