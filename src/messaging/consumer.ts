import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';

export type MessageHandler = (payload: EachMessagePayload) => Promise<void>;

export interface ConsumerOptions {
  readonly topic: string;
  readonly groupId: string;
  readonly handler: MessageHandler;
  readonly fromBeginning?: boolean;
}

export async function createConsumer(kafka: Kafka, options: ConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId: options.groupId });

  await consumer.connect();
  await consumer.subscribe({ topic: options.topic, fromBeginning: options.fromBeginning ?? false });

  // consumer.run() starts the fetch loop in the background and resolves
  // before the initial group join/partition assignment completes — a
  // message published right after this function returns can otherwise race
  // that rebalance and go undelivered until the next one. Waiting for the
  // group-join event means callers get back a consumer that's actually
  // ready to receive.
  const joinedGroup = new Promise<void>((resolve) => {
    consumer.on(consumer.events.GROUP_JOIN, () => resolve());
  });

  await consumer.run({ eachMessage: options.handler });
  await joinedGroup;

  return consumer;
}
