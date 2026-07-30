import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';

export type MessageHandler = (payload: EachMessagePayload) => Promise<void>;

export interface ConsumerOptions {
  readonly topic: string | readonly string[];
  readonly groupId: string;
  readonly handler: MessageHandler;
  readonly fromBeginning?: boolean;
  /**
   * Defaults to true (KafkaJS's own default). Idempotent consumers pass
   * false and rely on this function to commit each message's offset only
   * after `handler` resolves — so a crash between processing a message and
   * committing its offset always redelivers on restart rather than
   * silently skipping. See idempotent-consumer.ts for why redelivery is
   * safe: it's the whole reason autoCommit is turned off here.
   */
  readonly autoCommit?: boolean;
}

export async function createConsumer(kafka: Kafka, options: ConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId: options.groupId });

  const topics = typeof options.topic === 'string' ? [options.topic] : options.topic;
  const autoCommit = options.autoCommit ?? true;

  await consumer.connect();
  await consumer.subscribe({ topics: [...topics], fromBeginning: options.fromBeginning ?? false });

  // consumer.run() starts the fetch loop in the background and resolves
  // before the initial group join/partition assignment completes — a
  // message published right after this function returns can otherwise race
  // that rebalance and go undelivered until the next one. Waiting for the
  // group-join event means callers get back a consumer that's actually
  // ready to receive.
  const joinedGroup = new Promise<void>((resolve) => {
    consumer.on(consumer.events.GROUP_JOIN, () => resolve());
  });

  await consumer.run({
    autoCommit,
    eachMessage: async (payload) => {
      await options.handler(payload);

      if (!autoCommit) {
        const { topic, partition, message } = payload;
        await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      }
    },
  });
  await joinedGroup;

  return consumer;
}
