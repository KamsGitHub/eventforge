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
  await consumer.run({ eachMessage: options.handler });

  return consumer;
}
