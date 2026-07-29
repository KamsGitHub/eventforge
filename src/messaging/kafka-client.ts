import { Kafka, logLevel } from 'kafkajs';

export interface KafkaClientOptions {
  readonly brokers: readonly string[];
  readonly clientId: string;
}

export function createKafkaClient(options: KafkaClientOptions): Kafka {
  return new Kafka({
    clientId: options.clientId,
    brokers: [...options.brokers],
    logLevel: logLevel.NOTHING,
  });
}
