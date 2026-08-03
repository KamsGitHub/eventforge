import type { Kafka, Producer } from 'kafkajs';

import type { Logger } from '@/shared/logger';

import { envelopeLogFields } from './envelope-log-fields';

export function createProducer(kafka: Kafka): Producer {
  return kafka.producer({ idempotent: true });
}

export interface PublishOptions {
  readonly topic: string;
  /** Always the aggregateId (jobId), so all of one job's events land in the same partition. */
  readonly key: string;
  readonly value: unknown;
  /** When given, logs one line per publish with the envelope's correlation fields — see envelope-log-fields.ts. */
  readonly logger?: Logger;
}

export async function publish(producer: Producer, options: PublishOptions): Promise<void> {
  // acks defaults to -1 ("all") in kafkajs; an idempotent producer requires
  // exactly that, so it's left implicit rather than re-specified per call.
  await producer.send({
    topic: options.topic,
    messages: [{ key: options.key, value: JSON.stringify(options.value) }],
  });

  options.logger?.info({ topic: options.topic, ...envelopeLogFields(options.value) }, 'event published');
}
