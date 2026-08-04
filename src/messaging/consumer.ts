import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';

import type { Logger } from '@/shared/logger';

import { envelopeLogFields } from './envelope-log-fields';

/** What every handler actually receives: the raw KafkaJS payload plus a per-message child logger bound to that message's correlation fields, when a base `logger` was given to createConsumer. */
export type MessagePayloadWithLogger = EachMessagePayload & { readonly logger?: Logger };

export type MessageHandler = (payload: MessagePayloadWithLogger) => Promise<void>;

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
  /**
   * When given, every consumed message is logged once (topic, partition,
   * and the envelope's correlation fields), and `payload.logger` is a
   * child of this logger bound to the same fields — handlers use it for
   * any further logging so it stays attached to the message without every
   * handler having to re-derive it.
   */
  readonly logger?: Logger;
  /**
   * Passed straight through to KafkaJS's consumer constructor. Left
   * undefined (KafkaJS's own defaults) in every production call site;
   * exists so tests exercising real rebalance behavior can shorten the
   * broker's failure-detection window instead of waiting out the ~30s
   * default.
   */
  readonly sessionTimeout?: number;
  readonly heartbeatInterval?: number;
}

export async function createConsumer(kafka: Kafka, options: ConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({
    groupId: options.groupId,
    sessionTimeout: options.sessionTimeout,
    heartbeatInterval: options.heartbeatInterval,
  });

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
      let messageLogger: Logger | undefined;

      if (options.logger) {
        const body: unknown = JSON.parse(payload.message.value?.toString() ?? '{}');
        messageLogger = options.logger.child({
          topic: payload.topic,
          partition: payload.partition,
          ...envelopeLogFields(body),
        });
        messageLogger.info({}, 'event consumed');
      }

      await options.handler({ ...payload, logger: messageLogger });

      if (!autoCommit) {
        const { topic, partition, message } = payload;
        await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      }
    },
  });
  await joinedGroup;

  return consumer;
}
