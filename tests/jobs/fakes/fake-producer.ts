import type { Producer, ProducerRecord, RecordMetadata } from 'kafkajs';

export interface FakeProducer {
  readonly producer: Producer;
  readonly send: jest.Mock<Promise<RecordMetadata[]>, [ProducerRecord]>;
}

/** Minimal Producer double — only `send` is exercised by publish(). */
export function createFakeProducer(): FakeProducer {
  const send = jest.fn<Promise<RecordMetadata[]>, [ProducerRecord]>().mockResolvedValue([]);

  return { producer: { send } as unknown as Producer, send };
}
