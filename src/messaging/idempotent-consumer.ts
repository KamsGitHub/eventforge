import { Prisma, type PrismaClient } from '@/db/prisma-client';

import type { MessageHandler, MessagePayloadWithLogger } from './consumer';

export interface IdempotentConsumerOptions {
  readonly prisma: PrismaClient;
  /** Paired with the event's eventId in the ProcessedEvent unique constraint — unique per logical consumer. */
  readonly consumerName: string;
}

/**
 * The business logic to run for a not-yet-processed event, given the raw
 * Kafka payload, the transaction it must use for any database writes (so
 * they commit atomically with the ProcessedEvent row), and the message body
 * pre-parsed as JSON (handlers still validate/narrow it with their own zod
 * schema).
 */
export type IdempotentApply = (payload: MessagePayloadWithLogger, tx: Prisma.TransactionClient, body: unknown) => Promise<void>;

/**
 * Wraps a business handler so redelivery of an already-applied event is a
 * safe no-op: insert-or-skip into ProcessedEvent(eventId, consumerName)
 * (unique constraint) inside a transaction, then run `apply` — if the
 * insert won the race — inside that same transaction, so the two either
 * commit together or neither does. Without that atomicity, a crash between
 * the two would either lose the business change (insert committed first)
 * or let it double-apply on redelivery (insert committed after).
 *
 * Pair with createConsumer's `autoCommit: false` — the Kafka offset must
 * only commit after this transaction does, or a crash before the DB commit
 * would let the offset advance past an event that never actually applied.
 */
export function withIdempotency(options: IdempotentConsumerOptions, apply: IdempotentApply): MessageHandler {
  return async (payload) => {
    const body: unknown = JSON.parse(payload.message.value?.toString() ?? '{}');
    const eventId = (body as { eventId?: unknown }).eventId;

    if (typeof eventId !== 'string' || eventId.length === 0) {
      throw new Error(`message on topic ${payload.topic} has no eventId; cannot apply idempotency`);
    }

    await options.prisma.$transaction(async (tx) => {
      const isNewEvent = await tryMarkProcessed(tx, eventId, options.consumerName);

      if (!isNewEvent) {
        return;
      }

      await apply(payload, tx, body);
    });
  };
}

/** Returns true if this call won the race and inserted the row, false if another delivery already had. */
async function tryMarkProcessed(tx: Prisma.TransactionClient, eventId: string, consumerName: string): Promise<boolean> {
  try {
    await tx.processedEvent.create({ data: { eventId, consumerName } });

    return true;
  } catch (error) {
    // P2002 = unique constraint violation on (eventId, consumerName) — a
    // concurrent redelivery (e.g. mid-rebalance) already won this race; the
    // DB serializes it, so losing here is a safe no-op, not a real error.
    if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
      return false;
    }

    throw error;
  }
}
