import type { Prisma, PrismaClient } from '@/db/prisma-client';

import type { NewOutboxEvent, OutboxEventRow } from './outbox.types';

type Client = PrismaClient | Prisma.TransactionClient;

function toCreateInput(event: NewOutboxEvent): Prisma.OutboxEventCreateInput {
  return {
    aggregateId: event.aggregateId,
    eventType: event.eventType,
    topic: event.topic,
    messageKey: event.messageKey,
    payload: event.payload as Prisma.InputJsonValue,
  };
}

export async function insertOutboxEvent(client: Client, event: NewOutboxEvent): Promise<void> {
  await client.outboxEvent.create({ data: toCreateInput(event) });
}

/**
 * Claims the oldest unpublished row for the caller's transaction via
 * SELECT ... FOR UPDATE SKIP LOCKED, so concurrent publisher instances never
 * claim the same row. Prisma has no first-class API for this, hence
 * $queryRaw. Must be called inside a transaction — the row lock (and thus
 * the claim) only lasts as long as `tx` is open.
 */
export async function claimNextUnpublished(tx: Prisma.TransactionClient): Promise<OutboxEventRow | null> {
  const rows = await tx.$queryRaw<OutboxEventRow[]>`
    SELECT
      id,
      "aggregateId",
      "eventType",
      topic,
      "messageKey",
      payload,
      "createdAt",
      "publishedAt",
      "publishingAttempts",
      "lastError"
    FROM outbox_events
    WHERE "publishedAt" IS NULL
    ORDER BY "createdAt" ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  `;

  return rows[0] ?? null;
}

export async function markPublished(tx: Prisma.TransactionClient, id: string): Promise<void> {
  await tx.outboxEvent.update({ where: { id }, data: { publishedAt: new Date() } });
}

export async function markFailed(tx: Prisma.TransactionClient, id: string, error: string): Promise<void> {
  await tx.outboxEvent.update({
    where: { id },
    data: { publishingAttempts: { increment: 1 }, lastError: error },
  });
}

export async function countUnpublished(client: Client): Promise<number> {
  return client.outboxEvent.count({ where: { publishedAt: null } });
}
