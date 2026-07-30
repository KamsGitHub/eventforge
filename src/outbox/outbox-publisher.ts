import type { Producer } from 'kafkajs';

import type { PrismaClient } from '@/db/prisma-client';
import { publish } from '@/messaging/producer';

import { claimNextUnpublished, markFailed, markPublished } from './outbox.repository';
import type { OutboxEventRow } from './outbox.types';

const DEFAULT_POLL_INTERVAL_MS = 500;

export interface OutboxPublisherOptions {
  readonly prisma: PrismaClient;
  readonly producer: Producer;
  readonly pollIntervalMs?: number;
  /**
   * Invoked once per row after it's confirmed published, outside the claim
   * transaction (best-effort — a crash between here and the caller's own
   * write doesn't lose the event, only a downstream side effect of it).
   */
  readonly onPublished?: (row: OutboxEventRow) => Promise<void>;
}

interface ClaimResult {
  readonly row: OutboxEventRow;
  readonly failed: boolean;
}

/**
 * Polling outbox publisher: claims the oldest unpublished row (via
 * SELECT ... FOR UPDATE SKIP LOCKED, so multiple instances never claim the
 * same row), sends it to Kafka, and marks it published — all in one
 * transaction per row, so a lock is only held for one row's network
 * round trip rather than an entire batch.
 *
 * Guarantees at-least-once delivery, never exactly-once: if the process
 * dies after the Kafka send() acks but before the transaction commits, the
 * row is still unpublished on restart and gets sent again. Consumers must
 * be idempotent because of this, not despite it.
 */
export class OutboxPublisher {
  private timer: NodeJS.Timeout | undefined;
  private polling = false;

  constructor(private readonly options: OutboxPublisherOptions) {}

  start(): void {
    const intervalMs = this.options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.timer = setInterval(() => void this.pollOnce(), intervalMs);
  }

  stop(): void {
    clearInterval(this.timer);
    this.timer = undefined;
  }

  /**
   * Publishes every currently-unpublished row, one claim transaction at a
   * time, stopping as soon as a claim finds nothing left or a send fails
   * (that row is retried on the next poll rather than hammered in a tight
   * loop). Exposed directly so tests can drive it deterministically instead
   * of waiting on the timer.
   */
  async pollOnce(): Promise<number> {
    if (this.polling) {
      return 0;
    }

    this.polling = true;
    try {
      let published = 0;

      for (;;) {
        const result = await this.claimAndPublishOne();

        if (!result || result.failed) {
          break;
        }

        published += 1;
        await this.options.onPublished?.(result.row);
      }

      return published;
    } finally {
      this.polling = false;
    }
  }

  private async claimAndPublishOne(): Promise<ClaimResult | null> {
    return this.options.prisma.$transaction(async (tx) => {
      const row = await claimNextUnpublished(tx);

      if (!row) {
        return null;
      }

      try {
        await publish(this.options.producer, { topic: row.topic, key: row.messageKey, value: row.payload });
        await markPublished(tx, row.id);

        return { row, failed: false };
      } catch (error) {
        await markFailed(tx, row.id, error instanceof Error ? error.message : String(error));

        return { row, failed: true };
      }
    });
  }
}
