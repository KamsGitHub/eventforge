export interface OutboxEventRow {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly topic: string;
  readonly messageKey: string;
  readonly payload: unknown;
  readonly createdAt: Date;
  readonly publishedAt: Date | null;
  readonly publishingAttempts: number;
  readonly lastError: string | null;
}

export interface NewOutboxEvent {
  readonly aggregateId: string;
  readonly eventType: string;
  readonly topic: string;
  readonly messageKey: string;
  readonly payload: unknown;
}
