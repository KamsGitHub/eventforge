-- CreateTable
CREATE TABLE "outbox_events" (
    "id" TEXT NOT NULL,
    "aggregateId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "topic" TEXT NOT NULL,
    "messageKey" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "publishedAt" TIMESTAMP(3),
    "publishingAttempts" INTEGER NOT NULL DEFAULT 0,
    "lastError" TEXT,

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
-- Partial: only unpublished rows are ever scanned by the publisher's claim
-- query, so indexing published rows too would be pure waste that grows
-- forever. Prisma's schema DSL has no partial-index syntax, so this is
-- hand-written rather than generated from `@@index` in schema.prisma.
CREATE INDEX "outbox_events_createdAt_idx" ON "outbox_events"("createdAt") WHERE "publishedAt" IS NULL;
