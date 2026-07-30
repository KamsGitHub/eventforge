-- CreateTable
CREATE TABLE "processed_events" (
    "id" TEXT NOT NULL,
    "eventId" TEXT NOT NULL,
    "consumerName" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "processed_events_eventId_consumerName_key" ON "processed_events"("eventId", "consumerName");
