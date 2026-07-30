# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

EventForge: a Kafka-based distributed job processing platform in TypeScript, built as a milestone-by-milestone portfolio project. It starts as a modular monolith and is incrementally evolved into independently deployable microservices. The full plan — every milestone's objective, architectural decisions, database/Kafka changes, tests, and completion criteria — lives in `docs/roadmap.html` (open it in a browser; it's a long, self-contained reference doc, not something to read via `cat`).

**Always check `docs/roadmap.html` for the current milestone's scope before adding features.** Each milestone explicitly states what should _not_ be built yet — respect that; don't pull forward work from a later milestone (e.g. don't wire real Kafka/Postgres logic before Milestone 6/9 says to) even if it seems convenient.

## Commands

```bash
npx prisma generate    # regenerate the Prisma client into generated/ (gitignored; run after clone/schema change)
npm run dev            # tsx watch src/server.ts — http://localhost:3000/health
npm run lint           # ESLint (flat config, type-aware)
npm run typecheck      # tsc --noEmit against tsconfig.json (src + tests + prisma.config.ts)
npm run boundaries     # dependency-cruiser module-boundary check — see below
npm test               # Jest (all tests under tests/**/*.test.ts)
npm run build          # tsc -p tsconfig.build.json, then tsc-alias rewrites @/ aliases in dist/
npm start              # node dist/src/server.js (run npm run build first)
npm run format         # prettier --check .
npm run format:write   # prettier --write .
```

Run a single test file: `npx jest tests/smoke.test.ts`. Run tests matching a name: `npx jest -t "some test name"`.

CI (`.github/workflows/ci.yml`) runs lint → typecheck → boundaries → test on every push/PR against `main`, on Node 22. **`npm run boundaries` requires Node ^22||^24||>=26** (a `dependency-cruiser` constraint) — this already broke CI once when the workflow was pinned to Node 20; don't drop the Node version below that.

**`npm test` needs a working Docker daemon**: `tests/integration/postgres.smoke.test.ts`, `tests/integration/jobs/prisma-job.repository.test.ts`, and (since Milestone 6/7) two Kafka suites under `tests/integration/messaging/` and `tests/integration/execution/` each boot a real, throwaway Postgres or Kafka container via Testcontainers (no mocking Kafka/Postgres in tests meant to verify integration behavior, per the roadmap). The `test` script also sets `NODE_OPTIONS=--experimental-vm-modules` (via `cross-env`, for Windows/cross-shell portability) — required by Prisma 7's driver-adapter runtime, not optional; removing it breaks every test that touches a real `PrismaClient` with a cryptic "dynamic import callback was invoked without --experimental-vm-modules" error.

`jest.config.js` pins `maxWorkers: process.env.CI ? 1 : 2`: with several of these suites now booting a real JVM-based broker or database, Jest's default worker count (`cpus - 1`) runs enough of them concurrently to starve each container of CPU during startup — measured as most of the Testcontainers suites failing/timing out in the same run. Capping at 2 was measurably both faster and more reliable than the default _and_ than full serialization (`maxWorkers: 1`) in testing **on a 4-core dev machine**. GitHub's hosted CI runner only has 2 vCPUs, and by Milestone 9 there are enough Testcontainers suites that 2 concurrent workers there starves whichever broker starts last — CI runs fully serialized (`maxWorkers: 1`) instead; this is a case where the "faster than full serialization" finding from local testing doesn't transfer to a smaller box, not a contradiction of it. `src/messaging/consumer.ts`'s `createConsumer()` also waits for the KafkaJS `GROUP_JOIN` event before returning — `consumer.run()` alone resolves before the initial rebalance/partition assignment completes, so a message published right after used to be able to race it and go undelivered until the next rebalance. Even with all of this, on a sufficiently loaded machine these suites can occasionally still time out (120s per `it()`, already generous) — rerun before assuming a real regression; confirm by running the file alone (`npx jest <path> --runInBand`), which should reliably finish in 15–30s.

**`@/*` path aliases don't resolve at runtime in compiled output** — `tsc` alone leaves `require('@/app')` in `dist/`, which Node can't resolve. `npm run build` runs `tsc-alias` afterward specifically to rewrite those to relative `require()` paths; don't remove that step or add a new build path that skips it.

### Local infrastructure (Milestone 2)

`docker-compose.yml` at the repo root runs Postgres 16 and Kafka (`apache/kafka:4.3.1`, KRaft mode, no ZooKeeper) plus a one-off `kafka-init` job and an optional `kafka-ui`. Bring it up with `docker compose up -d postgres kafka && docker compose up kafka-init`. The 8 topics (`jobs.requested/started/completed/failed`, `jobs.retry-1/2/3`, `jobs.dead-letter`) are created explicitly by `infrastructure/kafka/create-topics.sh` — **never add reliance on broker auto-create**; if a new topic is needed, add it to that script's `TOPICS` array. `src/` started actually consuming/producing at Milestone 7 (see below) — before that, the topics existing didn't mean they were wired into the app.

### App vs. server split (Milestone 3)

`src/app.ts` exports `buildApp(deps)`, which builds and returns a Fastify instance — it never calls `.listen()`. This is what lets tests use `app.inject()` without binding a port. `src/server.ts` is the only side-effecting entrypoint: it loads `.env` via `dotenv/config`, calls `buildApp()`, `.listen()`s, and wires `SIGINT`/`SIGTERM` to `app.close()` before `process.exit()`. **Any new route or plugin goes in `app.ts` (or a module's `api/`), never in `server.ts`.**

Config is loaded once via `loadConfig()` in `src/config/env.ts` (zod-validated, defaults for missing vars, throws on invalid ones) and attached to the Fastify instance as `app.config` — modules read `app.config`, they don't call `process.env` directly. `NODE_ENV`, `PORT`, `LOG_LEVEL`, and (since Milestone 4) `DATABASE_URL` are validated; extend `envSchema` when a milestone actually starts consuming a new var (e.g. `KAFKA_BROKERS` at Milestone 6), rather than adding it speculatively.

The pino logger (`src/shared/logger.ts`) is passed into Fastify via the `loggerInstance` option (not `logger` — Fastify 5 uses `loggerInstance` specifically for a pre-built logger instance).

### Prisma / database (Milestone 4)

This project is on **Prisma 7**, which changed enough that assumptions from Prisma 4–6 don't hold:

- **Driver adapters are mandatory.** `datasource.url` in `prisma/schema.prisma` is gone entirely (`url = env(...)` is now a hard schema-validation error) — the connection string lives in `prisma.config.ts` (`datasource.url: process.env['DATABASE_URL']`), and `PrismaClient` requires an `adapter` (we use `@prisma/adapter-pg`'s `PrismaPg`). There is no "classic" no-adapter mode to fall back to.
- **The generator is `prisma-client` (not `prisma-client-js`)**, outputting real `.ts` source — not a prebuilt package — to `generated/prisma/` (gitignored; run `npx prisma generate` after cloning or changing the schema). We pinned `moduleFormat = "cjs"` in the generator block in `prisma/schema.prisma`; without it, the generator emits ESM (`import.meta.url`), which breaks under our CommonJS/ts-jest setup.
- **`src/db/prisma-client.ts` vs `src/db/prisma.ts` is a deliberate split, not duplication.** `prisma-client.ts` is side-effect-free: it just re-exports `PrismaClient`, `Prisma`, `PrismaJobRow`, and a `createPrismaClient(url?)` factory. `prisma.ts` is the actual singleton: `export const prisma = createPrismaClient()` runs `loadConfig()` at module load time, which throws without a live `DATABASE_URL`. **Anything that only needs the client type/factory (tests constructing their own client, e.g. against a Testcontainers database) must import from `prisma-client.ts`, not `prisma.ts`** — importing the latter for its types would still eagerly execute the singleton line and crash without `DATABASE_URL` set.
- Jest's `moduleNameMapper` strips a trailing `.js` from relative imports (`^(\.{1,2}/.*)\.js$`) because the generated client uses NodeNext-style `./foo.js` imports against `.ts` source files with no compiled `.js` on disk — without the mapping, Jest can't resolve them.
- `.dependency-cruiser.cjs`'s `doNotFollow` excludes `^generated` (alongside `node_modules`) — otherwise the boundary/circular check cruises into the generated client's own internal circular references, which aren't ours to fix.

### Job persistence (Milestone 4)

`modules/jobs/domain/job.entity.ts` is the `Job` aggregate: immutable (`transitionTo()` returns a new `Job`, never mutates), with the state machine's legal transitions as its only enforcement point (throws `IllegalJobStateTransitionError` from `domain/errors.ts` otherwise). `JobStatus` lives in its own `domain/job-status.ts` file specifically to avoid a circular import between `job.entity.ts` and `errors.ts` (both needed the type; extracting it broke the cycle instead of one importing from the other).

`modules/jobs/infrastructure/prisma-job.repository.ts` is **the only file in the jobs module allowed to import the Prisma client** — this is the seam the execution-service extraction (Milestone 18) cuts along. Optimistic concurrency: `update()` does `updateMany({ where: { id, version }, data: { ..., version: { increment: 1 } } })` and throws `JobVersionConflictError` if `count === 0` — it does not trust the entity to have bumped its own version; the entity's `transitionTo()` never touches `version` at all, by design (version is purely a persistence-layer concern, closer to how JPA's `@Version` works than something the domain reasons about).

### Job API and composition root (Milestone 5)

`src/app.ts` no longer constructs its own dependencies — `buildApp({ config?, jobService })` requires a `JobService` to be passed in. **`server.ts` is the composition root**: it's the only place that imports the real `prisma` singleton and `PrismaJobRepository`, wires them into a `JobService`, and hands that to `buildApp()`. This keeps `app.ts` free of any eager Prisma singleton import (importing `@/db/prisma` anywhere triggers its `loadConfig()` call — see above) and keeps tests able to pass an `InMemoryJobRepository`-backed `JobService` instead. **If you add a new module's routes, follow the same pattern**: the module's `api/routes.ts` takes a service instance as a parameter; `server.ts` wires the real one, tests wire a fake one — `app.ts` itself stays ignorant of which.

Request validation/serialization goes through `fastify-type-provider-zod` (`validatorCompiler`/`serializerCompiler` set once in `app.ts`, `.withTypeProvider<ZodTypeProvider>()` on the instance) — route schemas are the same zod schemas from `modules/jobs/api/schemas.ts`, reused rather than re-declared, so they're ready to feed an OpenAPI generator later. **`modules/jobs/api/routes.ts` has a `FastifyBaseLogger`-vs-concrete-pino-instance generic mismatch** (same root cause as the `app.ts` return-type issue from Milestone 3, now surfacing at the `registerJobRoutes` call site instead) — worked around with a local `AppInstance` type alias that sets the Logger generic to `any` deliberately (documented inline). Don't try to make it structurally exact; it's a known Fastify+custom-logger+generic-method variance quirk, not a bug in our code.

`PrismaJobRepository.create()` translates Prisma's `P2002` unique-constraint error (on `idempotencyKey`) into a domain-level `DuplicateJobSubmissionError` — `JobService.createJob()` catches that and returns the existing job via `findByIdempotencyKey()` instead of erroring. This is the HTTP-level duplicate-submission guard (client-supplied `Idempotency-Key` header) — keep it separate from the Kafka `ProcessedEvent` idempotency table arriving at Milestone 10; different mechanism, different table.

### Kafka producer/consumer foundation and event contracts (Milestone 6)

`src/messaging/{kafka-client,producer,consumer}.ts` are thin KafkaJS wrappers, not a framework: `createKafkaClient({ brokers, clientId })` builds the shared `Kafka` instance, `createProducer()` returns an idempotent producer (`idempotent: true` — KafkaJS then requires `acks: -1`, which is already its default, so it's left implicit rather than re-specified per call), and `createConsumer(kafka, { topic, groupId, handler })` connects, subscribes, and starts `run()` in one call, returning the `Consumer` for the caller to `disconnect()`. `KAFKA_BROKERS`/`KAFKA_CLIENT_ID` are validated in `envSchema`.

`src/contracts/envelope.ts` + `src/contracts/events/*.ts` are the event envelope and per-event zod schemas (`JobRequested/Started/Completed/Failed`, each schema-versioned as `...SchemaV1`), built now specifically so they're ready before the outbox (M9) and idempotent consumption (M10) need them — see the roadmap's cross-cutting "Event envelope" section for the exact wire shape this mirrors.

**Testcontainers + Kafka gotcha, worth knowing before reaching for `@testcontainers/kafka` again**: its `KafkaContainer` only works with Confluent images (its KRaft bootstrap script targets `/etc/confluent/docker/*`) — it's incompatible with the `apache/kafka:4.3.1` image `docker-compose.yml` uses, so Kafka integration tests use `confluentinc/cp-kafka:7.5.0` instead, purely to get a hermetic broker. Also, its **default wait strategy only waits for the TCP port to accept connections**, which fires while the JVM is still initializing — connecting that early gets an immediate `ECONNRESET` on the first real request. Fixed with an explicit `.withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))` so `.start()` doesn't resolve until the broker can actually serve requests. (See the `npm test`/`maxWorkers` note above for the other Testcontainers gotcha this milestone surfaced.)

### Basic asynchronous job execution (Milestone 7)

The `execution` module's first real Kafka wiring: `modules/execution/domain/job-handler.port.ts` defines `JobHandler.execute(payload): Promise<unknown>`; `modules/execution/application/execute-job.service.ts` is a registry (`ReadonlyMap<jobType, JobHandler>`) that throws `UnknownJobTypeError` for an unregistered type; `modules/execution/infrastructure/jobs-requested.consumer.ts` consumes `jobs.requested`, calls the registry, and publishes `JobCompleted` (success) or `JobFailed` (any thrown error, `attempt` hardcoded to `1` — no retries until later) directly to Kafka — **no outbox on this outbound side yet, that's M9's concern, and it never touches the Job row** (see the cross-cutting rule: execution only ever publishes events). `GenerateReportHandler` (`domain/handlers/generate-report.handler.ts`) is the one registered handler so far; it zod-validates `{ reportName: string }` and rejects (deliberately, to give the failure path something real to exercise) when that's missing or the wrong type.

**`server.ts` now constructs the first real `Kafka`/`Producer`/`Consumer` instances** — a `createKafkaClient` from `app.config.kafkaBrokers`/`kafkaClientId`, a producer connected at startup, and `startJobsRequestedConsumer(...)` — and disconnects both in `shutdown()` before `app.close()`. This corrects an earlier (wrong) assumption in this file that Kafka wiring in the composition root starts at M9/M10: the outbox (M9) and consumer idempotency (M10) are refinements of flows that go live here and at M8, not the first time a connection exists. **`JobService.createJob()` still doesn't publish anything** — jobs created via the HTTP API stay `PENDING` forever until M8 wires `JobService.create` to actually produce `JobRequested`; only a manually-published `JobRequested` event (e.g. from a test or script) flows through today.

### Job lifecycle events and status updates (Milestone 8)

Closes the loop: the full `jobs.requested → jobs.started → jobs.completed|failed` workflow now runs end-to-end, at-most-once (no outbox, no consumer idempotency yet — those are M9/M10). Two things now happen that didn't before:

- **`JobService.createJob()` really publishes.** After the row is inserted `PENDING`, it builds a `JobRequested` envelope and does a **direct `producer.send`** (via the shared `publish()` helper) — still no outbox, exactly the gap M9 closes — then immediately transitions the job to `QUEUED` and persists that via `JobRepository.update()`. `JobService`'s constructor now takes a `Producer` as a second argument alongside the repository. **Superseded at M9** — see below: the direct `producer.send` and the immediate `QUEUED` transition are both replaced by the outbox.
- **Execution publishes `JobStarted`** in `jobs-requested.consumer.ts`, unconditionally, before invoking the handler (not inside the try/catch that produces `JobCompleted`/`JobFailed` — a start notification isn't a handler outcome).

**`modules/jobs/infrastructure/job-status.consumer.ts`** is the jobs module's own status consumer — the _only_ writer of `Job.status`, per the cross-cutting rule below. It subscribes to `jobs.started`/`jobs.completed`/`jobs.failed` on one consumer group and calls `Job.transitionTo()` accordingly (`RUNNING` / `SUCCEEDED` with `result` / `FAILED` with `error`). This needed `createConsumer()` (`src/messaging/consumer.ts`) to accept `topic: string | readonly string[]` — previously single-topic-only — since one consumer group here legitimately needs three topics, not three separate consumer groups each independently rebalancing.

**Not yet, deliberately**: redelivery of an already-applied `jobs.started`/`completed`/`failed` event throws `IllegalJobStateTransitionError` (e.g. re-consuming `jobs.started` after the job is already `RUNNING`) rather than being ignored — this is the "duplicate events currently double-apply" gap the roadmap calls out, fixed by consumer idempotency at M10, not worked around here.

`tests/integration/e2e/job-lifecycle.e2e.test.ts` is the first true end-to-end test: real Postgres + real Kafka (both via Testcontainers), the full `buildApp()`, both consumers, and both the execution module's handler and the jobs module's status consumer wired together — POSTs a job via `app.inject()`, polls GET until terminal, and asserts the final state/timestamps for both the success and failure paths.

### Transactional outbox (Milestone 9)

Removes the M8 direct `producer.send` from job creation — that was the exact "crash between insert and publish loses the event" gap this milestone closes. `JobService` drops its `Producer` dependency entirely and goes back to `constructor(jobs: JobRepository)`.

- **`OutboxEvent` model** (`prisma/schema.prisma`, table `outbox_events`): `id, aggregateId, eventType, topic, messageKey, payload (jsonb), createdAt, publishedAt (nullable), publishingAttempts, lastError`. The unpublished-rows index is **hand-written raw SQL in the migration** (`WHERE "publishedAt" IS NULL`) — Prisma's schema DSL has no partial-index syntax, so `schema.prisma` only declares a plain `@@index([createdAt])` to keep `prisma migrate dev` from treating the hand-added `WHERE` as drift it should revert; expect to re-edit that clause back in if a future `prisma migrate dev` regenerates this index.
- **`JobService.createJob()` now calls `JobRepository.createWithOutboxEvent(job, outboxEvent)`** instead of `create()` — `PrismaJobRepository`'s implementation wraps `tx.job.create` + the shared `insertOutboxEvent(tx, event)` (from `src/outbox/outbox.repository.ts`) in one `prisma.$transaction`. The job stays `PENDING`; nothing transitions it to `QUEUED` at creation time anymore (see below).
- **`src/outbox/`** is deliberately module-agnostic (like `src/messaging/`), not jobs-specific, so other modules can grow their own outbox use later without teaching this code what a "Job" is:
  - `outbox.repository.ts`: `insertOutboxEvent` (works against either a plain `PrismaClient` or a `Prisma.TransactionClient`), and `claimNextUnpublished(tx)` — `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` via `$queryRaw`, since Prisma has no first-class API for row-claiming. Must be called inside a transaction; the claim only lasts as long as `tx` stays open.
  - `outbox-publisher.ts`: `OutboxPublisher` polls on an interval (`start()`/`stop()`), claiming+sending+marking **one row per transaction** (not a whole batch in one transaction — a lock is only held for one row's Kafka round trip). `pollOnce()` is exposed directly so tests can drive it deterministically instead of waiting on the timer; it drains the full unpublished backlog but stops immediately on a send failure (that row's `publishingAttempts`/`lastError` are still committed) rather than tight-looping retries with no backoff.
- **The `PENDING -> QUEUED` transition now happens when the publisher confirms delivery, not at creation.** `OutboxPublisher` takes an optional `onPublished(row)` hook, invoked after a row's `publishedAt` commits (outside that transaction — this hand-off is best-effort, not atomic with the commit). `modules/jobs/infrastructure/job-outbox-published.handler.ts` supplies the jobs-specific implementation (`createJobOutboxPublishedHandler(jobRepository)`): for a `JobRequested` row, looks up the job by `aggregateId` and transitions `PENDING -> QUEUED` through the repository — keeping the "jobs module's own repository is the only writer of `Job.status`" rule intact even though the publisher itself lives outside `modules/jobs`. `server.ts` wires this hook and starts/stops the publisher alongside the two consumers.
- **At-least-once, never exactly-once, by design**: if the process dies after Kafka's `send()` acks but before the transaction commits, the row is still unpublished on restart and gets sent again — proven directly in `tests/integration/outbox/outbox-publisher.test.ts` by claiming a row, sending it for real, then throwing inside the transaction instead of letting it commit (simulating the crash without needing to kill the process), then running a real publisher and asserting two deliveries landed on the topic. The same file also proves two concurrent `OutboxPublisher` instances against the same Postgres never claim (and therefore never send) the same row twice.

## Architecture

### Module boundary rule (the load-bearing constraint)

`src/modules/{jobs,execution,notifications,audit}/` each have an internal `api/ → application/ → domain/ → infrastructure/` slice. **A module may only be reached through its own `api/` folder (or through Kafka events) — never by importing another module's `application/`, `domain/`, or `infrastructure/` directly.** `shared/` may not depend on any module.

This is enforced mechanically by `.dependency-cruiser.cjs` (`npm run boundaries`), not just by convention — it will fail the build on a violation, including in CI. The reason this rule exists: the roadmap's whole arc is extracting `execution` (then `notifications`, then `audit`) into standalone services later without a rewrite. Every cross-module interaction has to already be Kafka-events-only or api-surface-only, from the very first line of code, or the later extraction turns into a rewrite. If you're about to import from `../../other-module/domain/...`, that's the boundary rule stopping you — the fix is to go through Kafka or that module's `api/`, not to add an exception to the dependency-cruiser config.

`shared/` is for cross-cutting technical code only (logger, generic result types, etc.) — never business logic. Business logic belongs inside a module.

### Target folder layout

```
src/
├── app.ts         # buildApp() — pure, testable, no listen()
├── server.ts      # entrypoint: listen() + graceful shutdown
├── config/        # env/config loading
├── db/            # database wiring
├── messaging/     # Kafka producer/consumer wrapper
├── outbox/        # transactional outbox publisher
├── shared/        # cross-cutting technical utilities only
└── modules/
    ├── jobs/           # job management: API, state machine, persistence (authoritative for Job status)
    ├── execution/      # job execution: handler registry, Kafka consumers (never writes Job status directly)
    ├── notifications/  # job-completion notifications
    └── audit/          # append-only audit trail
```

`prisma/schema.prisma` + `prisma/migrations/` live at the repo root (not under `src/`); `generated/prisma/` (the generated client) is gitignored.

### Key design decisions to preserve as milestones land

- **PostgreSQL is authoritative for job state; Kafka only transports events.** The `jobs` module's own status consumer is the only writer of `Job.status` — `execution` (and later services) must never update the Job row directly, even before it's extracted into its own service. This is what makes the eventual service extraction a deployment change instead of a rewrite.
- **Event contracts (`src/contracts`, introduced at Milestone 6) are built ahead of need**, isolated from day one, because they become `packages/event-contracts` unchanged when the monorepo split happens at Milestone 18.
- **Two distinct "idempotency" mechanisms exist (or will) and must not be conflated**: an HTTP `Idempotency-Key` header for duplicate job submissions (delivered, Milestone 5 — see `DuplicateJobSubmissionError`), and a Kafka `ProcessedEvent(eventId, consumerName)` table for duplicate event delivery (Milestone 10). Different tables, different concerns.
- **The outbox pattern guarantees at-least-once delivery, never exactly-once** — there's a real gap between a Kafka send succeeding and the outbox row being marked published. Consumers are idempotent _because_ of this, not despite it.
- Prisma has no first-class `SELECT ... FOR UPDATE SKIP LOCKED` — the outbox publisher's row-claiming needs `$queryRaw`.

### TypeScript config notes

- Path alias `@/*` maps to `./src/*` (see `tsconfig.json`); mirrored in `jest.config.js`'s `moduleNameMapper`.
- `moduleResolution: "Node16"` is paired with `module: "Node16"` deliberately (TypeScript 6 deprecated the old `"Node"`/`baseUrl` combination) — don't reintroduce `baseUrl` or revert to `moduleResolution: "Node"`.
- `tsconfig.build.json` extends the base config, restricts `include` to `src`, and excludes `**/*.test.ts` — that's the one used by `npm run build`; the base `tsconfig.json` (via `npm run typecheck`) covers both `src` and `tests`.
