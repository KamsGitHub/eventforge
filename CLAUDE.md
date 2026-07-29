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

**`npm test` needs a working Docker daemon**: `tests/integration/postgres.smoke.test.ts` and `tests/integration/jobs/prisma-job.repository.test.ts` each boot a real, throwaway Postgres container via Testcontainers (no mocking Kafka/Postgres in tests meant to verify integration behavior, per the roadmap). The `test` script also sets `NODE_OPTIONS=--experimental-vm-modules` (via `cross-env`, for Windows/cross-shell portability) — required by Prisma 7's driver-adapter runtime, not optional; removing it breaks every test that touches a real `PrismaClient` with a cryptic "dynamic import callback was invoked without --experimental-vm-modules" error.

**`@/*` path aliases don't resolve at runtime in compiled output** — `tsc` alone leaves `require('@/app')` in `dist/`, which Node can't resolve. `npm run build` runs `tsc-alias` afterward specifically to rewrite those to relative `require()` paths; don't remove that step or add a new build path that skips it.

### Local infrastructure (Milestone 2)

`docker-compose.yml` at the repo root runs Postgres 16 and Kafka (`apache/kafka:4.3.1`, KRaft mode, no ZooKeeper) plus a one-off `kafka-init` job and an optional `kafka-ui`. Bring it up with `docker compose up -d postgres kafka && docker compose up kafka-init`. The 8 topics (`jobs.requested/started/completed/failed`, `jobs.retry-1/2/3`, `jobs.dead-letter`) are created explicitly by `infrastructure/kafka/create-topics.sh` — **never add reliance on broker auto-create**; if a new topic is needed, add it to that script's `TOPICS` array. Nothing in `src/` talks to Kafka yet (starts at Milestone 6) — the topics existing does not mean they're wired into the app.

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

`src/messaging/{kafka-client,producer,consumer}.ts` are thin KafkaJS wrappers, not a framework: `createKafkaClient(brokers)` builds the shared `Kafka` instance, `createProducer()` returns an idempotent producer (`idempotent: true` — KafkaJS then requires `acks: -1`, which is already its default, so it's left implicit rather than re-specified per call), and `createConsumer(kafka, { topic, groupId, handler })` connects, subscribes, and starts `run()` in one call, returning the `Consumer` for the caller to `disconnect()`. `KAFKA_BROKERS` (comma-separated) is now validated in `envSchema`, but **nothing wires this into `server.ts` yet** — that starts at Milestone 9 (outbox) / Milestone 10 (consumers). Don't add a Kafka connection to the composition root before then.

`src/contracts/envelope.ts` + `src/contracts/events/*.ts` are the event envelope and per-event zod schemas (`JobRequested/Started/Completed/Failed`, each schema-versioned as `...SchemaV1`), built now specifically so they're ready before the outbox (M9) and idempotent consumption (M10) need them — see the roadmap's cross-cutting "Event envelope" section for the exact wire shape this mirrors. **No module actually produces or consumes these yet** — `jobs` still leaves rows `PENDING` forever; wiring starts at M9.

**Testcontainers + Kafka gotcha, worth knowing before reaching for `@testcontainers/kafka` again**: its `KafkaContainer` only works with Confluent images (its KRaft bootstrap script targets `/etc/confluent/docker/*`) — it's incompatible with the `apache/kafka:4.3.1` image `docker-compose.yml` uses, so the foundation test in `tests/integration/messaging/kafka.foundation.test.ts` uses `confluentinc/cp-kafka:7.5.0` instead, purely to get a hermetic broker. Also, its **default wait strategy only waits for the TCP port to accept connections**, which fires while the JVM is still initializing — connecting that early gets an immediate `ECONNRESET` on the first real request. Fixed with an explicit `.withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))` so `.start()` doesn't resolve until the broker can actually serve requests.

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
