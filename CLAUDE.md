# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

EventForge: a Kafka-based distributed job processing platform in TypeScript, built as a milestone-by-milestone portfolio project. It starts as a modular monolith and is incrementally evolved into independently deployable microservices. The full plan — every milestone's objective, architectural decisions, database/Kafka changes, tests, and completion criteria — lives in `docs/roadmap.html` (open it in a browser; it's a long, self-contained reference doc, not something to read via `cat`).

**Always check `docs/roadmap.html` for the current milestone's scope before adding features.** Each milestone explicitly states what should _not_ be built yet — respect that; don't pull forward work from a later milestone (e.g. don't wire real Kafka/Postgres logic before Milestone 6/9 says to) even if it seems convenient.

## Commands

```bash
npm run dev           # tsx watch src/server.ts — http://localhost:3000/health
npm run lint          # ESLint (flat config, type-aware)
npm run typecheck     # tsc --noEmit against tsconfig.json (src + tests)
npm run boundaries    # dependency-cruiser module-boundary check — see below
npm test              # Jest (all tests under tests/**/*.test.ts)
npm run build         # tsc -p tsconfig.build.json -> dist/ (src only, no tests)
npm start             # node dist/server.js (run npm run build first)
npm run format        # prettier --check .
npm run format:write   # prettier --write .
```

Run a single test file: `npx jest tests/smoke.test.ts`. Run tests matching a name: `npx jest -t "some test name"`.

CI (`.github/workflows/ci.yml`) runs lint → typecheck → boundaries → test on every push/PR against `main`, on Node 22. **`npm run boundaries` requires Node ^22||^24||>=26** (a `dependency-cruiser` constraint) — this already broke CI once when the workflow was pinned to Node 20; don't drop the Node version below that.

**`npm test` needs a working Docker daemon** — `tests/integration/postgres.smoke.test.ts` boots a real Postgres container via Testcontainers (no mocking Kafka/Postgres in tests meant to verify integration behavior, per the roadmap).

### Local infrastructure (Milestone 2)

`docker-compose.yml` at the repo root runs Postgres 16 and Kafka (`apache/kafka:4.3.1`, KRaft mode, no ZooKeeper) plus a one-off `kafka-init` job and an optional `kafka-ui`. Bring it up with `docker compose up -d postgres kafka && docker compose up kafka-init`. The 8 topics (`jobs.requested/started/completed/failed`, `jobs.retry-1/2/3`, `jobs.dead-letter`) are created explicitly by `infrastructure/kafka/create-topics.sh` — **never add reliance on broker auto-create**; if a new topic is needed, add it to that script's `TOPICS` array. Nothing in `src/` talks to this stack yet (starts at Milestone 4/6) — the compose stack existing does not mean it's wired into the app.

### App vs. server split (Milestone 3)

`src/app.ts` exports `buildApp(config?)`, which builds and returns a Fastify instance — it never calls `.listen()`. This is what lets tests use `app.inject()` without binding a port. `src/server.ts` is the only side-effecting entrypoint: it loads `.env` via `dotenv/config`, calls `buildApp()`, `.listen()`s, and wires `SIGINT`/`SIGTERM` to `app.close()` before `process.exit()`. **Any new route or plugin goes in `app.ts` (or a module's `api/`), never in `server.ts`.**

Config is loaded once via `loadConfig()` in `src/config/env.ts` (zod-validated, defaults for missing vars, throws on invalid ones) and attached to the Fastify instance as `app.config` — modules read `app.config`, they don't call `process.env` directly. Only `NODE_ENV`, `PORT`, and `LOG_LEVEL` are validated so far; extend `envSchema` when a milestone actually starts consuming a new var (e.g. `DATABASE_URL` at Milestone 4), rather than adding it speculatively.

The pino logger (`src/shared/logger.ts`) is passed into Fastify via the `loggerInstance` option (not `logger` — Fastify 5 uses `loggerInstance` specifically for a pre-built logger instance).

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

### Key design decisions to preserve as milestones land

- **PostgreSQL is authoritative for job state; Kafka only transports events.** The `jobs` module's own status consumer is the only writer of `Job.status` — `execution` (and later services) must never update the Job row directly, even before it's extracted into its own service. This is what makes the eventual service extraction a deployment change instead of a rewrite.
- **Event contracts (`src/contracts`, introduced at Milestone 6) are built ahead of need**, isolated from day one, because they become `packages/event-contracts` unchanged when the monorepo split happens at Milestone 18.
- **Two distinct "idempotency" mechanisms will exist and must not be conflated**: an HTTP `Idempotency-Key` header for duplicate job submissions (Milestone 5), and a Kafka `ProcessedEvent(eventId, consumerName)` table for duplicate event delivery (Milestone 10). Different tables, different concerns.
- **The outbox pattern guarantees at-least-once delivery, never exactly-once** — there's a real gap between a Kafka send succeeding and the outbox row being marked published. Consumers are idempotent _because_ of this, not despite it.
- Prisma has no first-class `SELECT ... FOR UPDATE SKIP LOCKED` — the outbox publisher's row-claiming needs `$queryRaw`.

### TypeScript config notes

- Path alias `@/*` maps to `./src/*` (see `tsconfig.json`); mirrored in `jest.config.js`'s `moduleNameMapper`.
- `moduleResolution: "Node16"` is paired with `module: "Node16"` deliberately (TypeScript 6 deprecated the old `"Node"`/`baseUrl` combination) — don't reintroduce `baseUrl` or revert to `moduleResolution: "Node"`.
- `tsconfig.build.json` extends the base config, restricts `include` to `src`, and excludes `**/*.test.ts` — that's the one used by `npm run build`; the base `tsconfig.json` (via `npm run typecheck`) covers both `src` and `tests`.
