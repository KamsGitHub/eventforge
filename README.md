# EventForge

A Kafka-based distributed job processing platform, written entirely in TypeScript. It starts as a well-structured modular monolith and is incrementally evolved into independently deployable microservices — demonstrating clean architecture, reliability patterns (transactional outbox, idempotent consumers, retries/DLQ), observability, and testing along the way.

**Status:** early stage. Milestones 1–5 of 24 complete — repo scaffold, local Postgres + Kafka, a running Fastify server, Prisma-backed Job persistence, and a working `POST/GET /api/jobs` API with duplicate-submission protection. No Kafka wiring yet — jobs created via the API stay `PENDING` forever for now; that starts at Milestone 6.

The full build plan — every milestone's objective, architecture decisions, database/Kafka changes, tests, and completion criteria — lives in [`docs/roadmap.html`](docs/roadmap.html) (open it in a browser).

## Stack

Node.js · TypeScript · Fastify · PostgreSQL · Prisma · Apache Kafka (KafkaJS) · Zod · Docker Compose · Jest · Testcontainers · Pino · Prometheus/Grafana · React

## Architecture

A modular monolith, structured so each business module can later be extracted into its own service without a rewrite:

```
src/
├── app.ts              # builds the Fastify instance (pure, testable — no listen())
├── server.ts           # entrypoint: listens, wires graceful shutdown
├── config/             # zod-validated environment loading
├── db/                 # Prisma client wiring (prisma.ts = singleton, prisma-client.ts = side-effect-free re-export)
├── messaging/          # Kafka producer/consumer wrapper (Milestone 6)
├── outbox/             # transactional outbox publisher (Milestone 9)
├── shared/             # cross-cutting technical utilities only — no business logic
└── modules/
    ├── jobs/            # job management: API, state machine, persistence
    │   ├── api/           # Fastify routes + zod request/response schemas
    │   ├── application/   # JobService — orchestrates domain + repository
    │   ├── domain/        # Job entity + transitionTo() state machine, JobRepository port, domain errors
    │   └── infrastructure/  # PrismaJobRepository — the only file here allowed to import the Prisma client
    ├── execution/       # job execution: handler registry, Kafka consumers
    ├── notifications/   # job-completion notifications
    └── audit/           # append-only audit trail
```

`prisma/schema.prisma` defines the `Job` model (state, payload/result as JSONB, optimistic-concurrency `version` column, unique `idempotencyKey`). Migrations live in `prisma/migrations/`; the generated client is emitted to `generated/prisma/` (gitignored — run `npx prisma generate` after cloning).

Each module owns its own `api → application → domain → infrastructure` slice. A module may only be reached through its `api/` folder or through Kafka events — never by reaching into another module's `application`, `domain`, or `infrastructure` layers directly. This boundary is enforced by [`dependency-cruiser`](.dependency-cruiser.cjs), not just convention: `npm run boundaries` fails the build on a violation.

## API

```
POST   /api/jobs              create a job
GET    /api/jobs               list jobs (?status=, ?limit=, ?offset=)
GET    /api/jobs/:jobId        get a job by id
```

`POST /api/jobs` takes `{ "type": string, "payload": object }` and an optional `Idempotency-Key` header. Replaying the same key returns the original job (same `id`, same `payload`) with `201` again — no second row is created; this is an HTTP-level duplicate-submission guard, distinct from the Kafka consumer idempotency that arrives at Milestone 10. Jobs are created `PENDING` and stay there — nothing consumes them yet, since Kafka isn't wired up until Milestone 6/9. `correlationId` is generated server-side per job; clients don't set it.

## Getting started

```bash
npm install
cp .env.example .env  # DATABASE_URL is required — config fails fast without it
npx prisma generate    # generates the Prisma client into generated/ (gitignored)
npm run dev            # tsx watch src/server.ts — http://localhost:3000/health
npm run lint           # ESLint
npm run typecheck      # tsc --noEmit
npm run boundaries     # module-boundary check (dependency-cruiser)
npm test               # Jest
npm run build          # compile to dist/ and rewrite @/ aliases via tsc-alias
npm start              # node dist/src/server.js (after npm run build)
```

All four checks (lint, typecheck, boundaries, test) run in CI on every push and pull request — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml). The test suite includes Testcontainers-backed integration tests (a Postgres smoke test and the full `PrismaJobRepository` suite, each against a fresh, throwaway Postgres container with migrations applied), so `npm test` needs a working Docker daemon. It also needs `NODE_OPTIONS=--experimental-vm-modules` (wired into the `test` script already) — a real Prisma 7 / driver-adapter requirement, not optional.

### Local infrastructure

```bash
docker compose up -d postgres kafka   # start Postgres + Kafka (KRaft, single node), wait for healthy
docker compose up kafka-init          # idempotent: creates the 8 topics if they don't already exist
docker compose up -d kafka-ui         # optional web UI at http://localhost:8080
```

| Topic                        | Purpose                                         |
| ---------------------------- | ----------------------------------------------- |
| `jobs.requested`             | A job has been accepted and is ready to execute |
| `jobs.started`               | Execution has picked up a job                   |
| `jobs.completed`             | A job finished successfully                     |
| `jobs.failed`                | An attempt failed                               |
| `jobs.retry-1` / `-2` / `-3` | Tiered retry backoff before giving up           |
| `jobs.dead-letter`           | Retries exhausted; needs manual intervention    |

All topics are created explicitly by `infrastructure/kafka/create-topics.sh` (3 partitions, replication factor 1) — broker auto-create is intentionally never relied on. Postgres credentials match `.env.example`'s `DATABASE_URL`, now read by `src/config/env.ts` and used by `prisma.config.ts`/`src/db/prisma.ts`. `KAFKA_BROKERS`/`KAFKA_CLIENT_ID` aren't read by application code yet (Milestone 6).

Run migrations against the compose Postgres with `npx prisma migrate deploy` (or `npx prisma migrate dev` when authoring a new one).

Connectivity checks:

```bash
docker compose exec postgres psql -U eventforge -d eventforge -c '\dt'
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Tear down with `docker compose down` (add `-v` to also drop the Postgres/Kafka data volumes).

## Roadmap

See [`docs/roadmap.html`](docs/roadmap.html) for the full 24-milestone plan, including the job state machine, Kafka topic catalog, event envelope shape, and the eventual extraction of `execution`, `notifications`, and `audit` into standalone services.

## License

Unlicensed / personal portfolio project.
