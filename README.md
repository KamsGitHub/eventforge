# EventForge

A Kafka-based distributed job processing platform, written entirely in TypeScript. It starts as a well-structured modular monolith and is incrementally evolved into independently deployable microservices — demonstrating clean architecture, reliability patterns (transactional outbox, idempotent consumers, retries/DLQ), observability, and testing along the way.

**Status:** early stage. Milestones 1–3 of 24 complete — repo scaffold with enforced module boundaries, a local Postgres + Kafka environment, and a running Fastify server with a health endpoint. No database or Kafka wiring from the app yet; that starts at Milestone 4.

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
├── db/                 # database wiring (Milestone 4)
├── messaging/          # Kafka producer/consumer wrapper (Milestone 6)
├── outbox/             # transactional outbox publisher (Milestone 9)
├── shared/             # cross-cutting technical utilities only — no business logic
└── modules/
    ├── jobs/            # job management: API, state machine, persistence
    ├── execution/       # job execution: handler registry, Kafka consumers
    ├── notifications/   # job-completion notifications
    └── audit/           # append-only audit trail
        └── {api,application,domain,infrastructure}/
```

Each module owns its own `api → application → domain → infrastructure` slice. A module may only be reached through its `api/` folder or through Kafka events — never by reaching into another module's `application`, `domain`, or `infrastructure` layers directly. This boundary is enforced by [`dependency-cruiser`](.dependency-cruiser.cjs), not just convention: `npm run boundaries` fails the build on a violation.

## Getting started

```bash
npm install
npm run dev          # tsx watch src/server.ts — http://localhost:3000/health
npm run lint         # ESLint
npm run typecheck    # tsc --noEmit
npm run boundaries   # module-boundary check (dependency-cruiser)
npm test             # Jest
npm run build        # compile src/ to dist/
npm start            # node dist/server.js (after npm run build)
```

All four checks (lint, typecheck, boundaries, test) run in CI on every push and pull request — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml). The test suite includes a Testcontainers-backed Postgres smoke test, so `npm test` needs a working Docker daemon.

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

All topics are created explicitly by `infrastructure/kafka/create-topics.sh` (3 partitions, replication factor 1) — broker auto-create is intentionally never relied on. Postgres credentials and the Kafka broker address match `.env.example`; the app doesn't read `DATABASE_URL`/`KAFKA_BROKERS` yet (that's Milestone 4/6) — only `NODE_ENV`, `PORT`, and `LOG_LEVEL` are consumed so far, via `src/config/env.ts`.

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
