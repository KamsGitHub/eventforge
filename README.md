# EventForge

A Kafka-based distributed job processing platform, written entirely in TypeScript. It starts as a well-structured modular monolith and is incrementally evolved into independently deployable microservices — demonstrating clean architecture, reliability patterns (transactional outbox, idempotent consumers, retries/DLQ), observability, and testing along the way.

**Status:** early stage. Milestone 1 of 24 complete — repo scaffold, strict TypeScript, linting, and enforced module boundaries. No HTTP server, database, or Kafka wiring yet; that starts at Milestone 2.

The full build plan — every milestone's objective, architecture decisions, database/Kafka changes, tests, and completion criteria — lives in [`docs/roadmap.html`](docs/roadmap.html) (open it in a browser).

## Stack

Node.js · TypeScript · Fastify · PostgreSQL · Prisma · Apache Kafka (KafkaJS) · Zod · Docker Compose · Jest · Testcontainers · Pino · Prometheus/Grafana · React

## Architecture

A modular monolith, structured so each business module can later be extracted into its own service without a rewrite:

```
src/
├── config/            # environment/config loading (Milestone 3)
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
npm run lint        # ESLint
npm run typecheck   # tsc --noEmit
npm run boundaries  # module-boundary check (dependency-cruiser)
npm test            # Jest
npm run build        # compile src/ to dist/
```

All four checks (lint, typecheck, boundaries, test) run in CI on every push and pull request — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Docker Compose (Postgres + Kafka) arrives in Milestone 2; there's nothing to run yet beyond the checks above.

## Roadmap

See [`docs/roadmap.html`](docs/roadmap.html) for the full 24-milestone plan, including the job state machine, Kafka topic catalog, event envelope shape, and the eventual extraction of `execution`, `notifications`, and `audit` into standalone services.

## License

Unlicensed / personal portfolio project.
