const moduleNameMapper = {
  '^@/(.*)$': '<rootDir>/src/$1',
  // The generated Prisma client (generated/prisma) uses NodeNext-style
  // relative imports with explicit .js extensions against .ts source
  // files (no compiled .js exists) — strip the extension so Jest
  // resolves the real .ts files.
  '^(\\.{1,2}/.*)\\.js$': '$1',
};

/** @type {import('jest').Config} */
module.exports = {
  rootDir: '.',
  // Every integration-test file now shares one Postgres + one Kafka
  // container for the whole run (tests/integration/setup.ts), booted once
  // in globalSetup instead of per file. Several of the real consumers under
  // test (job-status.consumer.ts, retry-router.ts, jobs-requested.consumer.ts,
  // ...) use hardcoded, production consumer-group IDs — two integration test
  // files racing against the *same* broker with the *same* group ID would
  // silently steal each other's messages via Kafka's own rebalance protocol.
  // Serializing fully avoids that; it's cheap now that container boot (the
  // original reason for running 2 workers locally) only happens once per run
  // rather than once per file.
  maxWorkers: 1,
  projects: [
    {
      displayName: 'unit',
      preset: 'ts-jest',
      testEnvironment: 'node',
      testMatch: ['<rootDir>/tests/**/*.test.ts'],
      testPathIgnorePatterns: ['<rootDir>/tests/integration/'],
      moduleNameMapper,
    },
    {
      displayName: 'integration',
      preset: 'ts-jest',
      testEnvironment: 'node',
      testMatch: ['<rootDir>/tests/integration/**/*.test.ts'],
      moduleNameMapper,
      globalSetup: '<rootDir>/tests/integration/global-setup.ts',
      globalTeardown: '<rootDir>/tests/integration/global-teardown.ts',
    },
  ],
};
