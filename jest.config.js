/** @type {import('jest').Config} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  testMatch: ['<rootDir>/tests/**/*.test.ts'],
  // Several suites boot a real JVM-based broker/database via Testcontainers.
  // Jest's default worker count (~cpus-1) runs enough of them concurrently
  // to starve each container of CPU during startup, causing spurious
  // timeouts — capping workers at 2 was measurably faster and more reliable
  // than both the default and full serialization (maxWorkers: 1) in testing
  // on a 4-core dev machine. GitHub's hosted runner only has 2 vCPUs, and
  // by Milestone 9 there are enough Testcontainers suites (each booting its
  // own Kafka/Postgres container) that 2 concurrent workers there starves
  // whichever broker starts last — serialize fully in CI instead.
  maxWorkers: process.env.CI ? 1 : 2,
  moduleNameMapper: {
    '^@/(.*)$': '<rootDir>/src/$1',
    // The generated Prisma client (generated/prisma) uses NodeNext-style
    // relative imports with explicit .js extensions against .ts source
    // files (no compiled .js exists) — strip the extension so Jest
    // resolves the real .ts files.
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
