/** @type {import('jest').Config} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  testMatch: ['<rootDir>/tests/**/*.test.ts'],
  moduleNameMapper: {
    '^@/(.*)$': '<rootDir>/src/$1',
    // The generated Prisma client (generated/prisma) uses NodeNext-style
    // relative imports with explicit .js extensions against .ts source
    // files (no compiled .js exists) — strip the extension so Jest
    // resolves the real .ts files.
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
