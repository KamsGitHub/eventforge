// @ts-check
import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';

export default tseslint.config(
  {
    ignores: [
      'dist/**',
      'node_modules/**',
      'coverage/**',
      'generated/**',
      '*.config.mjs',
      '*.config.js',
      '.dependency-cruiser.cjs',
      // The dashboard (Milestone 16) is a separate app with its own
      // package.json/tsconfig/lint tooling (oxlint) — it's a pure HTTP
      // client of the API, never sharing code with the server, so it's
      // excluded from the root project's own lint/type config rather than
      // forced to satisfy server-side rules (e.g. type-checked linting
      // against this tsconfig, which doesn't even include dashboard/src).
      'dashboard/**',
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/consistent-type-imports': 'error',
    },
  },
  eslintConfigPrettier,
);
