/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: 'no-cross-module-internals',
      comment:
        "A module may only be reached through its api/ folder. Reaching into another module's " +
        'application, domain, or infrastructure layer creates a hidden coupling that blocks ' +
        'future service extraction.',
      severity: 'error',
      from: { path: '^src/modules/([^/]+)/' },
      to: { path: '^src/modules/(?!$1/)[^/]+/(application|domain|infrastructure)/' },
    },
    {
      name: 'shared-must-not-depend-on-modules',
      comment: 'shared/ holds cross-cutting technical code only; it must never depend on a business module.',
      severity: 'error',
      from: { path: '^src/shared/' },
      to: { path: '^src/modules/' },
    },
    {
      name: 'no-circular',
      comment: 'Circular dependencies make future extraction and reasoning about the codebase harder.',
      severity: 'warn',
      from: {},
      to: { circular: true },
    },
  ],
  options: {
    tsPreCompilationDeps: true,
    tsConfig: {
      fileName: 'tsconfig.json',
    },
    enhancedResolveOptions: {
      exportsFields: ['exports'],
      conditionNames: ['require', 'node', 'default'],
    },
  },
};
