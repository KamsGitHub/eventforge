import client from 'prom-client';

/**
 * Own registry (rather than the prom-client default one) so tests that
 * import this module repeatedly across files never hit "metric already
 * registered" from a shared global registry.
 */
export const metricsRegistry = new client.Registry();
client.collectDefaultMetrics({ register: metricsRegistry });

/**
 * Labeled by job type + outcome only — never jobId, which would blow up
 * cardinality (a new time series per job forever). Observed once per
 * dispatch in modules/execution/infrastructure/process-job-requested.ts,
 * the application-service boundary that already knows both the type and
 * how the attempt ended.
 */
export const jobDurationSeconds = new client.Histogram({
  name: 'job_duration_seconds',
  help: 'Job execution duration in seconds, labeled by job type and outcome',
  labelNames: ['type', 'outcome'] as const,
  registers: [metricsRegistry],
});

/**
 * Incremented once per Job.status transition — modules/jobs/infrastructure's
 * status/dead-letter consumers are the only writers of Job.status, so they're
 * the only places this counter is touched.
 */
export const jobsTotal = new client.Counter({
  name: 'jobs_total',
  help: 'Total count of job status transitions, labeled by status',
  labelNames: ['status'] as const,
  registers: [metricsRegistry],
});

export const retryTotal = new client.Counter({
  name: 'retry_total',
  help: 'Total count of retry dispatches, labeled by retry tier',
  labelNames: ['tier'] as const,
  registers: [metricsRegistry],
});

export const deadLetterTotal = new client.Counter({
  name: 'dead_letter_total',
  help: 'Total count of messages routed to the dead-letter topic',
  registers: [metricsRegistry],
});

/**
 * Set once per outbox-publisher poll cycle (see outbox/outbox-publisher.ts),
 * reusing that cycle's own cadence rather than adding a separate poll loop —
 * a second high-frequency poll would add its own load to the same table.
 */
export const outboxBacklog = new client.Gauge({
  name: 'outbox_backlog',
  help: 'Number of currently unpublished outbox rows',
  registers: [metricsRegistry],
});
