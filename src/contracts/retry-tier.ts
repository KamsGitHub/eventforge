export type RetryTier = 1 | 2 | 3;

export const MAX_RETRY_TIER: RetryTier = 3;

export const RETRY_TIER_TOPICS: Readonly<Record<RetryTier, string>> = {
  1: 'jobs.retry-1',
  2: 'jobs.retry-2',
  3: 'jobs.retry-3',
};

/**
 * Which retry topic a just-failed attempt routes to. Tiers are capped at 3
 * (only 3 retry topics exist) even if maxAttempts allows more retries than
 * that — later retries all reuse tier 3's delay. Separate per-tier topics
 * (rather than one topic + encoded delay) surface backlog-per-tier in
 * monitoring: a job stuck at tier 3 is a different signal than one at tier 1.
 *
 * Lives under src/contracts (not modules/execution/domain) because both the
 * execution module (dispatches to these topics) and the jobs module (maps
 * them to a status transition) need it — importing a module's own domain/
 * from another module would violate the module-boundary rule.
 */
export function tierForAttempt(attempt: number): RetryTier {
  return Math.min(attempt, MAX_RETRY_TIER) as RetryTier;
}
