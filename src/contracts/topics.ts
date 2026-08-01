/**
 * Topic name constants needed by more than one module (importing a topic
 * string defined inside another module's infrastructure/ would itself be a
 * module-boundary violation, so cross-module topic names live here instead).
 */
export const JOBS_DEAD_LETTER_TOPIC = 'jobs.dead-letter';
export const JOBS_CANCELLED_TOPIC = 'jobs.cancelled';
