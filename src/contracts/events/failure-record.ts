import { z } from 'zod';

/** One entry in a job's accumulated failure history, carried forward across retry hops. */
export const failureRecordSchema = z.object({
  attempt: z.number().int().positive(),
  error: z.string(),
  failedAt: z.iso.datetime(),
});

export type FailureRecord = z.infer<typeof failureRecordSchema>;
