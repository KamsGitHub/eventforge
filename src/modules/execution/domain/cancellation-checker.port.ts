/**
 * Checks whether a job has been cancelled, without execution ever importing
 * the jobs module's domain/infrastructure directly (that would violate the
 * module-boundary rule). The composition root (server.ts) supplies the real
 * implementation, backed by the jobs module's own repository.
 */
export type CancellationChecker = (jobId: string) => Promise<boolean>;
