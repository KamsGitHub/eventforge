import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';

import { cancelJob, getJob, retryJob } from '../api/client';
import { usePolling } from '../hooks/usePolling';
import { StatusBadge } from '../components/StatusBadge';

const CANCELLABLE_STATUSES = new Set(['PENDING', 'QUEUED', 'RUNNING']);
const RETRYABLE_STATUSES = new Set(['FAILED', 'DEAD_LETTERED']);

export function JobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [actionError, setActionError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);

  const { data: job, error, loading } = usePolling(() => getJob(jobId!), [jobId, refreshNonce]);

  async function handleAction(action: (id: string) => Promise<unknown>) {
    if (!jobId) return;
    setActing(true);
    setActionError(null);
    try {
      await action(jobId);
      setRefreshNonce((n) => n + 1);
    } catch (err) {
      setActionError(err instanceof Error ? err.message : String(err));
    } finally {
      setActing(false);
    }
  }

  if (loading && !job) return <p>Loading…</p>;
  if (error && !job) return <p className="error">Failed to load job: {error.message}</p>;
  if (!job) return null;

  const canCancel = CANCELLABLE_STATUSES.has(job.status) && !job.cancelRequested;
  const canRetry = RETRYABLE_STATUSES.has(job.status);

  return (
    <div>
      <p>
        <Link to="/">&larr; Back to jobs</Link>
      </p>

      <h1>
        Job <code>{job.id}</code>
      </h1>

      <p>
        <StatusBadge status={job.status} /> {job.cancelRequested && <em>(cancel requested)</em>}
      </p>

      <dl className="job-fields">
        <dt>Type</dt>
        <dd>{job.type}</dd>

        <dt>Attempt</dt>
        <dd>
          {job.attempt} / {job.maxAttempts}
        </dd>

        <dt>Correlation ID</dt>
        <dd>{job.correlationId}</dd>

        <dt>Error</dt>
        <dd>{job.error ?? '—'}</dd>
      </dl>

      <h2>Timeline</h2>
      <ul className="timeline">
        <li>Created: {new Date(job.createdAt).toLocaleString()}</li>
        <li>Started: {job.startedAt ? new Date(job.startedAt).toLocaleString() : '—'}</li>
        <li>Completed: {job.completedAt ? new Date(job.completedAt).toLocaleString() : '—'}</li>
        <li>Last updated: {new Date(job.updatedAt).toLocaleString()}</li>
      </ul>

      <h2>Payload</h2>
      <pre>{JSON.stringify(job.payload, null, 2)}</pre>

      {job.result !== null && (
        <>
          <h2>Result</h2>
          <pre>{JSON.stringify(job.result, null, 2)}</pre>
        </>
      )}

      <div className="actions">
        <button disabled={!canCancel || acting} onClick={() => handleAction(cancelJob)}>
          Cancel
        </button>
        <button disabled={!canRetry || acting} onClick={() => handleAction(retryJob)}>
          Retry
        </button>
      </div>

      {actionError && <p className="error">{actionError}</p>}
    </div>
  );
}
