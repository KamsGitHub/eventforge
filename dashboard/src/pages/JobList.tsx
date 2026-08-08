import { useState } from 'react';
import { Link } from 'react-router-dom';

import { listJobs, type JobStatus } from '../api/client';
import { usePolling } from '../hooks/usePolling';
import { StatusBadge } from '../components/StatusBadge';

const PAGE_SIZE = 20;
const STATUSES: JobStatus[] = ['PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED'];

export function JobList() {
  const [status, setStatus] = useState<JobStatus | ''>('');
  const [offset, setOffset] = useState(0);

  const { data: jobs, error, loading } = usePolling(
    () => listJobs({ status: status || undefined, limit: PAGE_SIZE, offset }),
    [status, offset],
  );

  function handleStatusChange(next: JobStatus | '') {
    setStatus(next);
    setOffset(0);
  }

  return (
    <div>
      <h1>Jobs</h1>

      <div className="toolbar">
        <label>
          Status:{' '}
          <select
            aria-label="Status"
            value={status}
            onChange={(event) => handleStatusChange(event.target.value as JobStatus | '')}
          >
            <option value="">All</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </div>

      {error && <p className="error">Failed to load jobs: {error.message}</p>}
      {loading && !jobs && <p>Loading…</p>}

      {jobs && (
        <>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Attempt</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <Link to={`/jobs/${job.id}`}>{job.id}</Link>
                  </td>
                  <td>{job.type}</td>
                  <td>
                    <StatusBadge status={job.status} />
                  </td>
                  <td>
                    {job.attempt}/{job.maxAttempts}
                  </td>
                  <td>{new Date(job.createdAt).toLocaleString()}</td>
                </tr>
              ))}
              {jobs.length === 0 && (
                <tr>
                  <td colSpan={5}>No jobs found.</td>
                </tr>
              )}
            </tbody>
          </table>

          <div className="pagination">
            <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}>
              Previous
            </button>
            <button disabled={jobs.length < PAGE_SIZE} onClick={() => setOffset(offset + PAGE_SIZE)}>
              Next
            </button>
          </div>
        </>
      )}
    </div>
  );
}
