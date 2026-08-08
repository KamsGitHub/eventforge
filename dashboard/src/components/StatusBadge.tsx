import type { JobStatus } from '../api/client';

const CLASS_BY_STATUS: Record<JobStatus, string> = {
  PENDING: 'status status-pending',
  QUEUED: 'status status-queued',
  RUNNING: 'status status-running',
  SUCCEEDED: 'status status-succeeded',
  FAILED: 'status status-failed',
  DEAD_LETTERED: 'status status-dead',
  CANCELLED: 'status status-cancelled',
};

export function StatusBadge({ status }: { status: JobStatus }) {
  return <span className={CLASS_BY_STATUS[status]}>{status}</span>;
}
