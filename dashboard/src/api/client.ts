// Mirrors src/modules/jobs/api/schemas.ts's JobResponseSchema — kept as a
// plain type instead of importing the server's zod schema, since the
// dashboard must never import server code (Milestone 16's boundary rule).
export type JobStatus = 'PENDING' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'DEAD_LETTERED' | 'CANCELLED';

export interface JobResponse {
  id: string;
  type: string;
  status: JobStatus;
  payload: unknown;
  result: unknown;
  error: string | null;
  attempt: number;
  maxAttempts: number;
  idempotencyKey: string | null;
  correlationId: string;
  cancelRequested: boolean;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

export interface ListJobsParams {
  status?: JobStatus;
  limit?: number;
  offset?: number;
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body: unknown = await response.json().catch(() => null);
    const message =
      body !== null && typeof body === 'object' && 'error' in body && typeof body.error === 'string'
        ? body.error
        : `Request failed with status ${response.status}`;
    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

export async function listJobs(params: ListJobsParams = {}): Promise<JobResponse[]> {
  const query = new URLSearchParams();
  if (params.status) query.set('status', params.status);
  if (params.limit !== undefined) query.set('limit', String(params.limit));
  if (params.offset !== undefined) query.set('offset', String(params.offset));

  const response = await fetch(`/api/jobs?${query.toString()}`);
  const body = await parseResponse<{ jobs: JobResponse[] }>(response);
  return body.jobs;
}

export async function getJob(jobId: string): Promise<JobResponse> {
  const response = await fetch(`/api/jobs/${jobId}`);
  return parseResponse<JobResponse>(response);
}

export async function retryJob(jobId: string): Promise<JobResponse> {
  const response = await fetch(`/api/jobs/${jobId}/retry`, { method: 'POST' });
  return parseResponse<JobResponse>(response);
}

export async function cancelJob(jobId: string): Promise<JobResponse> {
  const response = await fetch(`/api/jobs/${jobId}/cancel`, { method: 'POST' });
  return parseResponse<JobResponse>(response);
}
