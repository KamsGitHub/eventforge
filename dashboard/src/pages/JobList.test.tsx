import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { JobList } from './JobList';
import * as client from '../api/client';
import type { JobResponse } from '../api/client';

vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof client>('../api/client');
  return { ...actual, listJobs: vi.fn() };
});

function makeJob(overrides: Partial<JobResponse> = {}): JobResponse {
  return {
    id: 'job-1',
    type: 'GENERATE_REPORT',
    status: 'PENDING',
    payload: {},
    result: null,
    error: null,
    attempt: 0,
    maxAttempts: 3,
    idempotencyKey: null,
    correlationId: 'corr-1',
    cancelRequested: false,
    createdAt: '2026-08-07T12:00:00.000Z',
    updatedAt: '2026-08-07T12:00:00.000Z',
    startedAt: null,
    completedAt: null,
    ...overrides,
  };
}

describe('JobList', () => {
  it('renders jobs returned by the API', async () => {
    vi.mocked(client.listJobs).mockResolvedValue([makeJob({ id: 'job-1', type: 'GENERATE_REPORT' })]);

    render(
      <MemoryRouter>
        <JobList />
      </MemoryRouter>,
    );

    expect(await screen.findByText('job-1')).toBeInTheDocument();
    expect(screen.getByText('GENERATE_REPORT')).toBeInTheDocument();
    expect(screen.getByText('PENDING', { selector: 'span' })).toBeInTheDocument();
  });

  it('shows an empty state when there are no jobs', async () => {
    vi.mocked(client.listJobs).mockResolvedValue([]);

    render(
      <MemoryRouter>
        <JobList />
      </MemoryRouter>,
    );

    expect(await screen.findByText('No jobs found.')).toBeInTheDocument();
  });

  it('re-queries the API with the selected status filter', async () => {
    vi.mocked(client.listJobs).mockResolvedValue([]);
    const user = userEvent.setup();

    render(
      <MemoryRouter>
        <JobList />
      </MemoryRouter>,
    );

    await waitFor(() => expect(client.listJobs).toHaveBeenCalledWith({ status: undefined, limit: 20, offset: 0 }));

    await user.selectOptions(screen.getByLabelText('Status'), 'FAILED');

    await waitFor(() => expect(client.listJobs).toHaveBeenCalledWith({ status: 'FAILED', limit: 20, offset: 0 }));
  });

  it('surfaces a load error', async () => {
    vi.mocked(client.listJobs).mockRejectedValue(new Error('network down'));

    render(
      <MemoryRouter>
        <JobList />
      </MemoryRouter>,
    );

    expect(await screen.findByText(/Failed to load jobs: network down/)).toBeInTheDocument();
  });
});
