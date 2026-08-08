import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { JobDetail } from './JobDetail';
import * as client from '../api/client';
import type { JobResponse } from '../api/client';

vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof client>('../api/client');
  return { ...actual, getJob: vi.fn(), cancelJob: vi.fn(), retryJob: vi.fn() };
});

function makeJob(overrides: Partial<JobResponse> = {}): JobResponse {
  return {
    id: 'job-1',
    type: 'GENERATE_REPORT',
    status: 'FAILED',
    payload: { reportName: 'weekly' },
    result: null,
    error: 'handler always fails',
    attempt: 1,
    maxAttempts: 3,
    idempotencyKey: null,
    correlationId: 'corr-1',
    cancelRequested: false,
    createdAt: '2026-08-07T12:00:00.000Z',
    updatedAt: '2026-08-07T12:00:05.000Z',
    startedAt: '2026-08-07T12:00:01.000Z',
    completedAt: null,
    ...overrides,
  };
}

function renderDetail() {
  return render(
    <MemoryRouter initialEntries={['/jobs/job-1']}>
      <Routes>
        <Route path="/jobs/:jobId" element={<JobDetail />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('JobDetail', () => {
  it('renders job fields and timeline', async () => {
    vi.mocked(client.getJob).mockResolvedValue(makeJob());

    renderDetail();

    expect(await screen.findByText('GENERATE_REPORT')).toBeInTheDocument();
    expect(screen.getByText('handler always fails')).toBeInTheDocument();
    expect(screen.getByText('FAILED')).toBeInTheDocument();
  });

  it('enables Retry for a FAILED job and calls the API on click', async () => {
    vi.mocked(client.getJob).mockResolvedValue(makeJob({ status: 'FAILED' }));
    vi.mocked(client.retryJob).mockResolvedValue(makeJob({ status: 'QUEUED' }));
    const user = userEvent.setup();

    renderDetail();

    const retryButton = await screen.findByRole('button', { name: 'Retry' });
    expect(retryButton).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled();

    await user.click(retryButton);

    await waitFor(() => expect(client.retryJob).toHaveBeenCalledWith('job-1'));
  });

  it('enables Cancel for a RUNNING job and calls the API on click', async () => {
    vi.mocked(client.getJob).mockResolvedValue(makeJob({ status: 'RUNNING', cancelRequested: false }));
    vi.mocked(client.cancelJob).mockResolvedValue(makeJob({ status: 'RUNNING', cancelRequested: true }));
    const user = userEvent.setup();

    renderDetail();

    const cancelButton = await screen.findByRole('button', { name: 'Cancel' });
    expect(cancelButton).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeDisabled();

    await user.click(cancelButton);

    await waitFor(() => expect(client.cancelJob).toHaveBeenCalledWith('job-1'));
  });

  it('disables both actions for a terminal SUCCEEDED job', async () => {
    vi.mocked(client.getJob).mockResolvedValue(makeJob({ status: 'SUCCEEDED', result: { ok: true } }));

    renderDetail();

    expect(await screen.findByRole('button', { name: 'Cancel' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeDisabled();
  });
});
