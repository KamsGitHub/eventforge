import { useEffect, useState, useCallback } from 'react';

const DEFAULT_INTERVAL_MS = 2000;

/**
 * Re-runs `fetcher` immediately and then every `intervalMs`, until the
 * component unmounts or `deps` change. This is the "polling" the roadmap's
 * Milestone 16 completion criteria calls for (WebSocket/SSE push is an
 * explicit stretch goal, not required).
 */
export function usePolling<T>(fetcher: () => Promise<T>, deps: unknown[], intervalMs = DEFAULT_INTERVAL_MS) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  const run = useCallback(fetcher, deps); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    async function tick() {
      try {
        const result = await run();
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err : new Error(String(err)));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void tick();
    const id = setInterval(() => void tick(), intervalMs);

    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [run, intervalMs]);

  return { data, error, loading };
}
