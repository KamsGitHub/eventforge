import { spawn, type ChildProcessByStdio } from 'node:child_process';
import path from 'node:path';
import type { Readable } from 'node:stream';

const FIXTURE_PATH = path.join(__dirname, 'idempotent-consumer-process.ts');

export interface FixtureEvent {
  readonly event: string;
  readonly [key: string]: unknown;
}

export interface FixtureProcess {
  readonly child: ChildProcessByStdio<null, Readable, Readable>;
  readonly events: readonly FixtureEvent[];
  /** Resolves with the first already-seen or future event matching `predicate`. */
  waitFor(predicate: (event: FixtureEvent) => boolean, timeoutMs?: number): Promise<FixtureEvent>;
  /**
   * Kills the whole process tree, not just the immediate child. `npx tsx`
   * is itself a 2-3 level process tree (npx's own shell shim, the tsx CLI,
   * and the actual node process tsx re-execs with its loader flags) —
   * `child.kill()` only reaches the first of those, leaving the real
   * consumer process (and its open Kafka connections) running as an orphan
   * that keeps Jest from exiting. `detached: true` below makes the child
   * its own process group leader, so signaling the negative pid reaches
   * every process in that tree.
   */
  kill(signal: NodeJS.Signals): void;
}

/**
 * Spawns tests/integration/fixtures/idempotent-consumer-process.ts as a real
 * child process — needed because the rebalance and graceful-shutdown tests
 * have to SIGKILL/SIGTERM a consumer mid-message, which isn't something you
 * can do to the Jest process itself. `npx tsx` runs the fixture directly
 * (no Jest VM sandbox involved), so it doesn't need the
 * --experimental-vm-modules flag `npm test` requires.
 */
export function spawnConsumerFixture(env: Record<string, string>): FixtureProcess {
  const child = spawn('npx', ['tsx', FIXTURE_PATH], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  });

  const events: FixtureEvent[] = [];
  const waiters: { predicate: (event: FixtureEvent) => boolean; resolve: (event: FixtureEvent) => void }[] = [];
  let buffer = '';

  child.stdout.on('data', (chunk: Buffer) => {
    buffer += chunk.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';

    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }

      const parsed = JSON.parse(line) as FixtureEvent;
      events.push(parsed);

      for (const waiter of [...waiters]) {
        if (waiter.predicate(parsed)) {
          waiters.splice(waiters.indexOf(waiter), 1);
          waiter.resolve(parsed);
        }
      }
    }
  });

  function waitFor(predicate: (event: FixtureEvent) => boolean, timeoutMs = 20_000): Promise<FixtureEvent> {
    const already = events.find(predicate);

    if (already) {
      return Promise.resolve(already);
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        predicate,
        resolve: (event: FixtureEvent) => {
          clearTimeout(timer);
          resolve(event);
        },
      };

      const timer = setTimeout(() => {
        const index = waiters.indexOf(waiter);

        if (index >= 0) {
          waiters.splice(index, 1);
        }

        reject(new Error('timed out waiting for fixture event matching predicate'));
      }, timeoutMs);

      waiters.push(waiter);
    });
  }

  function kill(signal: NodeJS.Signals): void {
    if (typeof child.pid === 'number') {
      try {
        process.kill(-child.pid, signal);
      } catch {
        // Already exited (e.g. a second kill() call in a finally block
        // after the process died on its own) — nothing left to signal.
      }
    }
  }

  return { child, events, waitFor, kill };
}
