import { Writable } from 'node:stream';

import pino from 'pino';

/**
 * A real pino instance writing to an in-memory stream instead of stdout —
 * the "pino test transport" the roadmap's Milestone 13 test calls for.
 * `lines()` parses every line written so far as JSON, in order, so a test
 * can grep them the same way it would grep real log output.
 */
export function createCapturingLogger(): { logger: pino.Logger; lines: () => Record<string, unknown>[] } {
  const chunks: string[] = [];
  const stream = new Writable({
    write(chunk: Buffer, _encoding, callback) {
      chunks.push(chunk.toString());
      callback();
    },
  });

  const logger = pino(stream);

  return {
    logger,
    lines: () =>
      chunks
        .join('')
        .split('\n')
        .filter((line) => line.length > 0)
        .map((line) => JSON.parse(line) as Record<string, unknown>),
  };
}
