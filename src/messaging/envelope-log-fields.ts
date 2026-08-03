/**
 * Every envelope (src/contracts/envelope.ts) carries these fields at its
 * top level. Read defensively off `unknown` rather than importing the zod
 * schema — the messaging wrapper logs whatever's on the wire, including a
 * malformed message that hasn't been validated yet.
 */
export interface EnvelopeLogFields {
  readonly eventId?: string;
  readonly eventType?: string;
  readonly aggregateId?: string;
  readonly correlationId?: string;
  readonly causationId?: string | null;
}

const STRING_FIELDS = ['eventId', 'eventType', 'aggregateId', 'correlationId'] as const;

export function envelopeLogFields(value: unknown): EnvelopeLogFields {
  if (typeof value !== 'object' || value === null) {
    return {};
  }

  const record = value as Record<string, unknown>;
  const fields: Record<string, string | null> = {};

  for (const field of STRING_FIELDS) {
    if (typeof record[field] === 'string') {
      fields[field] = record[field];
    }
  }

  if (typeof record['causationId'] === 'string' || record['causationId'] === null) {
    fields['causationId'] = record['causationId'];
  }

  return fields;
}
