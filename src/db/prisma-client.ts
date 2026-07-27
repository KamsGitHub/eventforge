/**
 * Side-effect-free re-export of the generated Prisma client, plus a factory
 * for constructing an instance. Import from here (not ./prisma) when a
 * caller wants to construct/point its own client — e.g. tests pointing at a
 * Testcontainers database — since ./prisma's singleton eagerly calls
 * loadConfig() at module load time, which requires a live DATABASE_URL and
 * shouldn't be a side effect of merely wanting the client type or factory.
 */
import { PrismaPg } from '@prisma/adapter-pg';

import { loadConfig } from '@/config/env';

import { Prisma, PrismaClient } from '../../generated/prisma/client';
import type { Job as PrismaJobRow } from '../../generated/prisma/client';

export { Prisma, PrismaClient };
export type { PrismaJobRow };

export function createPrismaClient(databaseUrl: string = loadConfig().databaseUrl): PrismaClient {
  const adapter = new PrismaPg({ connectionString: databaseUrl });

  return new PrismaClient({ adapter });
}
