import { Prisma, PrismaClient, createPrismaClient, type PrismaJobRow } from './prisma-client';

export { Prisma, PrismaClient, createPrismaClient };
export type { PrismaJobRow };

export const prisma = createPrismaClient();
