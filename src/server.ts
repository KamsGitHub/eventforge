import 'dotenv/config';

import { buildApp } from '@/app';

const app = buildApp();

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  app.log.info({ signal }, 'shutting down');

  try {
    await app.close();
    process.exit(0);
  } catch (error) {
    app.log.error({ error }, 'error during shutdown');
    process.exit(1);
  }
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

app
  .listen({ port: app.config.port, host: '0.0.0.0' })
  .then((address) => {
    app.log.info({ address }, 'server listening');
  })
  .catch((error: unknown) => {
    app.log.error({ error }, 'failed to start server');
    process.exit(1);
  });
