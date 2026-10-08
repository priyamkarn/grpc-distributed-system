import { parseArgs } from 'node:util';
import { createWorker } from '../src/worker.js';

const { values } = parseArgs({
  options: {
    port: { type: 'string', default: '50061' },
    id: { type: 'string' },
  },
});

const worker = createWorker({ id: values.id || `worker-${values.port}` });
await worker.start(Number(values.port));

process.on('SIGINT', async () => {
  await worker.stop();
  process.exit(0);
});
