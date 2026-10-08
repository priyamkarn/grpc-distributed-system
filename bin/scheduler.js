import { parseArgs } from 'node:util';
import { createScheduler } from '../src/scheduler.js';

const { values } = parseArgs({
  options: {
    port: { type: 'string', default: '50051' },
    workers: { type: 'string' },
    concurrency: { type: 'string', default: '2' },
  },
});

if (!values.workers) {
  console.error('usage: scheduler --workers host:port,host:port [--port 50051] [--concurrency 2]');
  process.exit(1);
}

const scheduler = createScheduler({
  workers: values.workers.split(','),
  workerConcurrency: Number(values.concurrency),
});

await scheduler.start(Number(values.port));

process.on('SIGINT', () => {
  scheduler.stop();
  process.exit(0);
});
