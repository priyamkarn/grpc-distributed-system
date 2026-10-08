import { createWorker } from './src/worker.js';
import { createScheduler } from './src/scheduler.js';
import { jobs, insecure, call } from './src/proto.js';

const W = ['localhost:50061', 'localhost:50062'];
const w1 = createWorker({ id: 'worker-1' });
const w2 = createWorker({ id: 'worker-2' });
await w1.start(50061);
await w2.start(50062);
const sched = createScheduler({ workers: W, workerConcurrency: 2 });
await sched.start(50051);

const client = new jobs.Scheduler('localhost:50051', insecure());
const rpc = (m, r) => call(client, m, r);
const submit = (type, payload, extra = {}) =>
  rpc('SubmitJob', { spec: { type, payload: JSON.stringify(payload), ...extra } }).then((r) => r.id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const wait = async (id) => {
  for (;;) {
    const j = await rpc('GetJob', { id });
    if (['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(j.state)) return j;
    await sleep(50);
  }
};
const show = (title, js) => {
  console.log(`\n--- ${title}`);
  js.forEach((j) => console.log(`  ${j.id.slice(0, 8)} ${j.spec.type.padEnd(6)} ${j.state.padEnd(9)} attempts=${j.attempts} worker=${j.worker || '-'} ${j.state === 'SUCCEEDED' ? 'result=' + j.result : 'error=' + j.error}`));
};

console.log('\n=== 1. Fan-out across workers');
const ids = await Promise.all([
  submit('echo', { hello: 'world' }),
  submit('sum', { numbers: [1, 2, 3, 4] }),
  submit('fib', { n: 90 }),
  submit('sleep', { ms: 300 }),
  submit('sleep', { ms: 300 }),
  submit('sleep', { ms: 300 }),
]);
show('results', await Promise.all(ids.map(wait)));

console.log('\n=== 2. Retries and permanent failure');
const flaky = await submit('flaky', { failRate: 0.7 }, { max_attempts: 8 });
const perm = await submit('fail', { message: 'bad input', permanent: true });
const unknown = await submit('nope', {});
show('results', await Promise.all([flaky, perm, unknown].map(wait)));

console.log('\n=== 3. Cancel a running job');
const long = await submit('sleep', { ms: 10000 });
while ((await rpc('GetJob', { id: long })).state !== 'RUNNING') await sleep(20);
console.log('  cancel ->', await rpc('CancelJob', { id: long }));
show('result', [await wait(long)]);

console.log('\n=== 4. Worker crash mid-flight (worker-2 killed)');
const sl = await Promise.all([1, 2, 3, 4].map(() => submit('sleep', { ms: 1200 })));
await sleep(400);
await w2.stop(true);
show('results (all should succeed on worker-1)', await Promise.all(sl.map(wait)));

console.log('\n=== Cluster status');
console.log(JSON.stringify(await rpc('Status', {}), null, 2));

client.close();
sched.stop();
await w1.stop(true);
process.exit(0);
