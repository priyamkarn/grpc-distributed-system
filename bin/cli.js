import { parseArgs } from 'node:util';
import { jobs, insecure, call } from '../src/proto.js';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    addr: { type: 'string', default: process.env.SCHEDULER_ADDR || 'localhost:50051' },
    'max-attempts': { type: 'string', default: '0' },
    timeout: { type: 'string', default: '0' },
    wait: { type: 'boolean', default: false },
    limit: { type: 'string', default: '20' },
  },
});
const [cmd, ...args] = positionals;
const client = new jobs.Scheduler(values.addr, insecure());
const rpc = (m, r) => call(client, m, r, { deadline: Date.now() + 5000 });

const fmt = (j) => {
  const dur = j.finished_at && j.started_at ? ` ${j.finished_at - j.started_at}ms` : '';
  const out = j.state === 'SUCCEEDED' ? ` result=${j.result}` : j.error ? ` error="${j.error}"` : '';
  return `${j.id}  ${j.state.padEnd(9)} ${j.spec.type.padEnd(6)} attempts=${j.attempts} worker=${j.worker || '-'}${dur}${out}`;
};

async function waitFor(id) {
  for (;;) {
    const j = await rpc('GetJob', { id });
    if (['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(j.state)) return j;
    await new Promise((r) => setTimeout(r, 200));
  }
}

try {
  switch (cmd) {
    case 'submit': {
      const [type, payload = ''] = args;
      const { id } = await rpc('SubmitJob', {
        spec: { type, payload, max_attempts: Number(values['max-attempts']), timeout_ms: Number(values.timeout) },
      });
      console.log(values.wait ? fmt(await waitFor(id)) : id);
      break;
    }
    case 'get': console.log(fmt(await rpc('GetJob', { id: args[0] }))); break;
    case 'wait': console.log(fmt(await waitFor(args[0]))); break;
    case 'cancel': console.log(await rpc('CancelJob', { id: args[0] })); break;
    case 'list': {
      const { jobs: list } = await rpc('ListJobs', { state: args[0]?.toUpperCase() || 'UNSPECIFIED', limit: Number(values.limit) });
      list.forEach((j) => console.log(fmt(j)));
      break;
    }
    case 'status': console.log(JSON.stringify(await rpc('Status', {}), null, 2)); break;
    default:
      console.log(`usage: cli [--addr host:port] <command>
  submit <type> [payloadJSON] [--max-attempts N] [--timeout MS] [--wait]
  get <id> | wait <id> | cancel <id> | list [STATE] [--limit N] | status
types: echo sleep sum fib flaky fail`);
  }
} catch (e) {
  console.error(`error: ${e.details || e.message}`);
  process.exitCode = 1;
} finally {
  client.close();
}
