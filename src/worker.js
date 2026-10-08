import { grpc, jobs } from './proto.js';
import { defaultHandlers, jobError } from './handlers.js';

export function createWorker({
  id,
  handlers = defaultHandlers,
  log = (message) => console.log(`[${id}] ${message}`),
}) {
  const running = new Map();
  let server;

  async function execute(call, callback) {
    const { job_id, type, payload, timeout_ms } = call.request;
    const handler = handlers[type];

    if (!handler)
      return callback(null, { ok: false, error: `unknown job type "${type}"`, retryable: false });

    let data;
    try {
      data = payload ? JSON.parse(payload) : undefined;
    } catch {
      return callback(null, { ok: false, error: 'payload is not valid JSON', retryable: false });
    }

    const controller = new AbortController();
    let reason = null;

    const abort = (kind) => {
      if (reason) return;
      reason = kind;
      controller.abort(jobError(kind === 'timeout' ? 'timeout' : 'cancelled', kind === 'timeout'));
    };

    running.set(job_id, abort);
    const timer = timeout_ms > 0 ? setTimeout(() => abort('timeout'), timeout_ms) : null;
    call.on('cancelled', () => abort('cancel'));

    log(`start ${job_id.slice(0, 8)} ${type}`);

    let response;
    try {
      const result = await handler(data, { signal: controller.signal });
      response = { ok: true, result: JSON.stringify(result ?? null) };
    } catch (e) {
      if (reason === 'cancel') response = { ok: false, error: 'cancelled', retryable: false };
      else if (reason === 'timeout')
        response = { ok: false, error: `timed out after ${timeout_ms}ms`, retryable: true };
      else response = { ok: false, error: e.message, retryable: e.retryable !== false };
    } finally {
      clearTimeout(timer);
      running.delete(job_id);
    }

    log(`done  ${job_id.slice(0, 8)} ${response.ok ? 'ok' : 'error: ' + response.error}`);
    callback(null, response);
  }

  async function start(port) {
    server = new grpc.Server();
    server.addService(jobs.Worker.service, {
      Execute: execute,
      Cancel: (call, callback) => {
        const abort = running.get(call.request.job_id);
        if (abort) abort('cancel');
        callback(null, { found: Boolean(abort) });
      },
      Ping: (_call, callback) => callback(null, { worker_id: id, inflight: running.size }),
    });
    await new Promise((resolve, reject) =>
      server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(),
        (err) => (err ? reject(err) : resolve())));
    log(`listening on :${port}`);
  }

  function stop(force = false) {
    if (force) {
      server.forceShutdown();
      return Promise.resolve();
    }
    return new Promise((resolve) => server.tryShutdown(() => resolve()));
  }

  return { id, start, stop };
}
