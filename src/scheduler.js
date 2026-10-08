import { randomUUID } from 'node:crypto';
import { grpc, jobs as proto, call, insecure } from './proto.js';

const DEFAULT_ATTEMPTS = 3;
const DEFAULT_TIMEOUT_MS = 30000;

const now = () => Date.now();
const rpcError = (code, message) => ({ code, message });

export function createScheduler({
  workers: addresses,
  workerConcurrency = 2,
  healthIntervalMs = 1000,
  log = (message) => console.log(`[scheduler] ${message}`),
}) {
  const store = new Map();
  const queue = [];
  const workers = addresses.map((address) => ({
    address,
    client: new proto.Worker(address, insecure()),
    healthy: false,
    inflight: 0,
    capacity: workerConcurrency,
    completed: 0,
  }));
  let server;
  let healthTimer;

  function submit(spec) {
    if (!spec?.type) throw rpcError(grpc.status.INVALID_ARGUMENT, 'spec.type is required');
    const job = {
      id: randomUUID(),
      spec: {
        type: spec.type,
        payload: spec.payload || '',
        max_attempts: Math.min(spec.max_attempts || DEFAULT_ATTEMPTS, 10),
        timeout_ms: spec.timeout_ms || DEFAULT_TIMEOUT_MS,
      },
      state: 'PENDING',
      attempts: 0,
      worker: '',
      result: '',
      error: '',
      created_at: now(),
      started_at: 0,
      finished_at: 0,
    };
    store.set(job.id, job);
    queue.push(job.id);
    log(`submitted ${job.id.slice(0, 8)} (${job.spec.type})`);
    dispatch();
    return job.id;
  }

  function get(id) {
    const job = store.get(id);
    if (!job) throw rpcError(grpc.status.NOT_FOUND, `job ${id} not found`);
    return job;
  }

  function list(state, limit) {
    const all = [...store.values()].reverse();
    const filtered = state && state !== 'UNSPECIFIED' ? all.filter((j) => j.state === state) : all;
    return filtered.slice(0, limit > 0 ? limit : 50);
  }

  function cancel(id) {
    const job = get(id);
    if (job.state === 'PENDING') {
      finish(job, 'CANCELLED', { error: 'cancelled' });
    } else if (job.state === 'RUNNING') {
      const worker = workers.find((w) => w.address === job.worker);
      finish(job, 'CANCELLED', { error: 'cancelled' });
      if (worker) call(worker.client, 'Cancel', { job_id: id }, { deadline: now() + 1000 }).catch(() => {});
    } else {
      return { cancelled: false, state: job.state };
    }
    log(`cancelled ${id.slice(0, 8)}`);
    return { cancelled: true, state: job.state };
  }

  function status() {
    const job_counts = {};
    for (const job of store.values()) job_counts[job.state] = (job_counts[job.state] || 0) + 1;
    return {
      workers: workers.map(({ address, healthy, inflight, capacity, completed }) => ({
        address,
        healthy,
        inflight,
        capacity,
        completed,
      })),
      job_counts,
      queue_depth: queue.filter((id) => store.get(id)?.state === 'PENDING').length,
    };
  }

  function pickWorker() {
    let best = null;
    for (const w of workers) {
      if (!w.healthy || w.inflight >= w.capacity) continue;
      if (!best || w.inflight / w.capacity < best.inflight / best.capacity) best = w;
    }
    return best;
  }

  function dispatch() {
    while (queue.length) {
      const job = store.get(queue[0]);
      if (!job || job.state !== 'PENDING') {
        queue.shift();
        continue;
      }
      const worker = pickWorker();
      if (!worker) return;
      queue.shift();
      run(job, worker);
    }
  }

  async function run(job, worker) {
    job.state = 'RUNNING';
    job.attempts++;
    job.worker = worker.address;
    job.started_at = now();
    job.error = '';
    worker.inflight++;
    log(`run    ${job.id.slice(0, 8)} -> ${worker.address} (attempt ${job.attempts}/${job.spec.max_attempts})`);

    let response;
    let failure;
    try {
      response = await call(
        worker.client,
        'Execute',
        {
          job_id: job.id,
          type: job.spec.type,
          payload: job.spec.payload,
          timeout_ms: job.spec.timeout_ms,
        },
        { deadline: now() + job.spec.timeout_ms + 5000 },
      );
    } catch (e) {
      failure = e;
    }
    worker.inflight--;

    if (job.state !== 'RUNNING' || job.worker !== worker.address) {
      dispatch();
      return;
    }

    if (failure) {
      if (failure.code === grpc.status.UNAVAILABLE || failure.code === grpc.status.CANCELLED) {
        worker.healthy = false;
        job.attempts--;
        log(`worker ${worker.address} unavailable; requeueing ${job.id.slice(0, 8)}`);
        requeue(job, 'worker unavailable', 0);
      } else {
        retryOrFail(job, `rpc error: ${failure.details || failure.message}`, true);
      }
    } else if (response.ok) {
      worker.completed++;
      finish(job, 'SUCCEEDED', { result: response.result });
      log(`ok     ${job.id.slice(0, 8)}`);
    } else {
      retryOrFail(job, response.error, response.retryable);
    }
    dispatch();
  }

  function retryOrFail(job, error, retryable) {
    if (retryable && job.attempts < job.spec.max_attempts) {
      const delay = Math.min(100 * 2 ** (job.attempts - 1), 2000);
      log(`retry  ${job.id.slice(0, 8)} in ${delay}ms (${error})`);
      requeue(job, error, delay);
    } else {
      finish(job, 'FAILED', { error });
      log(`failed ${job.id.slice(0, 8)}: ${error}`);
    }
  }

  function requeue(job, error, delayMs) {
    job.state = 'PENDING';
    job.worker = '';
    job.error = error;
    if (delayMs <= 0) {
      queue.unshift(job.id);
      return;
    }
    setTimeout(() => {
      if (job.state === 'PENDING') {
        queue.push(job.id);
        dispatch();
      }
    }, delayMs).unref();
  }

  function finish(job, state, { result = '', error = '' } = {}) {
    job.state = state;
    job.result = result;
    job.error = error;
    job.finished_at = now();
  }

  async function checkHealth() {
    await Promise.all(workers.map(async (worker) => {
      try {
        await call(worker.client, 'Ping', {}, { deadline: now() + 1000 });
        if (!worker.healthy) {
          worker.healthy = true;
          log(`worker ${worker.address} is healthy`);
        }
      } catch {
        if (worker.healthy) {
          worker.healthy = false;
          log(`worker ${worker.address} is DOWN`);
        }
      }
    }));
    dispatch();
  }

  const wrap = (fn) => async (c, callback) => {
    try {
      callback(null, await fn(c.request));
    } catch (e) {
      callback(e.code ? e : { code: grpc.status.INTERNAL, message: e.message });
    }
  };

  async function start(port) {
    await checkHealth();
    healthTimer = setInterval(checkHealth, healthIntervalMs);
    healthTimer.unref();

    server = new grpc.Server();
    server.addService(proto.Scheduler.service, {
      SubmitJob: wrap((r) => ({ id: submit(r.spec) })),
      GetJob: wrap((r) => get(r.id)),
      ListJobs: wrap((r) => ({ jobs: list(r.state, r.limit) })),
      CancelJob: wrap((r) => cancel(r.id)),
      Status: wrap(() => status()),
    });
    await new Promise((resolve, reject) =>
      server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(),
        (err) => (err ? reject(err) : resolve())));
    log(`listening on :${port}, workers: ${workers.map((w) => w.address).join(', ')}`);
  }

  function stop() {
    clearInterval(healthTimer);
    server?.forceShutdown();
    workers.forEach((w) => w.client.close());
  }

  return { start, stop, submit, get, list, cancel, status };
}
