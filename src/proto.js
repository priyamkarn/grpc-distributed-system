import grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

const definition = protoLoader.loadSync(path.join(here, '../proto/jobs.proto'), {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});

export const jobs = grpc.loadPackageDefinition(definition).jobs;
export { grpc };

export const insecure = () => grpc.credentials.createInsecure();

export const call = (client, method, request, options = {}) =>
  new Promise((resolve, reject) =>
    client[method](request, options, (err, res) => (err ? reject(err) : resolve(res))));
