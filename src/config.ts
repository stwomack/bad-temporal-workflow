import { readFileSync } from 'node:fs';
import type { ConnectionOptions } from '@temporalio/client';
import type { NativeConnectionOptions } from '@temporalio/worker';

export const TASK_QUEUE = 'cursed-orders';

export interface TemporalCloudConfig {
  address: string;
  namespace: string;
  apiKey?: string;
  tlsCertPath?: string;
  tlsKeyPath?: string;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable ${name}`);
  }
  return value;
}

// Reads Temporal Cloud settings from the environment. Auth is either an API key
// (TEMPORAL_API_KEY) or mTLS (TEMPORAL_TLS_CERT + TEMPORAL_TLS_KEY file paths).
export function loadConfig(): TemporalCloudConfig {
  const config: TemporalCloudConfig = {
    address: required('TEMPORAL_ADDRESS'),
    namespace: required('TEMPORAL_NAMESPACE'),
    apiKey: process.env.TEMPORAL_API_KEY,
    tlsCertPath: process.env.TEMPORAL_TLS_CERT,
    tlsKeyPath: process.env.TEMPORAL_TLS_KEY,
  };

  const hasMtls = Boolean(config.tlsCertPath && config.tlsKeyPath);
  if (!config.apiKey && !hasMtls) {
    throw new Error('Set TEMPORAL_API_KEY, or both TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY');
  }
  return config;
}

// Shared by the client and the worker; both option types accept the same shape.
export function connectionOptions(config: TemporalCloudConfig): ConnectionOptions & NativeConnectionOptions {
  if (config.apiKey) {
    return {
      address: config.address,
      tls: true,
      apiKey: config.apiKey,
      metadata: { 'temporal-namespace': config.namespace },
    };
  }
  return {
    address: config.address,
    tls: {
      clientCertPair: {
        crt: readFileSync(config.tlsCertPath!),
        key: readFileSync(config.tlsKeyPath!),
      },
    },
  };
}
