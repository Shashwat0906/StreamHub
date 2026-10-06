// Thin typed wrappers over the dashboard REST API. Live data does not come
// from here: it is pushed over Server-Sent Events (see live.tsx).
import type { ConsumerView, DLQEntry, ProduceRequest, ProduceResult, TrafficView } from './types';

export class ApiError extends Error {
  constructor(message: string, public status: number) {
    super(message);
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError('Dashboard server unreachable', 0);
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  let data: unknown = undefined;
  try {
    data = text ? JSON.parse(text) : undefined;
  } catch {
    /* non-JSON error body */
  }
  if (!res.ok) {
    const msg = (data as { error?: string } | undefined)?.error ?? (text || res.statusText);
    // Produce failures carry a full ProduceResult; surface its error.
    const perr = (data as ProduceResult | undefined)?.error;
    throw new ApiError(perr ?? msg, res.status);
  }
  return data as T;
}

export const api = {
  createTopic: (name: string, partitions: number, replicationFactor: number, configs?: Record<string, string>) =>
    request<{ name: string }>('POST', '/api/topics', { name, partitions, replicationFactor, configs }),
  deleteTopic: (name: string) => request<void>('DELETE', `/api/topics/${encodeURIComponent(name)}`),
  produce: (req: ProduceRequest) => request<ProduceResult>('POST', '/api/produce', req),
  dlq: () => request<{ entries: DLQEntry[]; maxRetries: number }>('GET', '/api/dlq'),
  retryDLQ: (id: string, payload?: string) => request<ProduceResult>('POST', '/api/dlq/retry', { id, payload }),
  traffic: (t: { running: boolean; topic?: string; rate?: number; failureRate?: number }) =>
    request<TrafficView>('POST', '/api/demo/traffic', t),
  addConsumer: (group: string, topic: string, delayMs: number, strategy: string) =>
    request<ConsumerView>('POST', '/api/demo/consumers', { group, topic, delayMs, strategy }),
  consumerAction: (id: string, action: 'crash' | 'stop' | 'restart') =>
    request<ConsumerView>('POST', `/api/demo/consumers/${id}/${action}`),
  removeConsumer: (id: string) => request<void>('DELETE', `/api/demo/consumers/${id}`),
  brokerAction: (id: number, action: 'kill' | 'stop' | 'start') =>
    request<unknown>('POST', `/api/cluster/brokers/${id}/${action}`),
};

export function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
