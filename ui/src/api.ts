// 后端 API 封装 + SSE 订阅
export async function api<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'content-type': 'application/json' },
    ...init,
  });
  const body = await res.json();
  if (!body.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body.data as T;
}

export const get = <T,>(p: string) => api<T>(p);
export const post = <T,>(p: string, data?: unknown) =>
  api<T>(p, { method: 'POST', body: data ? JSON.stringify(data) : undefined });
export const patch = <T,>(p: string, data: unknown) =>
  api<T>(p, { method: 'PATCH', body: JSON.stringify(data) });
export const del = <T,>(p: string) => api<T>(p, { method: 'DELETE' });

/** 订阅后端变更事件，返回取消函数 */
export function subscribeChanges(onChange: () => void): () => void {
  const es = new EventSource('/api/events/stream');
  es.addEventListener('change', () => onChange());
  return () => es.close();
}

export interface DagNode {
  id: number;
  name: string;
  owner: string;
  layer: number;
  enabled: number;
  todayStatus: string | null;
  todayDurationMs: number | null;
  todayAttempt: number | null;
}
export interface DagEdge {
  from_task: string;
  to_task: string;
}
export interface GraphData {
  partition: string;
  nodes: DagNode[];
  edges: DagEdge[];
}

export interface InstanceRow {
  id: number;
  task_id: number;
  task_name: string;
  owner: string;
  partition: string;
  status: string;
  attempt: number;
  trigger_type: string;
  duration_ms: number | null;
  error_message: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface BackfillRow {
  id: number;
  name: string;
  date_from: string;
  date_to: string;
  task_name: string | null;
  concurrency: number;
  status: string;
  created_at: string;
}

export interface AlertRow {
  id: number;
  task_name: string | null;
  partition: string | null;
  level: string;
  type: string;
  message: string;
  acknowledged: number;
  created_at: string;
}

export interface TaskRow {
  id: number;
  name: string;
  owner: string;
  description: string;
  executor_type: string;
  executor_config: string;
  retry_max: number;
  timeout_ms: number;
  sla_deadline: string | null;
  sla_duration_ms: number | null;
  enabled: number;
}
