import type { Store } from './db.js';
import { nowIso } from './config.js';

export interface CreateTaskInput {
  name: string;
  owner: string;
  description?: string;
  executor_type?: 'mock' | 'subprocess';
  executor_config?: Record<string, unknown>;
  retry_max?: number;
  retry_delay_ms?: number;
  timeout_ms?: number;
  sla_deadline?: string | null;
  sla_duration_ms?: number | null;
  sla_warn_ratio?: number;
  enabled?: boolean;
  upstreamNames?: string[];
}

/** 任务定义与依赖边的增删改，所有变更后强制做全图环检测 */
export class DagManager {
  constructor(private store: Store) {}

  createTask(input: CreateTaskInput): number {
    if (!/^[a-zA-Z][a-zA-Z0-9_.]{0,127}$/.test(input.name)) {
      throw new Error(`非法任务名: ${input.name}`);
    }
    if (this.store.getTaskByName(input.name)) throw new Error(`任务已存在: ${input.name}`);
    const now = nowIso();
    const info = this.store.db
      .prepare(
        `INSERT INTO task_def
         (name, owner, description, schedule, executor_type, executor_config, retry_max, retry_delay_ms,
          timeout_ms, sla_deadline, sla_duration_ms, sla_warn_ratio, enabled, created_at, updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      )
      .run(
        input.name,
        input.owner,
        input.description ?? '',
        'daily',
        input.executor_type ?? 'mock',
        JSON.stringify(input.executor_config ?? {}),
        input.retry_max ?? 2,
        input.retry_delay_ms ?? 1000,
        input.timeout_ms ?? 3_600_000,
        input.sla_deadline ?? null,
        input.sla_duration_ms ?? null,
        input.sla_warn_ratio ?? 0.8,
        input.enabled === false ? 0 : 1,
        now,
        now,
      );
    const id = Number(info.lastInsertRowid);
    for (const up of input.upstreamNames ?? []) this.addEdge(up, input.name);
    this.store.invalidateGraphCache();
    this.store.topologicalSort(); // 全图环检测
    this.store.logEvent({ taskId: id, event: 'task_created', detail: input.name });
    return id;
  }

  updateTask(name: string, patch: Partial<CreateTaskInput>): void {
    const task = this.store.getTaskByName(name);
    if (!task) throw new Error(`任务不存在: ${name}`);
    const cur = task as Record<string, unknown>;
    const sets: string[] = [];
    const vals: unknown[] = [];
    const map: Record<string, string> = {
      owner: 'owner',
      description: 'description',
      retry_max: 'retry_max',
      retry_delay_ms: 'retry_delay_ms',
      timeout_ms: 'timeout_ms',
      sla_deadline: 'sla_deadline',
      sla_duration_ms: 'sla_duration_ms',
      sla_warn_ratio: 'sla_warn_ratio',
    };
    for (const [k, col] of Object.entries(map)) {
      if (patch[k as keyof CreateTaskInput] !== undefined) {
        sets.push(`${col} = ?`);
        vals.push(patch[k as keyof CreateTaskInput]);
      }
    }
    if (patch.executor_type !== undefined) {
      sets.push('executor_type = ?');
      vals.push(patch.executor_type);
    }
    if (patch.executor_config !== undefined) {
      sets.push('executor_config = ?');
      vals.push(JSON.stringify(patch.executor_config));
    }
    if (patch.enabled !== undefined) {
      sets.push('enabled = ?');
      vals.push(patch.enabled ? 1 : 0);
    }
    if (!sets.length) return;
    sets.push('updated_at = ?');
    vals.push(nowIso(), cur.id);
    this.store.db.prepare(`UPDATE task_def SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
    this.store.logEvent({ taskId: cur.id as number, event: 'task_updated', detail: name });
  }

  addEdge(fromName: string, toName: string) {
    const from = this.store.getTaskByName(fromName);
    const to = this.store.getTaskByName(toName);
    if (!from || !to) throw new Error(`依赖的任务不存在: ${fromName} / ${toName}`);
    if (from.id === to.id) throw new Error('任务不能依赖自身');
    // 提前检测：to 已经是 from 的上游 => 加边必成环
    const ups = this.store.collectDownstream(Number(to.id), true);
    if (ups.includes(Number(from.id))) {
      throw new Error(`添加依赖 ${fromName} -> ${toName} 会形成环`);
    }
    this.store.db
      .prepare('INSERT OR IGNORE INTO task_dependency (from_task_id, to_task_id) VALUES (?,?)')
      .run(from.id, to.id);
    this.store.invalidateGraphCache();
    this.store.topologicalSort();
    this.store.logEvent({ event: 'edge_added', detail: `${fromName} -> ${toName}` });
  }

  removeEdge(fromName: string, toName: string) {
    const from = this.store.getTaskByName(fromName);
    const to = this.store.getTaskByName(toName);
    if (!from || !to) throw new Error('依赖的任务不存在');
    this.store.db
      .prepare('DELETE FROM task_dependency WHERE from_task_id = ? AND to_task_id = ?')
      .run(from.id, to.id);
    this.store.invalidateGraphCache();
    this.store.logEvent({ event: 'edge_removed', detail: `${fromName} -> ${toName}` });
  }

  deleteTask(name: string) {
    const task = this.store.getTaskByName(name);
    if (!task) return;
    this.store.db.prepare('DELETE FROM task_def WHERE id = ?').run(task.id);
    this.store.invalidateGraphCache();
    this.store.logEvent({ event: 'task_deleted', detail: name });
  }

  /** 整图导出（界面/压测脚本用） */
  exportGraph() {
    const tasks = this.store.allTasks();
    const edges = this.store.allEdges();
    const nameById = new Map(tasks.map((t) => [t.id as number, t.name as string]));
    return {
      tasks: tasks.map((t) => ({ ...t, executor_config: JSON.parse(t.executor_config as string) })),
      edges: edges.map((e) => ({
        from: nameById.get(e.from_task_id),
        to: nameById.get(e.to_task_id),
      })),
      layers: Object.fromEntries(
        [...this.store.layers().entries()].map(([id, l]) => [nameById.get(id), l]),
      ),
    };
  }
}
