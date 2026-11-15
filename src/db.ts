import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import type { Database as DB } from 'better-sqlite3';

/**
 * SQLite 持久层。
 * 所有状态落盘（WAL + 同步 FULL），调度器重启后据此恢复，
 * 实例状态机的迁移全部用单条条件 UPDATE 做 CAS，保证不会重复拉起。
 */
export class Store {
  db: DB;
  private open = true;
  // 邻接缓存：依赖图变更频率极低，派发热路径上直接命中内存
  private _upstream?: Map<number, number[]>;
  private _downstream?: Map<number, number[]>;
  private _edges?: { from_task_id: number; to_task_id: number }[];

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = FULL');
    this.db.pragma('foreign_keys = ON');
    this.db.pragma('busy_timeout = 5000');
    this.migrate();
  }

  /** 依赖图变更后调用，使邻接缓存失效 */
  invalidateGraphCache() {
    this._upstream = undefined;
    this._downstream = undefined;
    this._edges = undefined;
  }

  get isOpen() {
    return this.open;
  }

  close() {
    if (!this.open) return;
    this.open = false;
    this.db.close();
  }

  /** 便捷委托，供未封装的专用查询使用；关闭后抛错由调用方兜底 */
  prepare(sql: string) {
    if (!this.open) throw new Error('database is closed');
    return this.db.prepare(sql);
  }

  private migrate() {
    this.db.exec(`
    CREATE TABLE IF NOT EXISTS task_def (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      owner TEXT NOT NULL,
      description TEXT NOT NULL DEFAULT '',
      schedule TEXT NOT NULL DEFAULT 'daily',
      executor_type TEXT NOT NULL DEFAULT 'mock',
      executor_config TEXT NOT NULL DEFAULT '{}',
      retry_max INTEGER NOT NULL DEFAULT 2,
      retry_delay_ms INTEGER NOT NULL DEFAULT 1000,
      timeout_ms INTEGER NOT NULL DEFAULT 3600000,
      sla_deadline TEXT,
      sla_duration_ms INTEGER,
      sla_warn_ratio REAL NOT NULL DEFAULT 0.8,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS task_dependency (
      from_task_id INTEGER NOT NULL REFERENCES task_def(id) ON DELETE CASCADE,
      to_task_id   INTEGER NOT NULL REFERENCES task_def(id) ON DELETE CASCADE,
      PRIMARY KEY (from_task_id, to_task_id),
      CHECK (from_task_id <> to_task_id)
    );

    CREATE TABLE IF NOT EXISTS task_instance (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL REFERENCES task_def(id),
      partition TEXT NOT NULL,
      status TEXT NOT NULL,
      attempt INTEGER NOT NULL DEFAULT 0,
      run_token TEXT,
      lease_heartbeat TEXT,
      trigger_type TEXT NOT NULL,
      backfill_id INTEGER,
      queued_at TEXT,
      started_at TEXT,
      finished_at TEXT,
      duration_ms INTEGER,
      next_retry_at TEXT,
      error_message TEXT,
      sla_deadline_at TEXT,
      sla_warned INTEGER NOT NULL DEFAULT 0,
      sla_breached INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      UNIQUE (task_id, partition)
    );
    CREATE INDEX IF NOT EXISTS idx_inst_status ON task_instance(status);
    CREATE INDEX IF NOT EXISTS idx_inst_partition ON task_instance(partition);
    CREATE INDEX IF NOT EXISTS idx_inst_backfill ON task_instance(backfill_id);
    CREATE INDEX IF NOT EXISTS idx_inst_token ON task_instance(run_token);

    CREATE TABLE IF NOT EXISTS backfill (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      date_from TEXT NOT NULL,
      date_to TEXT NOT NULL,
      task_name TEXT,
      include_downstream INTEGER NOT NULL DEFAULT 1,
      concurrency INTEGER NOT NULL DEFAULT 8,
      status TEXT NOT NULL,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL,
      finished_at TEXT
    );

    CREATE TABLE IF NOT EXISTS alert (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER,
      instance_id INTEGER,
      partition TEXT,
      level TEXT NOT NULL,
      channel TEXT NOT NULL DEFAULT 'log',
      type TEXT NOT NULL,
      message TEXT NOT NULL,
      acknowledged INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_alert_ack ON alert(acknowledged, created_at);

    CREATE TABLE IF NOT EXISTS run_event (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      instance_id INTEGER,
      task_id INTEGER,
      partition TEXT,
      event TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_event_inst ON run_event(instance_id);

    -- 单实例领导锁：同一时刻只允许一个调度器持有租约
    CREATE TABLE IF NOT EXISTS scheduler_lock (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      owner_token TEXT NOT NULL,
      heartbeat_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS kv (
      k TEXT PRIMARY KEY,
      v TEXT NOT NULL
    );
    `);
  }

  // ---------- 任务定义 ----------
  getTaskByName(name: string) {
    return this.db.prepare('SELECT * FROM task_def WHERE name = ?').get(name) as
      | Record<string, unknown>
      | undefined;
  }

  allTasks() {
    return this.db.prepare('SELECT * FROM task_def ORDER BY id').all() as Record<string, unknown>[];
  }

  enabledTasks() {
    return this.db
      .prepare('SELECT * FROM task_def WHERE enabled = 1 ORDER BY id')
      .all() as Record<string, unknown>[];
  }

  /** 任务的直接上游 id 列表 */
  upstreamIds(taskId: number): number[] {
    if (this._upstream) return this._upstream.get(taskId) ?? [];
    const up = new Map<number, number[]>();
    const down = new Map<number, number[]>();
    const edges: { from_task_id: number; to_task_id: number }[] = [];
    for (const e of this.db.prepare('SELECT from_task_id, to_task_id FROM task_dependency').all() as { from_task_id: number; to_task_id: number }[]) {
      edges.push(e);
      if (!up.has(e.to_task_id)) up.set(e.to_task_id, []);
      up.get(e.to_task_id)!.push(e.from_task_id);
      if (!down.has(e.from_task_id)) down.set(e.from_task_id, []);
      down.get(e.from_task_id)!.push(e.to_task_id);
    }
    this._upstream = up;
    this._downstream = down;
    this._edges = edges;
    return up.get(taskId) ?? [];
  }

  /** 任务的直接下游 id 列表 */
  downstreamIds(taskId: number): number[] {
    if (this._downstream) return this._downstream.get(taskId) ?? [];
    this.upstreamIds(taskId); // 触发缓存构建
    return this._downstream!.get(taskId) ?? [];
  }

  allEdges(): { from_task_id: number; to_task_id: number }[] {
    if (this._edges) return this._edges;
    this.upstreamIds(-1);
    return this._edges!;
  }

  /** 递归收集某任务的全部下游（含自身可选），按拓扑序返回 */
  collectDownstream(taskId: number, includeSelf = true): number[] {
    const adj = new Map<number, number[]>();
    for (const e of this.allEdges()) {
      if (!adj.has(e.from_task_id)) adj.set(e.from_task_id, []);
      adj.get(e.from_task_id)!.push(e.to_task_id);
    }
    const seen = new Set<number>();
    const stack = includeSelf ? [taskId] : [...(adj.get(taskId) ?? [])];
    while (stack.length) {
      const cur = stack.pop()!;
      if (seen.has(cur)) continue;
      seen.add(cur);
      for (const nxt of adj.get(cur) ?? []) stack.push(nxt);
    }
    return this.topologicalSort().filter((id) => seen.has(id));
  }

  /**
   * Kahn 拓扑排序；同时检测环。
   * 返回任务 id 的拓扑序；有环时抛出错误并给出环上节点。
   */
  topologicalSort(): number[] {
    const tasks = this.allTasks();
    const ids = tasks.map((t) => t.id as number);
    const indeg = new Map<number, number>(ids.map((id) => [id, 0]));
    const adj = new Map<number, number[]>();
    for (const e of this.allEdges()) {
      indeg.set(e.to_task_id, (indeg.get(e.to_task_id) ?? 0) + 1);
      if (!adj.has(e.from_task_id)) adj.set(e.from_task_id, []);
      adj.get(e.from_task_id)!.push(e.to_task_id);
    }
    const queue = ids.filter((id) => indeg.get(id) === 0);
    const order: number[] = [];
    while (queue.length) {
      const cur = queue.shift()!;
      order.push(cur);
      for (const nxt of adj.get(cur) ?? []) {
        const d = (indeg.get(nxt) ?? 0) - 1;
        indeg.set(nxt, d);
        if (d === 0) queue.push(nxt);
      }
    }
    if (order.length !== ids.length) {
      const cyclic = ids.filter((id) => (indeg.get(id) ?? 0) > 0);
      const names = cyclic
        .map((id) => (tasks.find((t) => t.id === id)?.name as string) ?? `#${id}`)
        .join(', ');
      throw new Error(`依赖图存在环，环上节点: ${names}`);
    }
    return order;
  }

  /** 每个任务的 DAG 层级（最长路径长度，根=0），用于界面布局与关键路径计算 */
  layers(): Map<number, number> {
    const order = this.topologicalSort();
    const layer = new Map<number, number>();
    const upOf = new Map<number, number[]>();
    for (const e of this.allEdges()) {
      if (!upOf.has(e.to_task_id)) upOf.set(e.to_task_id, []);
      upOf.get(e.to_task_id)!.push(e.from_task_id);
    }
    for (const id of order) {
      const ups = upOf.get(id) ?? [];
      layer.set(id, ups.length ? Math.max(...ups.map((u) => (layer.get(u) ?? 0) + 1)) : 0);
    }
    return layer;
  }

  // ---------- 运行事件 ----------
  logEvent(opts: {
    instanceId?: number | null;
    taskId?: number | null;
    partition?: string | null;
    event: string;
    detail?: string | null;
  }) {
    this.db
      .prepare(
        'INSERT INTO run_event (instance_id, task_id, partition, event, detail, created_at) VALUES (?,?,?,?,?,?)',
      )
      .run(
        opts.instanceId ?? null,
        opts.taskId ?? null,
        opts.partition ?? null,
        opts.event,
        opts.detail ?? null,
        new Date().toISOString(),
      );
  }
}
