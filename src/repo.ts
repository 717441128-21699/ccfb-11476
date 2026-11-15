/** Repository: task/edge/instance queries and guarded state transitions. */
import { getDb, now, plain } from './db.ts';
import type { BackfillJob, EdgeDef, InstanceRun, TaskDef, TaskInstance } from './types.ts';
import { logEvent } from './events.ts';
import { invalidateDagCache } from './dag.ts';

/* ---------------- tasks & edges ---------------- */

export function upsertTask(t: Omit<TaskDef, 'id' | 'createdAt'>): void {
  const db = getDb();
  db.prepare(
    `INSERT INTO tasks (name, owner, command, retries, retry_backoff_ms, expected_duration_ms, sla_ms, sla_warn_ratio)
     VALUES (@name, @owner, @command, @retries, @retryBackoffMs, @expectedDurationMs, @slaMs, @slaWarnRatio)
     ON CONFLICT(name) DO UPDATE SET
       owner=excluded.owner, command=excluded.command, retries=excluded.retries,
       retry_backoff_ms=excluded.retry_backoff_ms, expected_duration_ms=excluded.expected_duration_ms,
       sla_ms=excluded.sla_ms, sla_warn_ratio=excluded.sla_warn_ratio`,
  ).run({
    name: t.name,
    owner: t.owner,
    command: t.command,
    retries: t.retries,
    retryBackoffMs: t.retryBackoffMs,
    expectedDurationMs: t.expectedDurationMs,
    slaMs: t.slaMs,
    slaWarnRatio: t.slaWarnRatio,
  });
  invalidateDagCache();
}

export function addEdge(upstream: string, downstream: string, offsetDays = 0): void {
  getDb()
    .prepare('INSERT OR IGNORE INTO edges (upstream_task, downstream_task, offset_days) VALUES (?, ?, ?)')
    .run(upstream, downstream, offsetDays);
  invalidateDagCache();
}

export function listTasks(): TaskDef[] {
  return getDb().prepare('SELECT * FROM tasks ORDER BY name').all().map(mapTask);
}

export function getTask(name: string): TaskDef | null {
  const row = getDb().prepare('SELECT * FROM tasks WHERE name = ?').get(name);
  return row ? mapTask(row) : null;
}

export function listEdges(): EdgeDef[] {
  return getDb().prepare('SELECT * FROM edges ORDER BY id').all().map(mapEdge);
}

function mapTask(row: unknown): TaskDef {
  const r = plain<Record<string, unknown>>(row);
  return {
    id: Number(r.id),
    name: String(r.name),
    owner: String(r.owner),
    command: String(r.command),
    retries: Number(r.retries),
    retryBackoffMs: Number(r.retry_backoff_ms),
    expectedDurationMs: Number(r.expected_duration_ms),
    slaMs: r.sla_ms === null ? null : Number(r.sla_ms),
    slaWarnRatio: Number(r.sla_warn_ratio),
    createdAt: String(r.created_at),
  };
}

function mapEdge(row: unknown): EdgeDef {
  const r = plain<Record<string, unknown>>(row);
  return {
    id: Number(r.id),
    upstreamTask: String(r.upstream_task),
    downstreamTask: String(r.downstream_task),
    offsetDays: Number(r.offset_days),
  };
}

/* ---------------- instances ---------------- */

export function getOrCreateInstance(
  taskName: string,
  logicalDate: string,
  backfillId: number | null,
): { instance: TaskInstance; created: boolean } {
  const db = getDb();
  const existing = db.prepare('SELECT * FROM instances WHERE task_name = ? AND logical_date = ?').get(taskName, logicalDate);
  if (existing) return { instance: mapInstanceRow(existing), created: false };
  db.prepare(
    `INSERT INTO instances (task_name, logical_date, status, backfill_id, enqueued_at)
     VALUES (?, ?, 'PENDING', ?, ?)`,
  ).run(taskName, logicalDate, backfillId, now());
  const row = db.prepare('SELECT * FROM instances WHERE task_name = ? AND logical_date = ?').get(taskName, logicalDate);
  const inst = mapInstanceRow(row);
  logEvent('INSTANCE_ENQUEUED', { instanceId: inst.id, taskName, logicalDate, detail: backfillId ? `backfill=${backfillId}` : null });
  return { instance: inst, created: true };
}

export function getInstance(id: number): TaskInstance | null {
  const row = getDb().prepare('SELECT * FROM instances WHERE id = ?').get(id);
  return row ? mapInstanceRow(row) : null;
}

export function listInstances(filter: { date?: string; task?: string; status?: string; limit?: number } = {}): TaskInstance[] {
  const where: string[] = [];
  const params: unknown[] = [];
  if (filter.date) { where.push('logical_date = ?'); params.push(filter.date); }
  if (filter.task) { where.push('task_name = ?'); params.push(filter.task); }
  if (filter.status) { where.push('status = ?'); params.push(filter.status); }
  const sql = `SELECT * FROM instances ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY id DESC LIMIT ?`;
  params.push(filter.limit ?? 500);
  return getDb().prepare(sql).all(...(params as (string | number)[])).map((r) => mapInstanceRow(r));
}

export function mapInstanceRow(row: unknown): TaskInstance {
  const r = plain<Record<string, unknown>>(row);
  return {
    id: Number(r.id),
    taskName: String(r.task_name),
    logicalDate: String(r.logical_date),
    status: String(r.status) as TaskInstance['status'],
    attempt: Number(r.attempt),
    retryAfter: (r.retry_after as string | null) ?? null,
    enqueuedAt: String(r.enqueued_at),
    startedAt: (r.started_at as string | null) ?? null,
    finishedAt: (r.finished_at as string | null) ?? null,
    durationMs: (r.duration_ms as number | null) ?? null,
    totalRuntimeMs: Number(r.total_runtime_ms ?? 0),
    backfillId: (r.backfill_id as number | null) ?? null,
    note: (r.note as string | null) ?? null,
  };
}

/** Reset an instance back to PENDING so the scheduler picks it up again (used by rerun & manual retry). */
export function resetInstance(id: number, note: string): void {
  const db = getDb();
  const inst = getInstance(id);
  if (!inst) throw new Error(`instance ${id} not found`);
  if (inst.status === 'RUNNING') throw new Error(`instance ${id} is RUNNING; kill it before rerun`);
  db.prepare(
    `UPDATE instances SET status='PENDING', attempt=0, retry_after=NULL, started_at=NULL,
       finished_at=NULL, duration_ms=NULL, total_runtime_ms=0, note=? WHERE id=?`,
  ).run(note, id);
  logEvent('INSTANCE_RESET', { instanceId: id, taskName: inst.taskName, logicalDate: inst.logicalDate, detail: note });
}

export function countInstancesByStatus(backfillId: number | null): Record<string, number> {
  const db = getDb();
  const rows = backfillId === null
    ? db.prepare(`SELECT status, COUNT(*) c FROM instances GROUP BY status`).all()
    : db.prepare(`SELECT status, COUNT(*) c FROM instances WHERE backfill_id = ? GROUP BY status`).all(backfillId);
  const out: Record<string, number> = {};
  for (const r of rows) { const p = plain<{ status: string; c: number }>(r); out[p.status] = p.c; }
  return out;
}

/* ---------------- runs ---------------- */

export function insertRun(instanceId: number, attempt: number, pid: number, markerPath: string): InstanceRun {
  const db = getDb();
  const info = db
    .prepare(
      `INSERT INTO instance_runs (instance_id, attempt, pid, status, marker_path, started_at, heartbeat_at)
       VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)`,
    )
    .run(instanceId, attempt, pid, markerPath, now(), now());
  return mapRunRow(db.prepare('SELECT * FROM instance_runs WHERE id = ?').get(info.lastInsertRowid));
}

export function listRuns(instanceId: number): InstanceRun[] {
  return getDb().prepare('SELECT * FROM instance_runs WHERE instance_id = ? ORDER BY id').all(instanceId)
    .map((r) => plain<InstanceRun>(mapRunRow(r)));
}

export function runningRuns(): InstanceRun[] {
  return getDb().prepare("SELECT * FROM instance_runs WHERE status = 'RUNNING' ORDER BY id").all()
    .map((r) => plain<InstanceRun>(mapRunRow(r)));
}

function mapRunRow(row: unknown): InstanceRun {
  const r = plain<Record<string, unknown>>(row);
  return {
    id: Number(r.id),
    instanceId: Number(r.instance_id),
    attempt: Number(r.attempt),
    pid: (r.pid as number | null) ?? null,
    status: String(r.status) as InstanceRun['status'],
    markerPath: (r.marker_path as string | null) ?? null,
    startedAt: String(r.started_at),
    heartbeatAt: (r.heartbeat_at as string | null) ?? null,
    finishedAt: (r.finished_at as string | null) ?? null,
    exitCode: (r.exit_code as number | null) ?? null,
    stdoutTail: (r.stdout_tail as string | null) ?? null,
    stderrTail: (r.stderr_tail as string | null) ?? null,
  };
}

/* ---------------- backfills ---------------- */

export function createBackfill(row: Omit<BackfillJob, 'id' | 'createdAt' | 'finishedAt' | 'status'> & { status?: BackfillJob['status'] }): BackfillJob {
  const db = getDb();
  const info = db
    .prepare(
      `INSERT INTO backfills (start_date, end_date, root_task, rerun_mode, max_concurrency, max_per_partition,
         parallel_partitions, status, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      row.startDate, row.endDate, row.rootTask ?? null, row.rerunMode, row.maxConcurrency,
      row.maxPerPartition, row.parallelPartitions, row.status ?? 'RUNNING', now(),
    );
  return plain<BackfillJob>(mapBackfillRow(db.prepare('SELECT * FROM backfills WHERE id = ?').get(info.lastInsertRowid)));
}

export function getBackfill(id: number): BackfillJob | null {
  const row = getDb().prepare('SELECT * FROM backfills WHERE id = ?').get(id);
  return row ? plain<BackfillJob>(mapBackfillRow(row)) : null;
}

export function listBackfills(): BackfillJob[] {
  return getDb().prepare('SELECT * FROM backfills ORDER BY id DESC').all().map((r) => plain<BackfillJob>(mapBackfillRow(r)));
}

function mapBackfillRow(row: unknown): BackfillJob {
  const r = plain<Record<string, unknown>>(row);
  return {
    id: Number(r.id),
    startDate: String(r.start_date),
    endDate: String(r.end_date),
    rootTask: (r.root_task as string | null) ?? null,
    rerunMode: Number(r.rerun_mode),
    maxConcurrency: Number(r.max_concurrency),
    maxPerPartition: Number(r.max_per_partition),
    parallelPartitions: Number(r.parallel_partitions),
    status: String(r.status) as BackfillJob['status'],
    createdAt: String(r.created_at),
    finishedAt: (r.finished_at as string | null) ?? null,
  };
}

export function finishBackfill(id: number, status: BackfillJob['status']): void {
  getDb().prepare("UPDATE backfills SET status = ?, finished_at = ? WHERE id = ?").run(status, now(), id);
}
