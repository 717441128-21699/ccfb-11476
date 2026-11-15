/**
 * Backfill & rerun orchestration.
 *
 * createBackfill  — requirement 3: pick a date range, run the whole DAG per partition in
 *                   dependency order with bounded parallelism (global cap, per-partition cap,
 *                   sliding window of N parallel partitions).
 * rerunDownstream — requirement 4: one click resets one task + every descendant for the chosen
 *                   partitions; unrelated tasks keep their existing SUCCESS instances and are
 *                   reused as dependencies.
 */
import { getDb, now } from './db.ts';
import { loadDag } from './dag.ts';
import { addDays, listDates } from './util/dates.ts';
import { config } from './config.ts';
import { createBackfill, getOrCreateInstance, mapInstanceRow, resetInstance } from './repo.ts';
import { logEvent } from './events.ts';
import { killProcess } from './executor.ts';
import type { BackfillJob, TaskInstance } from './types.ts';

/**
 * Earliest partition that must exist for a backfill beginning at `startDate`.
 * Follows cross-partition upstream edges transitively: if task B@d depends on A@d-1 and
 * A@d-1 itself depends on X@(d-1)-1, the backfill must reach d-2.
 */
function computeReachableStart(dag: ReturnType<typeof loadDag>, startDate: string): string {
  let earliest = startDate;
  // BFS over (task, accumulatedOffset) reachable by walking upstream edges from every task
  const seen = new Set<string>();
  const queue: Array<{ task: string; offset: number }> = dag.topo.map((t) => ({ task: t, offset: 0 }));
  while (queue.length) {
    const cur = queue.shift()!;
    const key = `${cur.task}:${cur.offset}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (cur.offset < 0) {
      const candidate = addDays(startDate, cur.offset);
      if (candidate < earliest) earliest = candidate;
    }
    const node = dag.nodes.get(cur.task);
    if (!node) continue;
    for (const edge of node.upstreams) {
      queue.push({ task: edge.upstreamTask, offset: cur.offset + edge.offsetDays });
    }
  }
  return earliest;
}

export interface BackfillParams {
  startDate: string;
  endDate: string;
  rootTask?: string | null;
  rerunMode?: boolean;
  maxConcurrency?: number;
  maxPerPartition?: number;
  parallelPartitions?: number;
}

export interface BackfillResult {
  job: BackfillJob;
  enqueued: number;
  reset: number;
  reused: number;
}

export function startBackfill(params: BackfillParams): BackfillResult {
  const dag = loadDag();
  // Cross-partition edges (offset<0) reference earlier partitions, possibly nested
  // (a -1 edge can chain into another -1 edge). Walk the dependency closure to find the
  // deepest historical partition actually reachable, and prime every partition in between.
  // This mirrors a real warehouse where a backfill [s,e] must also produce the historical
  // partitions its DAG reads from.
  const effectiveStart = computeReachableStart(dag, params.startDate);
  const dates = listDates(effectiveStart, params.endDate);
  const requestedStart = params.startDate;

  // rerun mode restricts the scope to a subtree; plain backfill covers the whole DAG
  const scope: string[] = params.rootTask ? dag.subtree(params.rootTask) : dag.topo;
  const scopeSet = new Set(scope);

  const job = createBackfill({
    startDate: params.startDate,
    endDate: params.endDate,
    rootTask: params.rootTask ?? null,
    rerunMode: params.rerunMode ? 1 : 0,
    maxConcurrency: params.maxConcurrency ?? config.defaultMaxConcurrency,
    maxPerPartition: params.maxPerPartition ?? config.defaultMaxPerPartition,
    parallelPartitions: params.parallelPartitions ?? config.defaultBackfillPartitions,
  });

  const db = getDb();
  let enqueued = 0;
  let reset = 0;
  let reused = 0;

  for (const date of dates) {
    const isHistoricalPrimer = date < requestedStart;
    for (const taskName of dag.topo) {
      if (!scopeSet.has(taskName)) continue; // outside rerun subtree: untouched
      const { instance, created } = getOrCreateInstance(taskName, date, job.id);
      // tag every scoped instance with this backfill so completion accounting is exact
      db.prepare('UPDATE instances SET backfill_id = ? WHERE id = ? AND backfill_id IS NULL').run(job.id, instance.id);
      if (created) {
        enqueued++;
        continue;
      }
      if (instance.status === 'PENDING' || instance.status === 'RUNNING') {
        enqueued++; // already queued / in flight for this partition
        continue;
      }
      if (params.rerunMode) {
        resetInstance(instance.id, `rerun from ${params.rootTask ?? '?'} backfill #${job.id}`);
        // reassign ownership so this backfill tracks the rerun to completion and its
        // sliding-window accounting includes the reset subtree
        db.prepare('UPDATE instances SET backfill_id = ? WHERE id = ?').run(job.id, instance.id);
        reset++;
      } else {
        // plain backfill: successful history is reused as-is; failures are retried
        if (instance.status === 'SUCCESS') {
          reused++;
        } else {
          resetInstance(instance.id, `backfill #${job.id} retry of ${instance.status}`);
          db.prepare('UPDATE instances SET backfill_id = ? WHERE id = ?').run(job.id, instance.id);
          reset++;
        }
      }
    }
    if (isHistoricalPrimer) {
      // primer partitions should finish first regardless of the sliding-window setting
      logEvent('BACKFILL_PRIMER_PARTITION', { logicalDate: date, detail: `backfill=${job.id}` });
    }
  }

  logEvent('BACKFILL_CREATED', {
    detail: `id=${job.id} dates=${dates.length} scope=${scope.length} enqueued=${enqueued} reset=${reset} reused=${reused}`,
  });
  return { job, enqueued, reset, reused };
}

/** Requirement 4: rerun one task and all its downstream tasks for a set of partitions. */
export function rerunDownstream(rootTask: string, dates: string[]): BackfillResult {
  const dag = loadDag();
  if (!dag.nodes.has(rootTask)) throw new Error(`unknown task ${rootTask}`);
  if (dates.length === 0) throw new Error('at least one partition date is required');
  const start = dates[0]!;
  const end = dates[dates.length - 1]!;
  return startBackfill({
    startDate: start,
    endDate: end,
    rootTask,
    rerunMode: true,
  });
}

/** Manual single-instance retry from the UI/API. */
export function retryInstance(instanceId: number): TaskInstance {
  const inst = dbGetInstance(instanceId);
  if (inst.status === 'RUNNING') throw new Error('instance is already running');
  resetInstance(instanceId, 'manual retry via API');
  logEvent('INSTANCE_MANUAL_RETRY', { instanceId, taskName: inst.taskName, logicalDate: inst.logicalDate });
  return dbGetInstance(instanceId);
}

/** Kill a running instance (best effort; marks KILLED). */
export function killInstance(instanceId: number): void {
  const db = getDb();
  const inst = dbGetInstance(instanceId);
  if (inst.status !== 'RUNNING') throw new Error(`instance is ${inst.status}, not RUNNING`);
  const runs = db.prepare("SELECT id FROM instance_runs WHERE instance_id=? AND status='RUNNING'").all(instanceId) as Array<{ id: number }>;
  for (const r of runs) {
    killProcess(r.id);
    db.prepare("UPDATE instance_runs SET status='LOST', finished_at=? WHERE id=?", ).run(now(), r.id);
  }
  db.prepare("UPDATE instances SET status='KILLED', finished_at=?, note='killed by operator' WHERE id=?", ).run(now(), instanceId);
  logEvent('INSTANCE_KILLED', { instanceId, taskName: inst.taskName, logicalDate: inst.logicalDate });
}

function dbGetInstance(id: number): TaskInstance {
  const row = getDb().prepare('SELECT * FROM instances WHERE id=?').get(id);
  if (!row) throw new Error(`instance ${id} not found`);
  return mapInstanceRow(row);
}
