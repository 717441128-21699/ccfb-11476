/**
 * Core scheduler.
 *
 * Design:
 *  - single scheduler process (guarded by a lock file); event-triggered scheduling + 100ms safety tick
 *  - every dispatch is an atomic claim transaction (UPDATE ... WHERE status='PENDING') so the same
 *    instance can never be launched twice, even under overlapping ticks
 *  - attempts are OS processes tracked in instance_runs with marker files + heartbeats;
 *    after restart, recoverOrphans() reconciles via pid/marker instead of blindly relaunching
 *  - partitions are independent DAG invocations; offset edges model cross-date dependencies
 */
import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.ts';
import { beginTx, commit, getDb, now, rollback } from './db.ts';
import { parseTs } from './util/dates.ts';
import { loadDag } from './dag.ts';
import { addDays } from './util/dates.ts';
import { raiseAlert } from './alerts.ts';
import { logEvent } from './events.ts';
import {
  finishBackfill,
  getBackfill,
  getInstance,
  getOrCreateInstance,
  getTask,
  listBackfills,
} from './repo.ts';
import {
  activeCount,
  activeRunIds,
  getActiveProcess,
  insertRunHeartbeat,
  isPidAlive,
  markerPathFor,
  persistRunOutput,
  readMarker,
  removeActive,
  spawnAttempt,
} from './executor.ts';
import type { BackfillJob, InstanceStatus, TaskInstance } from './types.ts';

const LOCK_FILE = path.join(config.dataDir, 'scheduler.lock');

export interface SchedulerOptions {
  /** automatically create today's partition instances (long-running server mode) */
  autoDaily?: boolean;
  /** stop after the scheduler becomes idle (no pending/running) — used by batch CLI runs & tests */
  stopWhenIdle?: boolean;
  idleGraceMs?: number;
}

export class Scheduler {
  private timer: NodeJS.Timeout | null = null;
  private lockRefresh: NodeJS.Timeout | null = null;
  private ticking = false;
  /** set when a trigger() arrives while a tick is running; the tick re-runs immediately after */
  private tickRequested = false;
  private stopped = false;
  private lastHeartbeatFlush = 0;
  private idleSince: number | null = null;
  private lockToken: string | null = null;
  private onDone: (() => void) | null = null;

  private opts: SchedulerOptions;
  constructor(opts: SchedulerOptions = {}) {
    this.opts = opts;
  }

  /* ---------------- lifecycle ---------------- */

  start(onDone?: () => void): void {
    this.acquireLock();
    this.onDone = onDone ?? null;
    this.recoverOrphans();
    logEvent('SCHEDULER_STARTED', { detail: `pid=${process.pid} autoDaily=${!!this.opts.autoDaily}` });
    // Tick cadence depends on execution mode: subprocess ticks must be sparse because each
    // spawn blocks the loop (~100ms); in-process ticks are cheap and tiny tasks finish in
    // milliseconds, so a fast cadence keeps layer transitions from stalling the DAG.
    const interval = config.executionMode === 'inprocess'
      ? Math.min(config.tickIntervalMs, 50)
      : config.tickIntervalMs;
    this.timer = setInterval(() => void this.tick(), interval);
    // NOTE: deliberately NOT unref'd — the scheduler timer must keep the event loop
    // alive between dispatch waves, otherwise the process exits with PENDING work.
    // run the first tick synchronously-ish and surface fatal errors instead of letting
    // the process exit silently with work still PENDING
    queueMicrotask(() => {
      this.tick().catch((err) => {
        console.error('[scheduler] initial tick failed:', err);
        process.exitCode = 1;
      });
    });
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.releaseLock();
    logEvent('SCHEDULER_STOPPED', { detail: `pid=${process.pid}` });
  }

  /** immediate scheduling pass after an external event (instance reset, backfill created, child exit) */
  trigger(): void {
    if (this.stopped) return;
    // If a tick is already running, queue one more pass instead of dropping the event:
    // this keeps the dependency cascade near-instant rather than waiting for the next timer.
    if (this.ticking) this.tickRequested = true;
    else void this.tick();
  }

  private acquireLock(): void {
    fs.mkdirSync(config.dataDir, { recursive: true });
    if (fs.existsSync(LOCK_FILE)) {
      const token = fs.readFileSync(LOCK_FILE, 'utf8');
      const [pidStr, tsStr] = token.split('|');
      const pid = Number(pidStr);
      const ts = Number(tsStr);
      if (pid && isPidAlive(pid) && Date.now() - ts < 60_000) {
        throw new Error(`another scheduler is running (pid ${pid}); remove ${LOCK_FILE} only if you are sure it is dead`);
      }
    }
    this.lockToken = `${process.pid}|${Date.now()}`;
    fs.writeFileSync(LOCK_FILE, this.lockToken);
    this.lockRefresh = setInterval(() => {
      try { fs.writeFileSync(LOCK_FILE, `${process.pid}|${Date.now()}`); } catch { /* shutting down */ }
    }, 15_000);
    this.lockRefresh.unref?.();
  }

  private releaseLock(): void {
    if (this.lockRefresh) clearInterval(this.lockRefresh);
    this.lockRefresh = null;
    try {
      if (fs.existsSync(LOCK_FILE) && fs.readFileSync(LOCK_FILE, 'utf8') === this.lockToken) {
        fs.rmSync(LOCK_FILE);
      }
    } catch { /* best effort */ }
  }

  /* ---------------- restart recovery (requirement 6) ---------------- */

  recoverOrphans(): void {
    const db = getDb();
    const runs = db.prepare("SELECT * FROM instance_runs WHERE status='RUNNING' ORDER BY id").all() as Array<Record<string, unknown>>;
    if (runs.length === 0) return;
    console.warn(`[scheduler] recovering ${runs.length} in-flight run(s) after restart...`);

    for (const row of runs) {
      const runId = Number(row.id);
      const instanceId = Number(row.instance_id);
      const pid = (row.pid as number | null) ?? null;
      const markerPath = (row.marker_path as string | null) ?? markerPathFor(runId);
      const marker = readMarker(markerPath);
      const alive = pid !== null && isPidAlive(pid);

      if (alive && marker && marker.status === 'RUNNING') {
        // child survived the scheduler restart: adopt by polling its marker — never spawned twice
        this.adoptProcess(runId, instanceId, pid, markerPath);
        db.prepare('UPDATE instance_runs SET heartbeat_at = ? WHERE id = ?').run(now(), runId);
        logEvent('RUN_ADOPTED', { instanceId, detail: `run=${runId} pid=${pid}` });
        continue;
      }

      // child is gone — trust the marker's terminal state if it wrote one
      const succeeded = marker?.status === 'SUCCESS';
      const exitCode = succeeded ? 0 : (marker?.exitCode ?? null);
      const note = marker?.error
        ?? (alive ? 'marker missing while process alive; marked LOST' : 'process died with no terminal marker (scheduler crash)');
      db.prepare(
        "UPDATE instance_runs SET status=?, finished_at=?, exit_code=?, heartbeat_at=? WHERE id=?",
      ).run(succeeded ? 'SUCCESS' : 'FAILED', now(), exitCode, now(), runId);
      logEvent('RUN_REAPED', {
        instanceId,
        detail: `run=${runId} marker=${marker?.status ?? 'none'} alive=${alive} note=${note}`,
      });
      this.resolveInstanceAfterExit(instanceId, runId, succeeded ? 0 : (exitCode ?? 1), note, true);
    }
  }

  /** Re-attach to a surviving child process by polling its marker (no ChildProcess handle survives restart). */
  private adoptProcess(runId: number, instanceId: number, pid: number, markerPath: string): void {
    const poll = setInterval(() => {
      const marker = readMarker(markerPath);
      const alive = isPidAlive(pid);
      if (marker && marker.status !== 'RUNNING') {
        clearInterval(poll);
        const db = getDb();
        db.prepare("UPDATE instance_runs SET status=?, finished_at=?, exit_code=?, heartbeat_at=? WHERE id=?",
        ).run(marker.status, now(), marker.exitCode, now(), runId);
        this.resolveInstanceAfterExit(instanceId, runId, marker.exitCode ?? (marker.status === 'SUCCESS' ? 0 : 1), marker.error ?? null, false);
      } else if (!alive) {
        clearInterval(poll);
        getDb().prepare("UPDATE instance_runs SET status='LOST', finished_at=?, heartbeat_at=? WHERE id=?", ).run(now(), now(), runId);
        this.resolveInstanceAfterExit(instanceId, runId, 1, 'process vanished (no marker)', true);
      } else {
        getDb().prepare('UPDATE instance_runs SET heartbeat_at = ? WHERE id = ?').run(now(), runId);
      }
    }, config.heartbeatIntervalMs);
    poll.unref?.();
  }

  /* ---------------- main tick ---------------- */

  async tick(): Promise<void> {
    if (this.ticking || this.stopped) return;
    this.ticking = true;
    this.statusCache = this.buildStatusCache();
    const er = getDb().prepare('SELECT MIN(logical_date) d FROM instances').get() as { d: string | null } | undefined;
    this.earliestPartition = er?.d ?? null;
    try {
      this.refreshHeartbeats();
      if (this.opts.autoDaily) this.enqueueDailyPartitions();
      this.markUpstreamFailures();
      this.dispatchReady();
      this.checkSlas();
      this.reconcileBackfills();
      getDb().prepare('UPDATE scheduler_meta SET last_tick_at = ? WHERE id = 1').run(now());
      this.maybeStopWhenIdle();
    } catch (err) {
      console.error('[scheduler] tick error:', err);
    } finally {
      this.ticking = false;
      this.statusCache = null;
      // A completion arrived during this tick and may have unblocked dependents: run another
      // pass immediately (microtask-deferred so the current stack unwinds first). This is what
      // keeps the DAG cascade near-instant instead of stalling until the next timer interval.
      if (this.tickRequested && !this.stopped) {
        this.tickRequested = false;
        queueMicrotask(() => void this.tick());
      }
    }
  }

  private refreshHeartbeats(): void {
    if (Date.now() - this.lastHeartbeatFlush < config.heartbeatIntervalMs) return;
    this.lastHeartbeatFlush = Date.now();
    for (const runId of activeRunIds()) {
      const proc = getActiveProcess(runId);
      if (!proc) continue;
      // in-process tasks share the scheduler pid and are by definition alive; real child
      // processes must pass the liveness check before we refresh their lease
      const alive = proc.inProcess || (proc.child?.pid !== undefined && proc.child.pid !== null && isPidAlive(proc.child.pid));
      if (alive) insertRunHeartbeat(runId);
    }
  }

  /* ---------------- daily auto enqueue ---------------- */

  /**
   * Ensure every task has an instance for today's partition (UTC).
   * Guard against racing an in-flight backfill: a today-instance is only enqueued once
   * every cross-partition (offset<0) upstream partition it depends on already exists.
   * Otherwise it could be dispatched while its historical upstream is still pending,
   * violating dependency order. Same-date upstreams are created here too and gate normally.
   */
  private enqueueDailyPartitions(): void {
    const dag = loadDag();
    const today = new Date().toISOString().slice(0, 10);
    const db = getDb();
    const instanceExists = (task: string, date: string): boolean =>
      db.prepare('SELECT 1 FROM instances WHERE task_name = ? AND logical_date = ?').get(task, date) !== undefined;
    for (const name of dag.topo) {
      if (instanceExists(name, today)) continue;
      const node = dag.nodes.get(name);
      const historicalReady = (node?.upstreams ?? []).every((edge) => {
        if (edge.offsetDays >= 0) return true; // same-date upstream is created in this same loop
        const upDate = addDays(today, edge.offsetDays);
        return instanceExists(edge.upstreamTask, upDate);
      });
      if (!historicalReady) continue; // wait for a backfill to produce the historical partition
      getOrCreateInstance(name, today, null);
    }
  }

  /* ---------------- dependency resolution ---------------- */

  /**
   * Per-tick cache of instance status, keyed `task|date`. Dependency resolution used to
   * issue one SQL query per upstream edge per PENDING task (quadratic over a partition);
   * loading the status map once per tick makes resolution O(edges) in-memory.
   */
  private statusCache: Map<string, InstanceStatus> | null = null;

  private buildStatusCache(): Map<string, InstanceStatus> {
    const rows = getDb()
      .prepare('SELECT task_name, logical_date, status FROM instances')
      .all() as Array<{ task_name: string; logical_date: string; status: InstanceStatus }>;
    const map = new Map<string, InstanceStatus>();
    for (const r of rows) map.set(`${r.task_name}|${r.logical_date}`, r.status);
    return map;
  }

  /** Earliest partition across the whole instance set; pre-backfill history is assumed ready. */
  private earliestPartition: string | null = null;

  /** Decide whether a PENDING instance is dispatchable. */
  private dependencyState(inst: TaskInstance): { ready: boolean; reason?: string; failedUpstream?: string } {
    const dag = loadDag();
    const node = dag.nodes.get(inst.taskName);
    if (!node) return { ready: false, reason: 'task missing from DAG' };
    const cache = this.statusCache ?? this.buildStatusCache();
    for (const edge of node.upstreams) {
      const upDate = addDays(inst.logicalDate, edge.offsetDays);
      // cross-partition edge reaching into pre-backfill history: assume data already exists
      if (this.earliestPartition !== null && upDate < this.earliestPartition) continue;
      const status = cache.get(`${edge.upstreamTask}|${upDate}`);
      if (!status) return { ready: false, reason: `waiting ${edge.upstreamTask}@${upDate} (not enqueued)` };
      if (status === 'SUCCESS') continue;
      if (status === 'FAILED' || status === 'UPSTREAM_FAILED' || status === 'KILLED') {
        return { ready: false, failedUpstream: `${edge.upstreamTask}@${upDate}=${status}` };
      }
      return { ready: false, reason: `waiting ${edge.upstreamTask}@${upDate}=${status}` };
    }
    return { ready: true };
  }

  private markUpstreamFailures(): void {
    // Only instances whose upstream contains a terminal-failed partition can transition.
    // Resolve those directly via edges instead of scanning every PENDING instance's upstreams.
    const db = getDb();
    const failed = db
      .prepare("SELECT task_name, logical_date FROM instances WHERE status IN ('FAILED','UPSTREAM_FAILED','KILLED')")
      .all() as Array<{ task_name: string; logical_date: string }>;
    if (failed.length === 0) return;
    const dag = loadDag();
    for (const f of failed) {
      const node = dag.nodes.get(f.task_name);
      if (!node) continue;
      for (const edge of node.downstreams) {
        const downDate = addDays(f.logical_date, -edge.offsetDays);
        const key = `${edge.downstreamTask}|${downDate}`;
        if (this.statusCache?.get(key) !== 'PENDING') continue;
        const row = db.prepare('SELECT * FROM instances WHERE task_name=? AND logical_date=?').get(edge.downstreamTask, downDate) as Record<string, unknown> | undefined;
        if (!row) continue;
        const inst = plainInstance(row);
        const dep = this.dependencyState(inst);
        if (dep.failedUpstream) {
          const info = db.prepare(
            "UPDATE instances SET status='UPSTREAM_FAILED', finished_at=?, note=? WHERE id=? AND status='PENDING'",
          ).run(now(), `upstream failed: ${dep.failedUpstream}`, inst.id);
          if (info.changes > 0) {
            this.statusCache?.set(`${inst.taskName}|${inst.logicalDate}`, 'UPSTREAM_FAILED');
            logEvent('INSTANCE_UPSTREAM_FAILED', { instanceId: inst.id, taskName: inst.taskName, logicalDate: inst.logicalDate, detail: dep.failedUpstream });
          }
        }
      }
    }
  }

  /* ---------------- dispatch ---------------- */

  private dispatchReady(): void {
    const db = getDb();
    const currentActive = activeCount();
    // The effective global cap is the largest concurrency limit among active backfills
    // (falling back to the scheduler default). A backfill may legitimately request a
    // higher cap than the default, so the fast path must not hard-code the default.
    const bfCapRow = db
      .prepare('SELECT MAX(max_concurrency) c FROM backfills WHERE status = \'RUNNING\'')
      .get() as { c: number | null } | undefined;
    const effectiveGlobalCap = bfCapRow?.c ?? config.defaultMaxConcurrency;
    if (currentActive >= effectiveGlobalCap) return; // global cap fast path (requirement 3)
    const freeSlots = effectiveGlobalCap - currentActive;

    // Earliest partition of any backfill: cross-partition upstreams pointing before it are
    // pre-existing history and treated as satisfied (a backfill cannot reach further back
    // than its own first partition, and warehouse data older than it is assumed available).
    const earliestPartition = this.earliestPartition;

    // Find dispatchable candidates in SQL rather than scanning every PENDING row in JS.
    // LEFT JOIN keeps upstream edges whose partition instance does not exist; those are
    // blocking only if their date lies within the backfilled range (>= earliestPartition).
    // An existing upstream blocks unless already SUCCESS.
    const candidateLimit = Math.max(freeSlots * 2, 20);
    // Build a valid SQLite date modifier per edge: negative offset -> '-N days',
    // non-negative -> '+N days'. Naive '+' || offset produces '+-1 days' for negatives,
    // which SQLite fails to parse (returns NULL and silently drops the dependency).
    const modifier = "(CASE WHEN e.offset_days < 0 THEN '-' || (-e.offset_days) ELSE '+' || e.offset_days END || ' days')";
    const rows = db
      .prepare(
        `SELECT i.* FROM instances i
         WHERE i.status='PENDING' AND (i.retry_after IS NULL OR i.retry_after <= ?)
           AND NOT EXISTS (
             SELECT 1 FROM edges e
             LEFT JOIN instances u
               ON u.task_name = e.upstream_task
              AND u.logical_date = date(i.logical_date, ${modifier})
             WHERE e.downstream_task = i.task_name
               AND date(i.logical_date, ${modifier}) >= COALESCE(?, date(i.logical_date))
               AND (u.status IS NULL OR u.status <> 'SUCCESS')
           )
         ORDER BY i.logical_date, i.id
         LIMIT ?`,
      )
      .all(now(), earliestPartition ?? null, candidateLimit) as Array<Record<string, unknown>>;
    if (rows.length === 0) return;

    // Precompute every per-tick lookup ONCE instead of one SQL query per PENDING row
    // (the old loop issued ~4 queries × all pending instances per tick → multi-second ticks).
    const bfCache = new Map<number, BackfillJob>();
    const partRunning = new Map<string, number>();
    for (const r of db
      .prepare("SELECT logical_date d, COUNT(*) c FROM instances WHERE status='RUNNING' GROUP BY logical_date")
      .all() as Array<{ d: string; c: number }>) {
      partRunning.set(r.d, r.c);
    }
    // sliding-window admission: earliest unfinished partition per backfill, computed once
    const windowFloor = new Map<number, string>();

    let slots = effectiveGlobalCap - currentActive;
    // In subprocess mode each spawn is a heavy (~100ms) blocking call, so cap launches
    // per tick to keep the event loop responsive. In-process tasks have no spawn cost,
    // so a tick may fill every free slot at once.
    let dispatchBudget = config.executionMode === 'inprocess' ? Number.MAX_SAFE_INTEGER : config.maxDispatchesPerTick;
    for (const row of rows) {
      if (slots <= 0 || dispatchBudget <= 0) return;
      const inst = plainInstance(row);

      let globalCap = config.defaultMaxConcurrency;
      let perPartitionCap = config.defaultMaxPerPartition;
      let bf: BackfillJob | null = null;
      if (inst.backfillId !== null) {
        bf = bfCache.get(inst.backfillId) ?? getBackfill(inst.backfillId);
        if (bf) {
          bfCache.set(inst.backfillId, bf);
          globalCap = bf.maxConcurrency;
          perPartitionCap = bf.maxPerPartition;
        }
      }
      if (currentActive + (effectiveGlobalCap - slots) >= globalCap) return;

      const runningHere = partRunning.get(inst.logicalDate) ?? 0;
      if (runningHere >= perPartitionCap) continue;

      const dep = this.dependencyState(inst);
      if (!dep.ready) continue;

      if (bf) {
        let floor = windowFloor.get(bf.id);
        if (floor === undefined) {
          const r = db
            .prepare(
              `SELECT MIN(logical_date) d FROM instances
               WHERE backfill_id=? AND status IN ('PENDING','RUNNING')`,
            )
            .get(bf.id) as { d: string | null } | undefined;
          floor = r?.d ?? inst.logicalDate;
          windowFloor.set(bf.id, floor);
        }
        // admit only the earliest partition + the next (parallelPartitions-1) dates
        const dayIndex = Math.round((Date.parse(inst.logicalDate) - Date.parse(floor)) / 86_400_000);
        if (dayIndex > bf.parallelPartitions - 1) continue;
      }

      this.claimAndSpawn(inst);
      partRunning.set(inst.logicalDate, (partRunning.get(inst.logicalDate) ?? 0) + 1);
      slots--;
      dispatchBudget--;
    }
  }

  /** Atomic claim + spawn. The conditional UPDATE is the duplicate-launch guard (requirement 6). */
  private claimAndSpawn(inst: TaskInstance): void {
    const db = getDb();
    beginTx(db);
    try {
      const claim = db
        .prepare("UPDATE instances SET status='RUNNING', attempt=attempt+1, started_at=COALESCE(started_at, ?) WHERE id=? AND status='PENDING'")
        .run(now(), inst.id);
      if (claim.changes === 0) {
        rollback(db);
        return; // another tick claimed it first
      }
      commit(db);
    } catch (err) {
      rollback(db);
      throw err;
    }
    // keep the per-tick status cache coherent for subsequent dependency checks
    this.statusCache?.set(`${inst.taskName}|${inst.logicalDate}`, 'RUNNING');

    const fresh = getInstance(inst.id)!;
    const { proc, completion } = spawnAttempt(fresh, fresh.attempt);
    if (fresh.attempt > 1) {
      logEvent('ATTEMPT_RETRY', { instanceId: fresh.id, taskName: fresh.taskName, logicalDate: fresh.logicalDate, detail: `attempt=${fresh.attempt}` });
      raiseAlert('RETRY', `${fresh.taskName}@${fresh.logicalDate} 第 ${fresh.attempt} 次尝试（重试）`, {
        taskName: fresh.taskName, instanceId: fresh.id, logicalDate: fresh.logicalDate,
      });
    }

    completion
      .then(({ exitCode }) => {
        const runId = proc.run.id;
        persistRunOutput(runId);
        removeActive(runId);
        this.resolveInstanceAfterExit(fresh.id, runId, exitCode ?? 1, null, false);
        this.trigger(); // cascade: wake dependents immediately instead of waiting for next tick
      })
      .catch((err) => {
        removeActive(proc.run.id);
        console.error('[scheduler] completion handler error', err);
        this.trigger();
      });
  }

  /** Apply final instance state after an attempt exits; handles retries (requirement 2). */
  private resolveInstanceAfterExit(instanceId: number, runId: number, exitCode: number, forcedNote: string | null, _fromRecovery: boolean): void {
    const db = getDb();
    const inst = getInstance(instanceId);
    if (!inst) return;
    // An operator kill (or any terminal transition that raced ahead) wins: do not let a
    // late completion/exit callback resurrect or retry the instance.
    if (inst.status === 'KILLED' || inst.status === 'SUCCESS') {
      db.prepare("UPDATE instance_runs SET status=?, finished_at=COALESCE(finished_at, ?), exit_code=? WHERE id=? AND status='RUNNING'",
      ).run(inst.status === 'KILLED' ? 'LOST' : 'SUCCESS', now(), exitCode, runId);
      return;
    }
    const task = getTask(inst.taskName);
    if (!task) return;
    const success = exitCode === 0;
    const marker = readMarker(markerPathFor(runId));
    const startedAt = parseTs((db.prepare('SELECT started_at FROM instance_runs WHERE id=?').get(runId) as { started_at: string }).started_at);
    const attemptRuntime = Math.max(0, Date.now() - startedAt);

    beginTx(db);
    try {
      if (success) {
        db.prepare(
          `UPDATE instances SET status='SUCCESS', finished_at=?, duration_ms=?, total_runtime_ms=total_runtime_ms+?, note=NULL WHERE id=?`,
        ).run(now(), attemptRuntime, attemptRuntime, instanceId);
        db.prepare("UPDATE instance_runs SET status='SUCCESS', finished_at=?, exit_code=? WHERE id=?", ).run(now(), 0, runId);
        logEvent('INSTANCE_SUCCESS', { instanceId, taskName: inst.taskName, logicalDate: inst.logicalDate, detail: `attempt=${inst.attempt} runtime=${attemptRuntime}ms` });
      } else {
        db.prepare('UPDATE instances SET total_runtime_ms=total_runtime_ms+? WHERE id=?', ).run(attemptRuntime, instanceId);
        db.prepare("UPDATE instance_runs SET status='FAILED', finished_at=?, exit_code=? WHERE id=?", ).run(now(), exitCode, runId);
        const willRetry = inst.attempt <= task.retries;
        if (willRetry) {
          const backoff = task.retryBackoffMs * 2 ** (inst.attempt - 1);
          db.prepare("UPDATE instances SET status='PENDING', retry_after=? WHERE id=?", )
            .run(new Date(Date.now() + backoff).toISOString(), instanceId);
          logEvent('ATTEMPT_FAILED_WILL_RETRY', {
            instanceId, taskName: inst.taskName, logicalDate: inst.logicalDate,
            detail: `attempt=${inst.attempt} exit=${exitCode} backoff=${backoff}ms note=${forcedNote ?? marker?.error ?? ''}`,
          });
        } else {
          const note = forcedNote ?? marker?.error ?? `exhausted ${inst.attempt} attempt(s), last exit=${exitCode}`;
          db.prepare("UPDATE instances SET status='FAILED', finished_at=?, note=? WHERE id=?", ).run(now(), note, instanceId);
          logEvent('INSTANCE_FAILED', { instanceId, taskName: inst.taskName, logicalDate: inst.logicalDate, detail: note });
          raiseAlert('TASK_FAILED', `任务 ${inst.taskName} 分区 ${inst.logicalDate} 重试 ${inst.attempt - 1} 次后仍失败: ${note}（负责人 ${task.owner}）`, {
            taskName: inst.taskName, instanceId, logicalDate: inst.logicalDate,
          });
        }
      }
      commit(db);
    } catch (err) {
      rollback(db);
      throw err;
    }
  }

  /* ---------------- SLA (requirement 5) ---------------- */

  private checkSlas(): void {
    const db = getDb();
    const rows = db
      .prepare(
        `SELECT i.*, t.sla_ms, t.sla_warn_ratio, t.owner
         FROM instances i JOIN tasks t ON t.name = i.task_name
         WHERE i.status IN ('PENDING','RUNNING') AND t.sla_ms IS NOT NULL`,
      )
      .all() as Array<Record<string, unknown>>;
    if (rows.length === 0) return;
    // bulk-load existing SLA alerts once per tick instead of two queries per instance
    const warnedIds = new Set<number>(
      (db.prepare("SELECT DISTINCT instance_id id FROM alerts WHERE type='SLA_WARNING' AND instance_id IS NOT NULL").all() as Array<{ id: number }>).map((r) => r.id),
    );
    const breachedIds = new Set<number>(
      (db.prepare("SELECT DISTINCT instance_id id FROM alerts WHERE type='SLA_BREACHED' AND instance_id IS NOT NULL").all() as Array<{ id: number }>).map((r) => r.id),
    );
    const nowMs = Date.now();
    for (const row of rows) {
      const inst = plainInstance(row);
      const slaMs = Number(row.sla_ms);
      const warnRatio = Number(row.sla_warn_ratio ?? 0.8);
      // SLA measures execution time: from actual RUNNING start (not queueing time)
      const base = parseTs(inst.startedAt ?? inst.enqueuedAt);
      const elapsed = inst.status === 'RUNNING' ? nowMs - base : 0;
      const owner = String(row.owner ?? '');
      if (!warnedIds.has(inst.id) && elapsed >= slaMs * warnRatio && elapsed < slaMs) {
        raiseAlert('SLA_WARNING', `SLA 预警: ${inst.taskName}@${inst.logicalDate} 已用 ${(elapsed / 1000).toFixed(1)}s / ${slaMs / 1000}s（${Math.round(warnRatio * 100)}%），负责人 ${owner}`, {
          taskName: inst.taskName, instanceId: inst.id, logicalDate: inst.logicalDate,
        });
        warnedIds.add(inst.id);
      }
      if (!breachedIds.has(inst.id) && elapsed >= slaMs) {
        raiseAlert('SLA_BREACHED', `SLA 超时: ${inst.taskName}@${inst.logicalDate} 超过 ${slaMs / 1000}s 仍未完成，负责人 ${owner}`, {
          taskName: inst.taskName, instanceId: inst.id, logicalDate: inst.logicalDate,
        });
        breachedIds.add(inst.id);
      }
    }
  }

  /* ---------------- backfill completion ---------------- */

  private reconcileBackfills(): void {
    const db = getDb();
    for (const bf of listBackfills()) {
      if (bf.status !== 'RUNNING') continue;
      const counts = db.prepare(
        'SELECT status, COUNT(*) c FROM instances WHERE backfill_id=? GROUP BY status',
      ).all(bf.id) as Array<{ status: string; c: number }>;
      const map = new Map(counts.map((c) => [c.status, c.c]));
      const total = counts.reduce((s, c) => s + c.c, 0);
      const terminal = (map.get('SUCCESS') ?? 0) + (map.get('FAILED') ?? 0) + (map.get('UPSTREAM_FAILED') ?? 0) + (map.get('KILLED') ?? 0);
      if (total === 0 || terminal < total) continue;
      const failed = (map.get('FAILED') ?? 0) + (map.get('UPSTREAM_FAILED') ?? 0);
      const status = failed > 0 ? 'FAILED' : 'DONE';
      finishBackfill(bf.id, status);
      const msg = `补数 #${bf.id} ${bf.startDate}~${bf.endDate} 完成: ${map.get('SUCCESS') ?? 0} 成功, ${failed} 失败`;
      raiseAlert(status === 'DONE' ? 'BACKFLOW_DONE' : 'BACKFLOW_FAILED', msg);
      logEvent('BACKFILL_FINISHED', { detail: `id=${bf.id} status=${status}` });
    }
  }

  /* ---------------- idle shutdown for batch runs ---------------- */

  private maybeStopWhenIdle(): void {
    if (!this.opts.stopWhenIdle) return;
    const db = getDb();
    const pending = Number((db.prepare("SELECT COUNT(*) c FROM instances WHERE status='PENDING'").get() as { c: number }).c);
    const running = Number((db.prepare("SELECT COUNT(*) c FROM instances WHERE status='RUNNING'").get() as { c: number }).c);
    const runningBf = Number((db.prepare("SELECT COUNT(*) c FROM backfills WHERE status='RUNNING'").get() as { c: number }).c);
    const anyWork = Number((db.prepare('SELECT COUNT(*) c FROM instances').get() as { c: number }).c) > 0
      || Number((db.prepare('SELECT COUNT(*) c FROM backfills').get() as { c: number }).c) > 0;
    if (!anyWork) return;
    // A wave boundary can briefly show 0 RUNNING while PENDING tasks wait for their
    // dependencies. Only stop after a *sustained* idle period with zero pending work.
    if (pending === 0 && running === 0 && activeCount() === 0 && runningBf === 0) {
      if (this.idleSince === null) this.idleSince = Date.now();
      if (Date.now() - this.idleSince >= (this.opts.idleGraceMs ?? 3000)) {
        this.stop();
        this.onDone?.();
      }
    } else {
      this.idleSince = null;
    }
  }
}

function plainInstance(row: Record<string, unknown>): TaskInstance {
  return {
    id: Number(row.id),
    taskName: String(row.task_name),
    logicalDate: String(row.logical_date),
    status: String(row.status) as InstanceStatus,
    attempt: Number(row.attempt),
    retryAfter: (row.retry_after as string | null) ?? null,
    enqueuedAt: String(row.enqueued_at),
    startedAt: (row.started_at as string | null) ?? null,
    finishedAt: (row.finished_at as string | null) ?? null,
    durationMs: (row.duration_ms as number | null) ?? null,
    totalRuntimeMs: Number(row.total_runtime_ms ?? 0),
    backfillId: (row.backfill_id as number | null) ?? null,
    note: (row.note as string | null) ?? null,
  };
}
