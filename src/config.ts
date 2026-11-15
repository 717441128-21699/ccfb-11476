/**
 * Global configuration. All paths/data live under DATA_DIR so a demo run
 * never touches anything outside the workspace.
 */
import path from 'node:path';

const WORKSPACE_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');

function envInt(name: string, fallback: number): number {
  const v = process.env[name];
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

export const config = {
  workspaceRoot: WORKSPACE_ROOT,
  dataDir: process.env.SCHED_DATA_DIR ?? path.join(WORKSPACE_ROOT, 'data'),
  get dbPath() {
    return path.join(this.dataDir, 'scheduler.db');
  },
  get scriptsDir() {
    return path.join(this.dataDir, 'fake-scripts');
  },
  get markersDir() {
    return path.join(this.dataDir, 'markers');
  },
  get logDir() {
    return path.join(this.dataDir, 'logs');
  },
  /** scheduler polling interval (ms) — event-driven scheduling is additionally triggered immediately on completion */
  tickIntervalMs: envInt('SCHED_TICK_MS', 500),
  /**
   * Max new processes spawned per tick. On Windows child_process.spawn is a heavy
   * (~100ms) libuv operation; spawning the whole concurrency cap in one tick blocks the
   * event loop for over a second. Spreading launches across ticks keeps each tick short
   * while steady-state throughput is unchanged (slots refill continuously).
   */
  maxDispatchesPerTick: envInt('SCHED_DISPATCH_PER_TICK', 2),
  /** heartbeat written by the executor while a child is alive */
  heartbeatIntervalMs: envInt('SCHED_HEARTBEAT_MS', 500),
  /** a run whose lease/heartbeat is older than this is considered orphaned after a restart */
  leaseTimeoutMs: envInt('SCHED_LEASE_MS', 8000),
  /**
   * Default global concurrency cap (requirement 3: 并发数有上限).
   * Sized for a 16-core Windows box where each task is a real OS process: beyond ~12
   * concurrent node children, context-switching + AV scanning inflate per-task latency
   * faster than parallelism helps. Override with SCHED_MAX_CONCURRENCY.
   */
  defaultMaxConcurrency: envInt('SCHED_MAX_CONCURRENCY', 12),
  /** default per-partition (per logical date) concurrency cap */
  defaultMaxPerPartition: envInt('SCHED_MAX_PER_PARTITION', 10),
  /** default parallel partitions in a backfill */
  defaultBackfillPartitions: envInt('SCHED_BACKFILL_PARALLEL', 2),
  httpPort: envInt('SCHED_PORT', 7777),
  /**
   * Execution mode:
   *  'subprocess' (default, production): one real OS process per task attempt
   *  'inprocess' (demo/verify): fake tasks run as async functions with the identical
   *              marker/heartbeat/state contract, but zero child-process spawn cost —
   *              needed to keep large simulated backfills fast on process-spawn-bound hosts.
   */
  executionMode: (process.env.SCHED_EXEC_MODE === 'inprocess' ? 'inprocess' : 'subprocess') as 'subprocess' | 'inprocess',
} as const;
