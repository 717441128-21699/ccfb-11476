/** Domain types shared across scheduler, API and UI. */

export interface TaskDef {
  id: number;
  name: string;
  owner: string;
  /** command template, e.g. "node fake-scripts/t_001_root.js {date}" */
  command: string;
  /** retries after the first attempt; total attempts = retries + 1 */
  retries: number;
  /** base retry backoff in ms, exponential: base * 2^(attempt-1) */
  retryBackoffMs: number;
  /** expected runtime in ms, used for SLA default and graph planning */
  expectedDurationMs: number;
  /** SLA in ms measured from partition ready time; null = no SLA */
  slaMs: number | null;
  /** warn at this fraction of SLA consumed (requirement 5: 提前告警) */
  slaWarnRatio: number;
  createdAt: string;
}

export interface EdgeDef {
  id: number;
  upstreamTask: string;
  downstreamTask: string;
  /**
   * Partition offset. 0 = same-date dependency.
   * -1 means downstream's partition d depends on upstream's partition d-1 (classic daily DWS/DWD pattern).
   */
  offsetDays: number;
}

export type InstanceStatus =
  | 'PENDING'      // created, waiting for dependencies / scheduling
  | 'RUNNING'      // a run is currently executing
  | 'SUCCESS'
  | 'FAILED'       // retries exhausted
  | 'UPSTREAM_FAILED'
  | 'KILLED';

export interface TaskInstance {
  id: number;
  taskName: string;
  /** logical partition date, YYYY-MM-DD */
  logicalDate: string;
  status: InstanceStatus;
  attempt: number;
  /** earliest time the scheduler may dispatch another attempt (retry backoff) */
  retryAfter: string | null;
  enqueuedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  /** wall-clock duration of the final attempt in ms */
  durationMs: number | null;
  /** sum over all attempts in ms */
  totalRuntimeMs: number;
  backfillId: number | null;
  /** free-form JSON note, e.g. failure reason / kill operator */
  note: string | null;
}

/** One OS-level attempt of an instance. An instance can have several runs across retries. */
export interface InstanceRun {
  id: number;
  instanceId: number;
  attempt: number;
  pid: number | null;
  status: 'RUNNING' | 'SUCCESS' | 'FAILED' | 'LOST';
  markerPath: string | null;
  startedAt: string;
  heartbeatAt: string | null;
  finishedAt: string | null;
  exitCode: number | null;
  stdoutTail: string | null;
  stderrTail: string | null;
}

export type AlertType =
  | 'TASK_FAILED'
  | 'SLA_WARNING'
  | 'SLA_BREACHED'
  | 'RETRY'
  | 'BACKFLOW_DONE'
  | 'BACKFLOW_FAILED';

export interface Alert {
  id: number;
  type: AlertType;
  taskName: string | null;
  instanceId: number | null;
  logicalDate: string | null;
  message: string;
  channel: string;
  acked: number; // 0/1
  createdAt: string;
}

export interface EventRow {
  id: number;
  instanceId: number | null;
  taskName: string | null;
  logicalDate: string | null;
  eventType: string;
  detail: string | null;
  createdAt: string;
}

export interface BackfillJob {
  id: number;
  startDate: string;
  endDate: string;
  rootTask: string | null;
  /** rerun mode: only root + descendants are reset, other tasks reused as-is */
  rerunMode: number; // 0/1
  maxConcurrency: number;
  maxPerPartition: number;
  parallelPartitions: number;
  status: 'RUNNING' | 'DONE' | 'FAILED' | 'KILLED';
  createdAt: string;
  finishedAt: string | null;
}

export interface SchedulerMeta {
  singletonLock: string | null;
  lastTickAt: string | null;
}
