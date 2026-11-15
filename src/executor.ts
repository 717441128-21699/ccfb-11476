/**
 * Executor: one OS process per task attempt (the classic scheduler model).
 *
 * Restart-safety protocol (requirement 6):
 *  - every attempt gets a marker JSON file under data/markers/ and an instance_runs row
 *  - the task child writes RUNNING+pid at start, refreshes heartbeat, writes SUCCESS/FAILED at exit
 *  - after a scheduler restart, recoverOrphans() reconciles via pid + marker instead of relaunching
 *  - dispatch is an atomic claim transaction, so an instance is never launched twice
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.ts';
import { getDb, now } from './db.ts';
import type { InstanceRun, TaskInstance } from './types.ts';
import { logEvent } from './events.ts';

const nodeRequire = createRequire(import.meta.url);
const helperPath = path.join(config.workspaceRoot, 'scripts', 'fake-task-helper.cjs');
type FakeTaskFn = (opts: {
  markerPath: string; taskName: string; logicalDate: string; attempt: number;
  instanceId: number; runId: number; baseDuration: number; flakyRate: number;
  env: NodeJS.ProcessEnv;
}) => Promise<{ exitCode: number; error: string | null }>;
let fakeTaskFn: FakeTaskFn | null = null;
function getFakeTaskFn(): FakeTaskFn {
  if (!fakeTaskFn) fakeTaskFn = nodeRequire(helperPath).runFakeTask as FakeTaskFn;
  return fakeTaskFn;
}

export interface MarkerContent {
  instanceId: number;
  runId: number;
  taskName: string;
  logicalDate: string;
  attempt: number;
  pid: number;
  status: 'RUNNING' | 'SUCCESS' | 'FAILED';
  exitCode: number | null;
  startedAt: string;
  heartbeatAt: string;
  error?: string;
}

export interface ActiveProcess {
  run: InstanceRun;
  instanceId: number;
  /** real child process in subprocess mode; null in zero-spawn in-process mode */
  child: ChildProcess | null;
  markerPath: string;
  stdout: string;
  stderr: string;
  inProcess: boolean;
  /** synthetic kill flag for in-process tasks */
  killed?: boolean;
}

const live = new Map<number, ActiveProcess>();
const TAIL = 4000;

export function markerPathFor(runId: number): string {
  return path.join(config.markersDir, `run-${runId}.marker.json`);
}

export function readMarker(markerPath: string): MarkerContent | null {
  try {
    return JSON.parse(fs.readFileSync(markerPath, 'utf8')) as MarkerContent;
  } catch {
    return null;
  }
}

export function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

function resolveCommand(command: string, logicalDate: string): { exe: string; args: string[] } {
  const tokens = command.replace(/\{date\}/g, logicalDate).split(/\s+/).filter(Boolean);
  if (tokens.length === 0) throw new Error(`empty command: ${command}`);
  const exe = tokens.shift()!;
  const args = tokens.map((t) => {
    if (path.isAbsolute(t) || t === logicalDate) return t;
    const inData = path.join(config.dataDir, t);
    if (fs.existsSync(inData)) return inData;
    const inWs = path.join(config.workspaceRoot, t);
    if (fs.existsSync(inWs)) return inWs;
    return t;
  });
  return { exe, args };
}

export interface SpawnResult {
  proc: ActiveProcess;
  completion: Promise<{ exitCode: number | null; signal: NodeJS.Signals | null }>;
}

export function spawnAttempt(inst: TaskInstance, attempt: number): SpawnResult {
  const db = getDb();
  const placeholder = db
    .prepare(
      `INSERT INTO instance_runs (instance_id, attempt, pid, status, started_at, heartbeat_at)
       VALUES (?, ?, NULL, 'RUNNING', ?, ?)`,
    )
    .run(inst.id, attempt, now(), now());
  const runId = Number(placeholder.lastInsertRowid);
  const markerPath = markerPathFor(runId);
  const command = (db.prepare('SELECT command FROM tasks WHERE name = ?').get(inst.taskName) as { command: string }).command;

  if (config.executionMode === 'inprocess') {
    return spawnInProcess(inst, attempt, runId, markerPath, command);
  }
  return spawnSubProcess(inst, attempt, runId, markerPath, command);
}

/** Parse the trailing numeric args the generator appends to every fake-task command. */
function parseFakeArgs(command: string, logicalDate: string): { baseDuration: number; flakyRate: number } {
  const tokens = command.replace(/\{date\}/g, logicalDate).split(/\s+/).filter(Boolean);
  const nums = tokens.map(Number).filter((n) => Number.isFinite(n));
  return { baseDuration: nums[0] ?? 200, flakyRate: nums[1] ?? 0 };
}

/** Zero-spawn mode: run fake-task logic as an in-process async task (same marker contract). */
function spawnInProcess(
  inst: TaskInstance, attempt: number, runId: number, markerPath: string, command: string,
): SpawnResult {
  const db = getDb();
  const { baseDuration, flakyRate } = parseFakeArgs(command, inst.logicalDate);
  db.prepare('UPDATE instance_runs SET pid = ?, marker_path = ? WHERE id = ?').run(process.pid, markerPath, runId);
  const run = mapRun(db.prepare('SELECT * FROM instance_runs WHERE id = ?').get(runId) as Record<string, unknown>);
  insertRunHeartbeat(runId);

  const proc: ActiveProcess = { run, instanceId: inst.id, child: null, markerPath, stdout: '', stderr: '', inProcess: true };
  live.set(runId, proc);

  const taskEnv: NodeJS.ProcessEnv = {
    ...process.env,
    SCHED_MARKER_PATH: markerPath,
    SCHED_INSTANCE_ID: String(inst.id),
    SCHED_RUN_ID: String(runId),
    SCHED_LOGICAL_DATE: inst.logicalDate,
    SCHED_TASK_NAME: inst.taskName,
    SCHED_ATTEMPT: String(attempt),
  };
  const completion = new Promise<{ exitCode: number | null; signal: NodeJS.Signals | null }>((resolvePromise) => {
    // In-process mode shares the scheduler's main thread, so pass no marker path: the fake
    // task then skips synchronous file writes entirely (which would otherwise block the
    // event loop under high concurrency). instance_runs is the source of truth here.
    getFakeTaskFn()({
      markerPath: '', taskName: inst.taskName, logicalDate: inst.logicalDate, attempt,
      instanceId: inst.id, runId, baseDuration, flakyRate, env: taskEnv,
    })
      .then((r) => {
        if (proc.killed) { proc.stderr = 'killed by operator'; return resolvePromise({ exitCode: null, signal: 'SIGTERM' }); }
        if (r.error) proc.stderr = r.error;
        else proc.stdout = `[in-process] ${inst.taskName}@${inst.logicalDate} OK`;
        resolvePromise({ exitCode: r.exitCode, signal: null });
      })
      .catch((err: Error) => {
        proc.stderr = `in-process error: ${err.message}`;
        resolvePromise({ exitCode: 1, signal: null });
      });
  });

  logEvent('ATTEMPT_STARTED', {
    instanceId: inst.id, taskName: inst.taskName, logicalDate: inst.logicalDate,
    detail: `attempt=${attempt} in-process pid=${process.pid}`,
  });
  return { proc, completion };
}

/** Production mode: one real OS process per attempt. */
function spawnSubProcess(
  inst: TaskInstance, attempt: number, runId: number, markerPath: string, command: string,
): SpawnResult {
  const { exe, args } = resolveCommand(command, inst.logicalDate);
  const child = spawn(exe, args, {
    cwd: config.dataDir,
    env: {
      ...process.env,
      // real subprocess owns its own event loop, so the periodic marker heartbeat is safe
      SCHED_HEARTBEAT_FILE: '1',
      SCHED_MARKER_PATH: markerPath,
      SCHED_INSTANCE_ID: String(inst.id),
      SCHED_RUN_ID: String(runId),
      SCHED_LOGICAL_DATE: inst.logicalDate,
      SCHED_TASK_NAME: inst.taskName,
      SCHED_ATTEMPT: String(attempt),
    },
    windowsHide: true,
  });

  const db = getDb();
  db.prepare('UPDATE instance_runs SET pid = ?, marker_path = ? WHERE id = ?').run(child.pid ?? null, markerPath, runId);
  const run = mapRun(db.prepare('SELECT * FROM instance_runs WHERE id = ?').get(runId) as Record<string, unknown>);
  insertRunHeartbeat(runId);

  const proc: ActiveProcess = { run, instanceId: inst.id, child, markerPath, stdout: '', stderr: '', inProcess: false };
  live.set(runId, proc);

  child.stdout?.on('data', (chunk: Buffer) => {
    proc.stdout = (proc.stdout + chunk.toString()).slice(-TAIL);
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    proc.stderr = (proc.stderr + chunk.toString()).slice(-TAIL);
  });
  child.on('error', (err) => {
    proc.stderr = (proc.stderr + `spawn error: ${err.message}`).slice(-TAIL);
  });

  const completion = new Promise<{ exitCode: number | null; signal: NodeJS.Signals | null }>((resolvePromise) => {
    child.on('exit', (exitCode, signal) => resolvePromise({ exitCode, signal }));
  });

  logEvent('ATTEMPT_STARTED', {
    instanceId: inst.id, taskName: inst.taskName, logicalDate: inst.logicalDate,
    detail: `attempt=${attempt} pid=${child.pid ?? '?'}`,
  });

  return { proc, completion };
}

export function getActiveProcess(runId: number): ActiveProcess | undefined {
  return live.get(runId);
}

export function activeCount(): number {
  return live.size;
}

export function activeRunIds(): number[] {
  return [...live.keys()];
}

export function removeActive(runId: number): ActiveProcess | undefined {
  const proc = live.get(runId);
  live.delete(runId);
  return proc;
}

export function persistRunOutput(runId: number): void {
  const proc = live.get(runId);
  getDb()
    .prepare('UPDATE instance_runs SET stdout_tail = ?, stderr_tail = ? WHERE id = ?')
    .run((proc?.stdout ?? '').slice(-TAIL), (proc?.stderr ?? '').slice(-TAIL), runId);
}

export function insertRunHeartbeat(runId: number): void {
  getDb().prepare("UPDATE instance_runs SET heartbeat_at = ?, status='RUNNING' WHERE id = ?").run(now(), runId);
}

export function killProcess(runId: number): boolean {
  const proc = live.get(runId);
  if (!proc) return false;
  if (proc.inProcess || !proc.child) {
    // synthetic kill: the running async task resolves on its next completion with SIGTERM
    proc.killed = true;
    return true;
  }
  const child = proc.child;
  try {
    child.kill('SIGTERM');
    setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) {
        try { child.kill('SIGKILL'); } catch { /* gone */ }
      }
    }, 1500).unref();
    return true;
  } catch {
    return false;
  }
}

function mapRun(r: Record<string, unknown>): InstanceRun {
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
