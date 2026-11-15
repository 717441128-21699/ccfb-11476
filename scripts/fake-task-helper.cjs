/**
 * Fake task logic, usable two ways:
 *   1. as a standalone executable (the generated fake-scripts/*.cjs require this file)
 *   2. as an in-process function via runFakeTask(), for the zero-spawn execution mode
 *
 * Both paths implement the identical contract: marker file with RUNNING heartbeat and a
 * terminal SUCCESS/FAILED state, deterministic-ish duration, optional injected failures.
 */
const fs = require('node:fs');

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function writeMarker(path, content) {
  try { fs.writeFileSync(path, JSON.stringify(content)); } catch { /* stale marker treated as LOST */ }
}

/**
 * Run one fake task.
 * @param {object} opts
 * @param {string} opts.markerPath
 * @param {string} opts.taskName
 * @param {string} opts.logicalDate
 * @param {number} opts.attempt
 * @param {number} opts.instanceId
 * @param {number} opts.runId
 * @param {number} opts.baseDuration
 * @param {number} opts.flakyRate
 * @param {NodeJS.ProcessEnv} [opts.env] injected-failure env (defaults to process.env)
 * @returns {Promise<{exitCode:number, error:string|null}>}
 */
async function runFakeTask(opts) {
  const env = opts.env ?? process.env;
  const getEnv = (n) => env[n] ?? null;
  const { markerPath, taskName, logicalDate, attempt, instanceId, runId } = opts;
  const baseDuration = Number(opts.baseDuration ?? 200);
  const flakyRate = Number(opts.flakyRate ?? 0);
  const pid = process.pid;

  const seedStr = `${taskName}|${logicalDate}`;
  let h = 0;
  for (let i = 0; i < seedStr.length; i++) h = (h * 31 + seedStr.charCodeAt(i)) >>> 0;
  const jitter = (h % 200) - 100;
  const duration = Math.max(20, baseDuration + jitter);

  const startedAt = new Date().toISOString();
  const baseMarker = { instanceId, runId, taskName, logicalDate, attempt, pid, startedAt };
  if (markerPath) {
    // In in-process execution many tasks share the scheduler's event loop: a periodic
    // synchronous file write per task becomes a disk-write storm that blocks scheduling.
    // The scheduler already maintains heartbeat_at on the run row, so only emit the
    // periodic marker heartbeat for real subprocesses (SCHED_HEARTBEAT_FILE=1).
    if (env.SCHED_HEARTBEAT_FILE === '1') {
      const heartbeat = setInterval(() => {
        writeMarker(markerPath, { ...baseMarker, status: 'RUNNING', exitCode: null, heartbeatAt: new Date().toISOString() });
      }, 400);
      // unref so the timer never keeps a short-lived process alive on its own
      if (typeof heartbeat.unref === 'function') heartbeat.unref();
    }
    writeMarker(markerPath, { ...baseMarker, status: 'RUNNING', exitCode: null, heartbeatAt: startedAt });
  }

  let failed = false;
  let reason = '';
  const forceFailOne = getEnv('SCHED_FORCE_FAIL');
  if (forceFailOne) {
    const [t, a] = forceFailOne.split(':');
    if (t === taskName && Number(a ?? '1') === attempt) { failed = true; reason = `injected failure SCHED_FORCE_FAIL attempt ${attempt}`; }
  }
  if (!failed && getEnv('SCHED_FORCE_FAIL_TASK') === taskName) {
    failed = true; reason = 'injected permanent failure SCHED_FORCE_FAIL_TASK';
  }
  // flaky only bites on the first attempt; retry attempts always succeed
  if (!failed && attempt === 1 && flakyRate > 0) {
    const r = (h % 1000) / 1000;
    if (r < flakyRate) { failed = true; reason = `simulated flaky failure (rate=${flakyRate}, draw=${r.toFixed(3)})`; }
  }

  await sleep(duration);

  if (failed) {
    if (markerPath) {
      writeMarker(markerPath, { ...baseMarker, status: 'FAILED', exitCode: 1, error: reason, heartbeatAt: new Date().toISOString() });
    }
    return { exitCode: 1, error: reason };
  }
  if (markerPath) {
    writeMarker(markerPath, { ...baseMarker, status: 'SUCCESS', exitCode: 0, heartbeatAt: new Date().toISOString() });
  }
  return { exitCode: 0, error: null };
}

module.exports = { runFakeTask };

// standalone executable entry point (when required by a generated fake-scripts/*.cjs)
if (require.main === module || process.env.SCHED_MARKER_PATH) {
  const logicalDate = process.argv[2] ?? process.env.SCHED_LOGICAL_DATE ?? '1970-01-01';
  const baseDuration = Number(process.argv[3] ?? 200);
  const flakyRate = Number(process.argv[4] ?? 0);
  const taskName = process.env.SCHED_TASK_NAME ?? process.argv[1]?.split(/[\\/]/).pop()?.replace(/\.cjs$/, '') ?? 'unknown';
  runFakeTask({
    markerPath: process.env.SCHED_MARKER_PATH,
    taskName,
    logicalDate,
    attempt: Number(process.env.SCHED_ATTEMPT ?? '1'),
    instanceId: Number(process.env.SCHED_INSTANCE_ID ?? '0'),
    runId: Number(process.env.SCHED_RUN_ID ?? '0'),
    baseDuration,
    flakyRate,
  }).then((r) => {
    if (r.exitCode !== 0) console.error(`[fake-task] ${taskName}@${logicalDate} attempt ${process.env.SCHED_ATTEMPT} FAILED: ${r.error}`);
    else console.log(`[fake-task] ${taskName}@${logicalDate} OK`);
    process.exit(r.exitCode);
  }).catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
