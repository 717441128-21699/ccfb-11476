/**
 * Self-contained single-partition performance probe.
 * Runs the scheduler in-process (no CLI/process chain), streams progress, and writes
 * a summary with the measured critical-path ratio. Invoked by perf experiments.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../src/config.ts';
import { ensureRuntimeDirs, getDb } from '../src/db.ts';
import { Scheduler } from '../src/scheduler.ts';
import { startBackfill } from '../src/backfill.ts';
import { loadDag, criticalPathMs } from '../src/dag.ts';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const outFile = path.join(ROOT, 'data', 'perf-probe.json');

function dateStr(d: Date): string { return d.toISOString().slice(0, 10); }

async function main(): Promise<void> {
  ensureRuntimeDirs();
  const db = getDb();
  const dag = loadDag();
  const logicalDate = process.env.PROBE_DATE ?? '2026-11-01';
  const res = startBackfill({
    startDate: logicalDate, endDate: logicalDate,
    maxConcurrency: Number(process.env.PROBE_CAP ?? 12),
    maxPerPartition: Number(process.env.PROBE_CAP ?? 10),
    parallelPartitions: 1,
  });
  const t0 = Date.now();
  const scheduler = new Scheduler({ autoDaily: false });
  scheduler.start();

  let lastSuccess = -1;
  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      const counts = db.prepare(
        'SELECT status, COUNT(*) c FROM instances WHERE backfill_id=? GROUP BY status',
      ).all(res.job.id) as Array<{ status: string; c: number }>;
      const map = new Map(counts.map((c) => [c.status, c.c]));
      const success = map.get('SUCCESS') ?? 0;
      if (success !== lastSuccess) {
        console.log(`t=${((Date.now() - t0) / 1000).toFixed(1)}s success=${success} running=${map.get('RUNNING') ?? 0} pending=${map.get('PENDING') ?? 0}`);
        lastSuccess = success;
      }
      const job = db.prepare('SELECT status FROM backfills WHERE id=?').get(res.job.id) as { status: string };
      if (job.status !== 'RUNNING') { clearInterval(timer); resolve(); }
    }, 1000);
  });
  const wallMs = Date.now() - t0;

  // measured CP from actual per-task runtimes, requested partition only
  const insts = db.prepare(
    "SELECT * FROM instances WHERE backfill_id=? AND logical_date=? AND status='SUCCESS'",
  ).all(res.job.id, logicalDate) as Array<Record<string, unknown>>;
  const weight = new Map(insts.map((i) => [String(i.task_name), Number(i.duration_ms ?? 400)]));
  const cp = criticalPathMs(dag, (name) => weight.get(name) ?? 400);
  const partStart = Math.min(...insts.map((i) => Date.parse(String(i.started_at))));
  const partEnd = Math.max(...insts.map((i) => Date.parse(String(i.finished_at))));
  const partWall = partEnd - partStart;
  const summary = {
    logicalDate,
    instances: insts.length,
    wallMs,
    partitionWallMs: partWall,
    measuredCriticalPathMs: cp,
    ratio: partWall / cp,
    dataDir: config.dataDir,
  };
  fs.writeFileSync(outFile, JSON.stringify(summary, null, 2));
  console.log('SUMMARY', JSON.stringify(summary, null, 2));
  scheduler.stop();
  process.exit(0);
}

main().catch((err) => { console.error('probe fatal:', err); process.exit(1); });
