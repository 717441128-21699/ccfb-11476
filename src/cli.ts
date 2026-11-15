/**
 * CLI entry point.
 *
 *   node src/cli.ts init                 create schema + runtime dirs
 *   node src/cli.ts gen                  generate the 200-task demo DAG + fake scripts
 *   node src/cli.ts serve                long-running scheduler + management UI
 *   node src/cli.ts backfill <s> <e>     one-shot backfill, process exits when idle
 *   node src/cli.ts rerun <task> <s> <e> rerun task + downstream for partitions
 *   node src/cli.ts status               quick DB summary
 *   node src/cli.ts demo                 full guided demo (gen -> 30d backfill -> verify)
 */
import path from 'node:path';
import { config } from './config.ts';
import { ensureRuntimeDirs, getDb } from './db.ts';
import { Scheduler } from './scheduler.ts';
import { startHttpServer } from './server.ts';
import { startBackfill, rerunDownstream } from './backfill.ts';
import { listTasks, listEdges, listBackfills, countInstancesByStatus } from './repo.ts';
import { loadDag, criticalPathMs } from './dag.ts';
import { addDays } from './util/dates.ts';

function printStatus(): void {
  const db = getDb();
  const tasks = listTasks();
  const edges = listEdges();
  console.log(`tasks=${tasks.length} edges=${edges.length} db=${path.resolve(config.dbPath)}`);
  if (tasks.length) {
    const dag = loadDag();
    const cp = criticalPathMs(dag, (n) => dag.nodes.get(n)!.task.expectedDurationMs);
    console.log(`layers=${dag.layers.length} expectedCriticalPath=${(cp / 1000).toFixed(2)}s`);
  }
  const inst = db.prepare('SELECT COUNT(*) c FROM instances').get() as { c: number };
  console.log(`instances=${inst.c}`, countInstancesByStatus(null));
  console.log('backfills=', listBackfills().map((b) => `#${b.id}:${b.status}`).join(', ') || 'none');
}

async function runBatch(mode: 'backfill' | 'rerun', args: string[]): Promise<void> {
  ensureRuntimeDirs();
  if (mode === 'backfill') {
    const [start, end] = args;
    if (!start || !end) throw new Error('usage: backfill <startDate> <endDate>');
    const res = startBackfill({ startDate: start, endDate: end });
    console.log(`[cli] backfill #${res.job.id} created: enqueued=${res.enqueued} reset=${res.reset} reused=${res.reused}`);
  } else {
    const [task, start, end] = args;
    if (!task || !start || !end) throw new Error('usage: rerun <taskName> <startDate> <endDate>');
    const dates: string[] = [];
    for (let d = start; d <= end; d = addDays(d, 1)) dates.push(d);
    const res = rerunDownstream(task, dates);
    console.log(`[cli] rerun #${res.job.id} from ${task}: reset=${res.reset} enqueued=${res.enqueued}`);
  }
  await new Promise<void>((resolve) => {
    const sched = new Scheduler({ autoDaily: false, stopWhenIdle: true, idleGraceMs: 2000 });
    sched.start(resolve);
  });
  printStatus();
}

async function main(): Promise<void> {
  const [cmd, ...rest] = process.argv.slice(2);
  ensureRuntimeDirs();

  switch (cmd) {
    case 'init': {
      getDb(); // creates schema
      console.log('[cli] initialized at', path.resolve(config.dataDir));
      break;
    }
    case 'gen': {
      const { generateDag } = await import('../tools/gen-dag.ts');
      const r = generateDag();
      console.log(`[cli] generated ${r.tasks.length} tasks, ${r.edgeCount} edges (${r.offsetEdgeCount} cross-partition)`);
      break;
    }
    case 'serve': {
      const sched = new Scheduler({ autoDaily: true });
      sched.start();
      startHttpServer(sched);
      process.on('SIGINT', () => { sched.stop(); process.exit(0); });
      process.on('SIGTERM', () => { sched.stop(); process.exit(0); });
      break;
    }
    case 'backfill':
      await runBatch('backfill', rest);
      break;
    case 'rerun':
      await runBatch('rerun', rest);
      break;
    case 'status':
      printStatus();
      break;
    case 'demo': {
      process.env.SCHED_EXEC_MODE ??= 'inprocess';
      const { runDemo } = await import('../tools/demo-run.ts');
      await runDemo();
      break;
    }
    default:
      console.log(`unknown command: ${cmd ?? ''}
commands: init | gen | serve | backfill <s> <e> | rerun <task> <s> <e> | status | demo`);
      process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error('[cli] fatal:', err);
  process.exit(1);
});
