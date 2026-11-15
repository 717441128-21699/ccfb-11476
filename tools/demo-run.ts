/**
 * Full guided demo implementation (loaded by demo.ts after the exec-mode env is set):
 *   1. generate the 200-task DAG and fake scripts
 *   2. start the scheduler + management UI
 *   3. submit a 30-day backfill
 *   4. stream progress until the backfill finishes, then print a summary
 */
import { config } from '../src/config.ts';
import { ensureRuntimeDirs, getDb } from '../src/db.ts';
import { Scheduler } from '../src/scheduler.ts';
import { startHttpServer } from '../src/server.ts';
import { startBackfill } from '../src/backfill.ts';
import { listBackfills } from '../src/repo.ts';
import { addDays } from '../src/util/dates.ts';

export async function runDemo(): Promise<void> {
  ensureRuntimeDirs();
  console.log('[demo] step 1: generating 200-task DAG + fake scripts...');
  const { generateDag } = await import('./gen-dag.ts');
  const gen = generateDag();
  console.log(`[demo] tasks=${gen.tasks.length} edges=${gen.edgeCount} crossPartitionEdges=${gen.offsetEdgeCount}`);
  console.log(`[demo] execution mode: ${config.executionMode}`);

  console.log('[demo] step 2: starting scheduler + UI on port', config.httpPort);
  const scheduler = new Scheduler({ autoDaily: false });
  scheduler.start();
  const server = startHttpServer(scheduler);

  const end = new Date().toISOString().slice(0, 10);
  const start = addDays(end, -29);
  console.log(`[demo] step 3: submitting backfill ${start} ~ ${end} (30 partitions)`);
  const res = startBackfill({
    startDate: start, endDate: end,
    maxConcurrency: 40, maxPerPartition: 40, parallelPartitions: 2,
  });
  console.log(`[demo] backfill #${res.job.id}: enqueued=${res.enqueued} reset=${res.reset} reused=${res.reused}`);

  // progress stream
  const db = getDb();
  let lastLine = '';
  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      const counts = db.prepare(
        'SELECT status, COUNT(*) c FROM instances WHERE backfill_id=? GROUP BY status',
      ).all(res.job.id) as Array<{ status: string; c: number }>;
      const map = new Map(counts.map((c) => [c.status, c.c]));
      const done = (map.get('SUCCESS') ?? 0);
      const line = `  进度 ${done}  RUNNING=${map.get('RUNNING') ?? 0} PENDING=${map.get('PENDING') ?? 0} FAILED=${map.get('FAILED') ?? 0}`;
      if (line !== lastLine) { console.log(line); lastLine = line; }
      const job = listBackfills().find((b) => b.id === res.job.id);
      if (job && job.status !== 'RUNNING') {
        clearInterval(timer);
        console.log(`[demo] backfill finished: ${job.status}`);
        resolve();
      }
    }, 1000);
  });

  console.log('[demo] done. management UI remains available at', `http://localhost:${config.httpPort}/`);
  console.log('[demo] press Ctrl+C to exit.');
  process.on('SIGINT', () => {
    scheduler.stop();
    server.close();
    process.exit(0);
  });
}
