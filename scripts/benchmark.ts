/**
 * 压测：200 任务 DAG × 30 天分区补数
 * 校验：
 *   1. 执行顺序不违反依赖（每个实例 started_at 晚于同分区所有上游 finished_at）
 *   2. 总墙钟耗时 <= 关键路径理论时长 × 1.2
 *   3. 所有实例终态正确（无丢失、无重复拉起）
 *
 * 用法: tsx scripts/benchmark.ts [--days 30] [--concurrency 1024]
 */
import fs from 'node:fs';
import { Store } from '../src/db.js';
import { Scheduler } from '../src/scheduler.js';
import { generateSyntheticDag } from '../src/synthetic.js';
import { dateRange, sleep } from '../src/config.js';
import type { SchedulerConfig, TaskInstance } from '../src/types.js';

function flag(name: string, def: string) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : def;
}

const DAYS = Number(flag('days', '30'));
const CONCURRENCY = Number(flag('concurrency', '1024'));
const DB_PATH = 'data/benchmark.db';

for (const f of [DB_PATH, `${DB_PATH}-wal`, `${DB_PATH}-shm`]) {
  try { fs.unlinkSync(f); } catch { /* 忽略 */ }
}

const cfg: SchedulerConfig = {
  dbPath: DB_PATH,
  maxConcurrent: CONCURRENCY,
  pollIntervalMs: 50,
  leaseTimeoutMs: 30_000,
  alertWebhookUrl: null,
  historyLookbackDays: 0,
};

const store = new Store(cfg.dbPath);
const scheduler = new Scheduler(store, cfg);

// ---------- 1. 造 200 任务 DAG ----------
const gen = generateSyntheticDag(store, {
  taskCount: 200,
  layerCount: 8,
  scriptDir: 'scripts/jobs',
  durationMs: 50, // 压测用短任务，放大调度开销占比（更严格的考验）
});
console.log(`\n=== Flowline 补数压测 ===`);
console.log(`任务=${gen.taskCount} 边=${gen.edgeCount} 层=${gen.layerCount}（各层 ${gen.layers.join('/')}）`);

await scheduler.start();
void scheduler.dispatchLoop();

// ---------- 2. 30 天补数 ----------
const today = new Date();
const to = today.toISOString().slice(0, 10);
const from = new Date(today.getTime() - (DAYS - 1) * 86400_000).toISOString().slice(0, 10);
const partitions = dateRange(from, to);

const t0 = Date.now();
const bf = scheduler.instances.createBackfill({
  name: `benchmark-${DAYS}d`,
  dateFrom: from,
  dateTo: to,
  taskName: null,
  concurrency: CONCURRENCY,
  createdBy: 'benchmark',
});
scheduler.kick();
console.log(`补数单 #${bf.id}：${from} ~ ${to}，共 ${DAYS} 个分区，期望实例 ${gen.taskCount * DAYS} 个`);

// ---------- 3. 等待补数单全部到终态 ----------
const drained = await scheduler.waitUntilDrain(180_000, bf.id);
const wallMs = Date.now() - t0;
if (!drained) {
  console.error('✗ 120s 内未跑完');
  process.exit(1);
}

// ---------- 4. 结果校验（只统计本补数单的实例）----------
const all = store.db
  .prepare('SELECT * FROM task_instance WHERE backfill_id = ? ORDER BY id')
  .all(bf.id) as TaskInstance[];
const expected = gen.taskCount * DAYS;
const failCount = all.filter((i) => i.status !== 'success').length;
console.log(`\n实例总数=${all.length}（期望 ${expected}），非成功=${failCount}，墙钟=${wallMs}ms`);

// 4.1 无丢失
let pass = true;
if (all.length !== expected) {
  console.error(`✗ 实例数不符: ${all.length} != ${expected}`);
  pass = false;
}
if (failCount > 0) {
  console.error('✗ 存在非成功实例:', all.filter((i) => i.status !== 'success').slice(0, 5));
  pass = false;
}
// 4.2 无重复拉起：每个 (task,partition) 只有一行，且 attempt 不会超过 1（无失败重试）
const dupes = all.filter((i) => i.attempt !== 1);
if (dupes.length) {
  console.error(`✗ ${dupes.length} 个实例 attempt!=1，存在重复拉起`);
  pass = false;
}

// 4.3 依赖顺序校验：同分区内，子任务开始 >= 每个上游结束
const tasks = store.allTasks();
const nameOf = new Map(tasks.map((t) => [t.id as number, t.name as string]));
const upCache = new Map<number, number[]>();
const instKey = new Map(all.map((i) => [`${i.task_id}@${i.partition}`, i]));
let violated = 0;
let checked = 0;
for (const inst of all) {
  const ups = upCache.get(inst.task_id) ?? store.upstreamIds(inst.task_id);
  upCache.set(inst.task_id, ups);
  for (const upId of ups) {
    const upInst = instKey.get(`${upId}@${inst.partition}`);
    if (!upInst || !upInst.finished_at || !inst.started_at) continue;
    checked++;
    if (new Date(inst.started_at).getTime() < new Date(upInst.finished_at!).getTime() - 2) {
      violated++;
      if (violated <= 3) {
        console.error(
          `✗ 依赖违反: ${nameOf.get(upId)}(${upInst.finished_at}) -> ${nameOf.get(inst.task_id)}(${inst.started_at}) @ ${inst.partition}`,
        );
      }
    }
  }
}
console.log(`依赖边校验 ${checked} 条（×${DAYS} 分区），违反=${violated}`);
if (violated > 0) pass = false;

// 4.4 关键路径理论时长（按分区：取每个任务实际 duration，DAG 最长路径）
const durOf = new Map(all.map((i) => [`${i.task_id}@${i.partition}`, i.duration_ms ?? 0]));
const topo = store.topologicalSort();
let cpMax = 0;
const cpPerPartition: Record<string, number> = {};
for (const p of partitions) {
  const best = new Map<number, number>();
  for (const id of topo) {
    const own = durOf.get(`${id}@${p}`) ?? 0;
    const ups = store.upstreamIds(id);
    const pred = ups.length ? Math.max(...ups.map((u) => best.get(u) ?? 0)) : 0;
    best.set(id, pred + own);
  }
  const cp = Math.max(...best.values());
  cpPerPartition[p] = cp;
  cpMax = Math.max(cpMax, cp);
}
const ratio = wallMs / cpMax;
console.log(`\n单分区关键路径: 最小=${Math.min(...Object.values(cpPerPartition))}ms 最大=${cpMax}ms`);
console.log(`总墙钟=${wallMs}ms，关键路径=${cpMax}ms，比值=${ratio.toFixed(3)}（要求 <= 1.200）`);
if (ratio > 1.2) {
  console.error('✗ 超过关键路径 1.2 倍');
  pass = false;
}

// 4.5 补数单据状态
const progress = scheduler.instances.backfillProgress(bf.id);
console.log(`补数单状态=${progress.backfill?.status ?? '?'} 进度=${progress.done}/${progress.total}`);

// 吞吐统计
const totalDur = all.reduce((s, i) => s + (i.duration_ms ?? 0), 0);
console.log(`累计任务耗时=${totalDur}ms，平均并行度=${(totalDur / wallMs).toFixed(1)}，吞吐=${(all.length / (wallMs / 1000)).toFixed(0)} 实例/秒`);

await scheduler.stop();
store.close();

if (pass) {
  console.log('\n✅ 压测全部通过：依赖顺序正确、无重复拉起、总耗时在关键路径 1.2 倍以内');
  process.exit(0);
} else {
  console.error('\n❌ 压测存在未通过项');
  process.exit(1);
}
void sleep;
