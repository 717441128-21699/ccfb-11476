/**
 * 重启恢复测试（需求 6）：
 *   A. 调度器在实例 running 中途被杀 -> 新进程接管，实例重新入队并最终成功，
 *      状态不丢、不会出现两个执行体同时写同一实例（attempt 连续、无重复成功）
 *   B. 排队退避（retry 等待）中被杀 -> 重启后重试正常完成
 *   C. 领导锁：旧租约未过期时第二个调度器拒绝启动
 */
import fs from 'node:fs';
import { Store } from '../src/db.js';
import { Scheduler } from '../src/scheduler.js';
import { DagManager } from '../src/dag.js';
import type { SchedulerConfig } from '../src/types.js';
import { sleep } from '../src/config.js';

const DB_PATH = 'data/recovery-test.db';
let BACKFILL_ID = 0;
for (const f of [DB_PATH, `${DB_PATH}-wal`, `${DB_PATH}-shm`]) {
  try { fs.unlinkSync(f); } catch { /* ignore */ }
}

const cfg: SchedulerConfig = {
  dbPath: DB_PATH,
  maxConcurrent: 4, // 故意压小并发，制造 running/queued 并存
  pollIntervalMs: 50,
  leaseTimeoutMs: 5_000,
  alertWebhookUrl: null,
  historyLookbackDays: 0,
};

function newScheduler() {
  const store = new Store(DB_PATH);
  return { store, scheduler: new Scheduler(store, cfg) };
}

// ---------- 建小型 DAG：a -> b,c -> d，带一个前两次失败的任务 ----------
{
  const { store, scheduler } = newScheduler();
  const dag = new DagManager(store);
  const common = {
    owner: 'qa',
    executor_type: 'mock' as const,
    retry_delay_ms: 500,
  };
  dag.createTask({ ...common, name: 'rec_a', executor_config: { mockDurationMs: 800 } });
  dag.createTask({
    ...common,
    name: 'rec_b',
    upstreamNames: ['rec_a'],
    executor_config: { mockDurationMs: 1500, failTimes: 2 }, // 前两次失败 => 第三次成功
    retry_max: 3,
  });
  dag.createTask({ ...common, name: 'rec_c', upstreamNames: ['rec_a'], executor_config: { mockDurationMs: 2000 } });
  dag.createTask({ ...common, name: 'rec_d', upstreamNames: ['rec_b', 'rec_c'], executor_config: { mockDurationMs: 600 } });
  const bf = scheduler.instances.createBackfill({
    name: 'recovery',
    dateFrom: '2026-11-01',
    dateTo: '2026-11-03', // 3 个分区 × 4 任务 = 12 实例
    taskName: null,
    concurrency: 2,
    createdBy: 'test',
  });
  BACKFILL_ID = bf.id;
  store.close();
}

// ---------- 第一次启动，跑 1.2 秒后硬杀（此时有 running 实例）----------
let proc1: { store: Store; scheduler: Scheduler };
{
  proc1 = newScheduler();
  await proc1.scheduler.start();
  void proc1.scheduler.dispatchLoop();
  await sleep(1200);
  const running = proc1.scheduler.runningSnapshot();
  if (running.length === 0) throw new Error('前置条件失败：杀进程时没有 running 实例');
  console.log(`[kill-1] 硬杀调度器，当时在跑: ${JSON.stringify(running)}`);
  // 模拟进程崩溃：simulateCrash 保留锁和 running 状态，再关 db 句柄
  proc1.scheduler.simulateCrash();
  proc1.store.close();
}

// ---------- 第二次启动（崩溃后立即重启，租约未过期，显式 takeover）----------
{
  const { store, scheduler } = newScheduler();
  await scheduler.start({ takeover: true });
  void scheduler.dispatchLoop();

  // 领导锁互斥检查：另一个未授权接管的调度器必须被拒
  const rivalStore = new Store(DB_PATH);
  let rejected = false;
  try {
    const rival = new Scheduler(rivalStore, cfg);
    await rival.start();
    await rival.stop();
  } catch (err) {
    rejected = true;
    console.log(`[lock] 并发调度器被正确拒绝: ${(err as Error).message}`);
  }
  rivalStore.close();
  if (!rejected) throw new Error('领导锁互斥失效');

  // 跑 2.5 秒后再杀一次（覆盖 ret​ry 退避/queued 场景）
  await sleep(2500);
  const mid = store.db.prepare("SELECT status, COUNT(*) c FROM task_instance GROUP BY status").all() as { status: string; c: number }[];
  console.log(`[kill-2] 第二次硬杀前状态分布: ${JSON.stringify(mid)}`);
  scheduler.simulateCrash();
  store.close();
}

// ---------- 第三次启动（再次崩溃后接管）：跑完并校验 ----------
const { store, scheduler } = newScheduler();
await scheduler.start({ takeover: true });
void scheduler.dispatchLoop();
const ok = await scheduler.waitUntilDrain(60_000, BACKFILL_ID);
if (!ok) throw new Error('恢复后 60s 内补数单未跑完');

const allInstances = store.db.prepare('SELECT * FROM task_instance ORDER BY id').all() as {
  name?: string;
  status: string;
  attempt: number;
  partition: string;
  task_id: number;
  run_token: string | null;
  backfill_id: number | null;
}[];
// 只校验本测试补数单的 12 个实例（start() 自动物化的当天 scheduled 实例不在断言范围）
const instances = allInstances.filter((i) => i.backfill_id != null);
let pass = true;
const byStatus = new Map<string, number>();
for (const i of instances) byStatus.set(i.status, (byStatus.get(i.status) ?? 0) + 1);
console.log(
  `[result] 补数实例 ${instances.length} 个，终态分布: ${JSON.stringify([...byStatus.entries()])}（另有自动物化 scheduled 实例 ${allInstances.length - instances.length} 个）`,
);

// 1. 12 个实例全部成功，无丢失
if (instances.length !== 12) {
  console.error(`✗ 补数实例数应为 12，实际 ${instances.length}`);
  pass = false;
}
if (byStatus.get('success') !== 12) {
  console.error('✗ 存在非成功实例');
  pass = false;
}
// 2. 无残留 running/租约
const stale = instances.filter((i) => i.run_token !== null);
if (stale.length) {
  console.error('✗ 终态实例仍持有 run_token');
  pass = false;
}
// 3. rec_b 前两次失败，最终 attempt=3；其他任务 attempt=1（恢复重跑不计重复成功）
const bInst = instances.filter((i) => {
  const t = store.db.prepare('SELECT name FROM task_def WHERE id = ?').get(i.task_id) as { name: string };
  return t.name === 'rec_b';
});
for (const i of bInst) {
  if (i.attempt !== 3) {
    console.error(`✗ rec_b 应在第 3 次尝试成功，实际 attempt=${i.attempt} @${i.partition}`);
    pass = false;
  }
}
const otherInst = instances.filter((i) => {
  const t = store.db.prepare('SELECT name FROM task_def WHERE id = ?').get(i.task_id) as { name: string };
  return t.name !== 'rec_b';
});
for (const i of otherInst) {
  if (i.attempt !== 1) {
    console.error(`✗ 非失败任务 attempt 应为 1，实际 ${i.attempt}`);
    pass = false;
  }
}
// 4. 恢复事件有审计
  const recoveries = store.db.prepare("SELECT COUNT(*) c FROM run_event WHERE event = 'recovered_on_boot'").get() as { c: number };
console.log(`[audit] recovered_on_boot 事件 ${recoveries.c} 条，run_event 共 ${store.db.prepare('SELECT COUNT(*) c FROM run_event').get() ? (store.db.prepare('SELECT COUNT(*) c FROM run_event').get() as { c: number }).c : 0} 条`);
if (recoveries.c === 0) {
  console.error('✗ 缺少恢复审计事件');
  pass = false;
}
// 5. 迟到回写拦截点存在（检查被 kill 的旧实例最终只成功一次）
const successEvents = store.db
  .prepare("SELECT instance_id, COUNT(*) c FROM run_event WHERE event = 'instance_success' GROUP BY instance_id")
  .all() as { instance_id: number; c: number }[];
if (successEvents.some((e) => e.c > 1)) {
  console.error('✗ 存在实例被标记成功多次（重复拉起）');
  pass = false;
}

await scheduler.stop();
store.close();
if (!pass) {
  console.error('\n❌ 重启恢复测试失败');
  process.exit(1);
}
console.log('\n✅ 重启恢复测试通过：两次硬杀后状态不丢、无重复拉起、失败重试延续、领导锁互斥有效');
