import fs from 'node:fs';
import path from 'node:path';
import type { Store } from './db.js';
import { DagManager } from './dag.js';
import { nowIso } from './config.js';

export interface SyntheticOptions {
  taskCount: number;       // 总任务数（含根）
  layerCount: number;      // DAG 层数
  scriptDir: string;       // 假执行脚本输出目录
  durationMs?: number;     // mock 基准耗时，缺省按层宽自动给
  failDemo?: boolean;      // 注入确定性失败（演示重试/告警/上游失败级联）
}

/** 确定性伪随机（无 Math.random，保证每次生成同一张图，压测可复现） */
function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * 生成一张分层 DAG：
 *  - 第 0 层为根任务（少量），其余任务均匀分布在后续层；
 *  - 每条边只从更早的层连来，保证无环；
 *  - 每个非根节点至少 1 个上游，根节点无上游；
 *  - 同时为每个任务落一个真实可跑的 Node 假脚本。
 */
export function generateSyntheticDag(store: Store, opts: SyntheticOptions) {
  const dag = new DagManager(store);
  const rand = mulberry32(20261116);
  const owners = ['alice', 'bob', 'carol', 'dave', 'erin'];

  // 每层节点数：根层 5 个，其余均分
  const rootCount = 5;
  const rest = opts.taskCount - rootCount;
  const perLayer = Math.floor(rest / (opts.layerCount - 1));
  const layers: string[][] = [];
  layers.push(Array.from({ length: rootCount }, (_, i) => `dim_root_${i}`));
  let seq = 0;
  for (let l = 1; l < opts.layerCount; l++) {
    const count = l === opts.layerCount - 1 ? rest - perLayer * (opts.layerCount - 2) : perLayer;
    const names: string[] = [];
    for (let i = 0; i < count; i++) names.push(`l${l}_task_${String(seq++).padStart(3, '0')}`);
    layers.push(names);
  }

  const baseDuration = opts.durationMs ?? 200;
  const scriptDirAbs = path.resolve(opts.scriptDir);
  fs.mkdirSync(scriptDirAbs, { recursive: true });

  // 共享假脚本运行器：每个任务的 wrapper 只是转发到它
  const worker = `// 由 gen-dag 自动生成的假执行脚本运行器（真实子进程模式可用）
// 读取环境变量 FLOWLINE_TASK / FLOWLINE_PARTITION / FLOWLINE_ATTEMPT
const task = process.env.FLOWLINE_TASK || 'unknown';
const partition = process.env.FLOWLINE_PARTITION || 'unknown';
const attempt = Number(process.env.FLOWLINE_ATTEMPT || '1');
const duration = Number(process.argv[2] || 200);
const failTimes = Number(process.argv[3] || 0);
const started = Date.now();
const timer = setInterval(() => {
  process.stdout.write(\`[\${task}] \${partition} attempt=\${attempt} running \${Date.now() - started}ms\\n\`);
}, Math.max(50, duration / 4));
setTimeout(() => {
  clearInterval(timer);
  if (attempt <= failTimes) {
    console.error(\`[\${task}] 模拟失败 attempt=\${attempt}\`);
    process.exit(1);
  }
  console.log(JSON.stringify({ task, partition, attempt, durationMs: Date.now() - started, status: 'ok' }));
  process.exit(0);
}, duration);
`;
  fs.writeFileSync(path.join(scriptDirAbs, '_worker.cjs'), worker, 'utf8');

  const create = (name: string, layer: number, upstreamNames: string[]) => {
    // 时长随层轻微增长，让关键路径更真实
    const dur = baseDuration + layer * 10;
    // 失败/SLA 演示任务按层级+固定序号选取，适配任意任务规模
    const isFailDemo = opts.failDemo && layer === 2 && name.endsWith('_005');
    const isSlaBreach = opts.failDemo && layer === 3 && name.endsWith('_010');
    const failTimes = isFailDemo ? 2 : 0;
    const slaDuration = isSlaBreach ? 1 : undefined; // 1ms 预算 => 必违约
    const wrapperPath = path.join(scriptDirAbs, `${name}.cjs`);
    fs.writeFileSync(
      wrapperPath,
      `// 自动生成：任务 ${name} 的假脚本（subprocess 执行器使用；mock 模式下不调用）\nrequire('child_process').spawnSync(process.execPath, [require('path').join(__dirname, '_worker.cjs'), '${dur}', '${failTimes}'], { stdio: 'inherit', env: process.env });\n`,
      'utf8',
    );
    // 幂等：任务定义已存在则只刷新脚本，不重复建
    if (store.getTaskByName(name)) return;
    dag.createTask({
      name,
      owner: owners[Math.floor(rand() * owners.length)],
      description: `合成任务 ${name}（第 ${layer} 层）`,
      executor_type: 'mock',
      executor_config: {
        mockDurationMs: dur,
        jitterRatio: 0.1,
        failTimes,
        script: wrapperPath,
      },
      retry_max: failTimes ? 3 : 1,
      retry_delay_ms: 200,
      timeout_ms: 60_000,
      sla_duration_ms: slaDuration,
      sla_warn_ratio: 0.8,
      upstreamNames,
    });
  };

  // 根层
  for (const name of layers[0]) create(name, 0, []);

  // 后续层：1~3 个上游，来自上一层为主、偶尔跨层；保证连通
  for (let l = 1; l < layers.length; l++) {
    const prev = layers[l - 1];
    for (const name of layers[l]) {
      const upCount = 1 + Math.floor(rand() * Math.min(3, prev.length));
      const ups = new Set<string>();
      while (ups.size < upCount) ups.add(prev[Math.floor(rand() * prev.length)]);
      // 10% 概率再加一条跨层边（增强图的复杂度）
      if (l >= 2 && rand() < 0.1) {
        const earlier = layers[Math.floor(rand() * (l - 1))];
        ups.add(earlier[Math.floor(rand() * earlier.length)]);
      }
      create(name, l, [...ups]);
    }
  }

  // 给两个根任务配时钟型 SLA（当天截止时刻），演示 deadline 预警
  if (opts.failDemo) {
    const soon = new Date(Date.now() + 30_000).toISOString().slice(11, 16);
    for (const name of ['dim_root_0', 'dim_root_1']) {
      const t = store.getTaskByName(name)!;
      store.db.prepare('UPDATE task_def SET sla_deadline = ? WHERE id = ?').run(soon, t.id);
    }
  }

  const edgeCount = store.allEdges().length;
  store.logEvent({ event: 'synthetic_dag_generated', detail: `${opts.taskCount} tasks @ ${nowIso()}` });
  return {
    taskCount: store.allTasks().length,
    edgeCount,
    layerCount: layers.length,
    scriptDir: scriptDirAbs,
    scriptCount: fs.readdirSync(scriptDirAbs).filter((f) => f.endsWith('.cjs')).length,    layers: layers.map((l) => l.length),
  };
}
