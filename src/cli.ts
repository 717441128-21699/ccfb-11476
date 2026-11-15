import type { Store } from './db.js';
import type { Scheduler } from './scheduler.js';
import { generateSyntheticDag } from './synthetic.js';

function parseFlags(rest: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < rest.length; i++) {
    if (rest[i].startsWith('--')) flags[rest[i].slice(2)] = rest[i + 1] && !rest[i + 1].startsWith('--') ? rest[++i] : 'true';
  }
  return flags;
}

/** 命令行管理入口（serve 之外的所有操作） */
export async function runCli(scheduler: Scheduler, store: Store, cmd: string, rest: string[]) {
  const flags = parseFlags(rest);
  switch (cmd) {
    case 'gen-dag': {
      // 生成合成 DAG 与假执行脚本
      const n = Number(flags.tasks ?? 200);
      const layers = Number(flags.layers ?? 8);
      const outDir = flags['script-dir'] ?? 'scripts/jobs';
      const res = generateSyntheticDag(store, {
        taskCount: n,
        layerCount: layers,
        scriptDir: outDir,
        durationMs: Number(flags['duration-ms'] ?? 0) || undefined,
        failDemo: flags['fail-demo'] === 'true',
      });
      console.log(`[gen-dag] 任务 ${res.taskCount} 个，依赖边 ${res.edgeCount} 条，层级 ${res.layerCount}`);
      console.log(`[gen-dag] 假脚本目录: ${res.scriptDir}（${res.scriptCount} 个 .cjs）`);
      break;
    }

    case 'tasks': {
      const tasks = store.allTasks();
      console.table(
        tasks.map((t) => ({
          id: t.id,
          name: t.name,
          owner: t.owner,
          executor: t.executor_type,
          retry: t.retry_max,
          sla: t.sla_deadline ?? `${t.sla_duration_ms ?? '-'}ms`,
          enabled: t.enabled ? 'Y' : 'N',
        })),
      );
      break;
    }

    case 'graph': {
      const g = scheduler.graphWithStatus(flags.date ?? new Date().toISOString().slice(0, 10));
      console.log(`节点 ${g.nodes.length}，边 ${g.edges.length}`);
      const byLayer = new Map<number, number>();
      for (const n of g.nodes) byLayer.set(n.layer, (byLayer.get(n.layer) ?? 0) + 1);
      console.log('各层节点数:', [...byLayer.entries()].sort((a, b) => a[0] - b[0]));
      break;
    }

    case 'backfill': {
      const bf = scheduler.instances.createBackfill({
        name: flags.name ?? `cli-${flags.from}_${flags.to}`,
        dateFrom: flags.from,
        dateTo: flags.to,
        taskName: flags.task || null,
        includeDownstream: flags['include-downstream'] !== 'false',
        concurrency: Number(flags.concurrency ?? 8),
        createdBy: 'cli',
      });
      console.log(`[backfill] 已创建 #${bf.id} ${bf.date_from}~${bf.date_to}`);
      break;
    }

    case 'rerun': {
      const partitions = rest.filter((a) => !a.startsWith('--') && a.match(/^\d{4}-\d{2}-\d{2}$/));
      const reset = scheduler.instances.rerunWithDownstream(
        flags.task,
        partitions.length ? partitions : [flags.date],
        flags.reason ?? 'CLI 重跑',
      );
      console.log(`[rerun] 已重置 ${reset} 个实例（${flags.task} 及其下游 × ${partitions.length || 1} 个分区）`);
      break;
    }

    case 'instances': {
      const rows = scheduler.listInstances({
        partition: flags.date,
        status: flags.status,
        limit: Number(flags.limit ?? 50),
      });
      console.table(
        rows.map((r) => ({
          id: (r as Record<string, unknown>).id,
          task: (r as Record<string, unknown>).task_name,
          partition: (r as Record<string, unknown>).partition,
          status: (r as Record<string, unknown>).status,
          attempt: (r as Record<string, unknown>).attempt,
          duration_ms: (r as Record<string, unknown>).duration_ms,
        })),
      );
      break;
    }

    case 'alerts': {
      console.table(scheduler.alerts.list(Number(flags.limit ?? 50), flags.unacked === 'true'));
      break;
    }

    default:
      console.error(`未知命令: ${cmd}
可用命令:
  gen-dag   [--tasks 200] [--layers 8] [--script-dir scripts/jobs] [--fail-demo true]
  tasks
  graph     [--date YYYY-MM-DD]
  backfill  --from YYYY-MM-DD --to YYYY-MM-DD [--task NAME] [--concurrency N] [--include-downstream false]
  rerun     --task NAME (--date D | D D ...) [--reason TEXT]
  instances [--date D] [--status S] [--limit N]
  alerts    [--unacked true]`);
      process.exitCode = 1;
  }
}
