import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import type { Scheduler } from './scheduler.js';
import { DagManager } from './dag.js';

/** REST API + 静态托管管理界面（ui/dist） */
export function createApp(scheduler: Scheduler, store: Scheduler['store']) {
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  const dag = new DagManager(store);
  const p = (req: express.Request, key: string): string => String(req.params[key]);
  const q = (req: express.Request, key: string): string | undefined => {
    const v = req.query[key];
    return v == null ? undefined : String(v);
  };

  // 统一错误包装
  const wrap =
    (fn: (req: express.Request, res: express.Response) => unknown | Promise<unknown>) =>
    async (req: express.Request, res: express.Response) => {
      try {
        const data = await fn(req, res);
        if (!res.headersSent) res.json({ ok: true, data });
      } catch (err) {
        res.status(400).json({ ok: false, error: (err as Error).message });
      }
    };

  // ---------- 任务定义 ----------
  app.get('/api/tasks', wrap(() => store.allTasks()));
  app.get('/api/tasks/:name', wrap((req) => store.getTaskByName(p(req, 'name')) ?? null));
  app.post(
    '/api/tasks',
    wrap((req) => {
      const id = dag.createTask(req.body);
      scheduler.kick();
      return { id };
    }),
  );
  app.patch(
    '/api/tasks/:name',
    wrap((req) => {
      dag.updateTask(p(req, 'name'), req.body);
      scheduler.kick();
      return { updated: p(req, 'name') };
    }),
  );
  app.delete(
    '/api/tasks/:name',
    wrap((req) => {
      dag.deleteTask(p(req, 'name'));
      return { deleted: p(req, 'name') };
    }),
  );
  app.post(
    '/api/dependencies',
    wrap((req) => {
      const { from, to } = req.body;
      dag.addEdge(from, to);
      scheduler.kick();
      return { edge: `${from}->${to}` };
    }),
  );
  app.delete(
    '/api/dependencies/:from/:to',
    wrap((req) => {
      dag.removeEdge(p(req, 'from'), p(req, 'to'));
      return { removed: `${p(req, 'from')}->${p(req, 'to')}` };
    }),
  );
  app.get(
    '/api/graph',
    wrap((req) =>
      scheduler.graphWithStatus(q(req, 'date') ?? new Date().toISOString().slice(0, 10)),
    ),
  );
  app.get('/api/graph/export', wrap(() => dag.exportGraph()));
  app.get('/api/topo', wrap(() => {
    const order = store.topologicalSort();
    const tasks = store.allTasks();
    return order.map((id) => tasks.find((t) => t.id === id)?.name);
  }));

  // ---------- 实例 ----------
  app.get(
    '/api/instances',
    wrap((req) =>
      scheduler.listInstances({
        partition: q(req, 'date'),
        taskId: q(req, 'taskId') ? Number(q(req, 'taskId')) : undefined,
        status: q(req, 'status'),
        limit: q(req, 'limit') ? Number(q(req, 'limit')) : undefined,
      }),
    ),
  );
  app.get('/api/instances/:id', wrap((req) => scheduler.getInstance(Number(p(req, 'id')))));
  app.post('/api/instances/:id/kill', wrap((req) => {
    const ok = scheduler.instances.killInstance(Number(p(req, 'id')));
    scheduler.kick();
    return { killed: ok };
  }));
  app.get('/api/events', wrap((req) =>
    scheduler.events(q(req, 'instanceId') ? Number(q(req, 'instanceId')) : undefined, 500),
  ));

  // ---------- 补数 ----------
  app.get('/api/backfills', wrap(() => scheduler.instances.listBackfills()));
  app.post(
    '/api/backfills',
    wrap((req) => {
      const bf = scheduler.instances.createBackfill({
        name: req.body.name,
        dateFrom: req.body.dateFrom,
        dateTo: req.body.dateTo,
        taskName: req.body.taskName ?? null,
        includeDownstream: req.body.includeDownstream,
        concurrency: Number(req.body.concurrency ?? 8),
        createdBy: req.body.createdBy ?? 'api',
      });
      scheduler.kick();
      return bf;
    }),
  );
  app.get('/api/backfills/:id', wrap((req) =>
    scheduler.instances.backfillProgress(Number(p(req, 'id'))),
  ));
  app.post('/api/backfills/:id/kill', wrap((req) => {
    const id = Number(p(req, 'id'));
    store
      .prepare("UPDATE backfill SET status = 'killed', finished_at = ? WHERE id = ? AND status = 'running'")
      .run(new Date().toISOString(), id);
    return { killed: id };
  }));

  // ---------- 一键重跑（任务 + 全下游 × 指定分区） ----------
  app.post(
    '/api/rerun',
    wrap((req) => {
      const partitions: string[] =
        req.body.partitions ?? [req.body.partition].filter(Boolean);
      if (!partitions.length) throw new Error('必须提供 partition 或 partitions');
      const reset = scheduler.instances.rerunWithDownstream(
        req.body.taskName,
        partitions,
        req.body.reason ?? '界面一键重跑',
      );
      scheduler.kick();
      return { resetInstances: reset };
    }),
  );

  // ---------- 手动触发单分区物化 ----------
  app.post('/api/materialize', wrap((req) => {
    const ids = scheduler.instances.materializePartition(String(req.body.date), 'manual');
    scheduler.kick();
    return { created: ids.length };
  }));

  // ---------- 告警 ----------
  app.get('/api/alerts', wrap((req) =>
    scheduler.alerts.list(Number(q(req, 'limit') ?? 100), q(req, 'unacked') === '1'),
  ));
  app.post('/api/alerts/:id/ack', wrap((req) => {
    scheduler.alerts.ack(Number(p(req, 'id')));
    return { acked: Number(p(req, 'id')) };
  }));

  // ---------- 运行时 ----------
  app.get('/api/runtime', wrap(() => ({
    running: scheduler.runningSnapshot(),
    maxConcurrent: scheduler['cfg'].maxConcurrent,
    dbPath: scheduler['cfg'].dbPath,
  })));
  app.get('/api/events/stream', (_req, res) => scheduler.attachSse(res));

  // ---------- 静态界面 ----------
  const uiDist = path.join(process.cwd(), 'ui', 'dist');
  if (fs.existsSync(uiDist)) {
    app.use(express.static(uiDist));
    app.get(/^(?!\/api\/).*/, (_req, res) => res.sendFile(path.join(uiDist, 'index.html')));
  }

  return app;
}
