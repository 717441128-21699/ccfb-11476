/** Minimal dependency-free HTTP server: REST API + static UI hosting. */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.ts';
import { getDb, plain } from './db.ts';
import { loadDag, criticalPathMs } from './dag.ts';
import { listAlerts, ackAlert } from './alerts.ts';
import {
  getBackfill,
  getInstance,
  listBackfills,
  listEdges,
  listInstances,
  listRuns,
  listTasks,
} from './repo.ts';
import { killInstance, rerunDownstream, retryInstance, startBackfill } from './backfill.ts';
import type { Scheduler } from './scheduler.ts';

const WEB_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'web');

type RouteMatch = RegExpExecArray;
type Handler = (body: unknown, url: URL, match: RouteMatch) => unknown;

export function createServer(scheduler: Scheduler | null): http.Server {
  const routes: Array<[string, RegExp, Handler]> = [
    ['GET', /^\/api\/health$/, () => ({ ok: true, ts: new Date().toISOString() })],
    ['GET', /^\/api\/tasks$/, () => listTasks()],
    ['GET', /^\/api\/edges$/, () => listEdges()],
    ['GET', /^\/api\/dag$/, () => dagPayload()],
    ['GET', /^\/api\/instances$/, (_b, url) => {
      const status = url.searchParams.get('status') ?? undefined;
      const date = url.searchParams.get('date') ?? undefined;
      const task = url.searchParams.get('task') ?? undefined;
      const limit = Number(url.searchParams.get('limit') ?? 500);
      return listInstances({ status, date, task, limit });
    }],
    ['GET', /^\/api\/instances\/(\d+)$/, (_b, _u, m) => {
      const id = Number(m![1]);
      const inst = getInstance(id);
      if (!inst) throw new HttpError(404, 'instance not found');
      return { instance: inst, runs: listRuns(id) };
    }],
    ['POST', /^\/api\/instances\/(\d+)\/retry$/, (_b, _u, m) => retryInstance(Number(m![1]))],
    ['POST', /^\/api\/instances\/(\d+)\/kill$/, (_b, _u, m) => {
      killInstance(Number(m![1]));
      scheduler?.trigger();
      return { ok: true };
    }],
    ['POST', /^\/api\/backfills$/, (body) => {
      const p = body as Record<string, unknown>;
      const res = startBackfill({
        startDate: String(p.startDate),
        endDate: String(p.endDate),
        rootTask: (p.rootTask as string | null) ?? null,
        rerunMode: Boolean(p.rerunMode),
        maxConcurrency: p.maxConcurrency as number | undefined,
        maxPerPartition: p.maxPerPartition as number | undefined,
        parallelPartitions: p.parallelPartitions as number | undefined,
      });
      scheduler?.trigger();
      return res;
    }],
    ['POST', /^\/api\/rerun-downstream$/, (body) => {
      const p = body as Record<string, unknown>;
      const dates = Array.isArray(p.dates) ? (p.dates as string[]) : [];
      const res = rerunDownstream(String(p.rootTask), dates);
      scheduler?.trigger();
      return res;
    }],
    ['GET', /^\/api\/backfills$/, () => listBackfills()],
    ['GET', /^\/api\/backfills\/(\d+)$/, (_b, _u, m) => {
      const id = Number(m![1]);
      const bf = getBackfill(id);
      if (!bf) throw new HttpError(404, 'backfill not found');
      const counts = getDb()
        .prepare('SELECT status, COUNT(*) c FROM instances WHERE backfill_id=? GROUP BY status')
        .all(id)
        .map((r) => { const p = plain<{ status: string; c: number }>(r); return { status: p.status, count: p.c }; });
      return { job: bf, counts };
    }],
    ['GET', /^\/api\/alerts$/, (_b, url) => listAlerts(Number(url.searchParams.get('limit') ?? 200), url.searchParams.get('acked') !== '0')],
    ['POST', /^\/api\/alerts\/(\d+)\/ack$/, (_b, _u, m) => ({ ok: ackAlert(Number(m![1])) })],
    ['GET', /^\/api\/events$/, (_b, url) => {
      const limit = Number(url.searchParams.get('limit') ?? 200);
      const instanceId = url.searchParams.get('instanceId');
      const sql = instanceId
        ? 'SELECT * FROM events WHERE instance_id=? ORDER BY id DESC LIMIT ?'
        : 'SELECT * FROM events ORDER BY id DESC LIMIT ?';
      const params = instanceId ? [Number(instanceId), limit] : [limit];
      return getDb().prepare(sql).all(...params);
    }],
  ];

  const server = http.createServer((req, res) => {
    void handle(req, res, routes, scheduler);
  });
  return server;
}

function dagPayload(): unknown {
  const dag = loadDag();
  const today = new Date().toISOString().slice(0, 10);
  const instRows = getDb()
    .prepare('SELECT task_name, status, duration_ms, attempt FROM instances WHERE logical_date = ?')
    .all(today) as Array<{ task_name: string; status: string; duration_ms: number | null; attempt: number }>;
  const todayMap = new Map(instRows.map((r) => [r.task_name, {
    status: r.status,
    durationMs: r.duration_ms,
    attempt: r.attempt,
  }]));
  const nodes = [...dag.nodes.values()].map((n) => ({
    name: n.task.name,
    owner: n.task.owner,
    layer: n.layer,
    expectedDurationMs: n.task.expectedDurationMs,
    slaMs: n.task.slaMs,
    today: todayMap.get(n.task.name) ?? null,
  }));
  const edges = listEdges();
  const cpExpected = criticalPathMs(dag, (name) => dag.nodes.get(name)!.task.expectedDurationMs);
  return { nodeCount: nodes.length, edgeCount: edges.length, layerCount: dag.layers.length, criticalPathExpectedMs: cpExpected, nodes, edges, layers: dag.layers };
}

class HttpError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
  }
}

async function handle(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  routes: Array<[string, RegExp, Handler]>,
  scheduler: Scheduler | null,
): Promise<void> {
  try {
    const url = new URL(req.url ?? '/', 'http://localhost');
    if (url.pathname.startsWith('/api/')) {
      let body: unknown = null;
      if (req.method === 'POST') {
        body = await readJson(req);
      }
      for (const [method, re, handler] of routes) {
        if (method !== req.method) continue;
        const m = re.exec(url.pathname);
        if (!m) continue;
        const data = handler(body, url, m);
        res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(data));
        return;
      }
      throw new HttpError(404, `no route for ${req.method} ${url.pathname}`);
    }
    serveStatic(url.pathname, res);
  } catch (err) {
    const status = err instanceof HttpError ? err.status : 500;
    if (status === 500) console.error('[http]', err);
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: (err as Error).message }));
  }
}

function readJson(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch (e) { reject(new HttpError(400, `invalid JSON: ${(e as Error).message}`)); }
    });
    req.on('error', reject);
  });
}

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.json': 'application/json; charset=utf-8',
};

function serveStatic(pathname: string, res: http.ServerResponse): void {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const file = path.normalize(path.join(WEB_DIR, rel));
  if (!file.startsWith(WEB_DIR)) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404, { 'content-type': 'text/plain' }); res.end('not found'); return;
  }
  res.writeHead(200, { 'content-type': MIME[path.extname(file)] ?? 'application/octet-stream' });
  fs.createReadStream(file).pipe(res);
}

export function startHttpServer(scheduler: Scheduler | null): http.Server {
  const server = createServer(scheduler);
  server.listen(config.httpPort, () => {
    console.log(`[http] management UI: http://localhost:${config.httpPort}/  (API base /api)`);
  });
  return server;
}
