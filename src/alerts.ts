/** Alert channels: console + DB (visible in UI). Optional webhook via SCHED_ALERT_WEBHOOK. */
import { getDb, now } from './db.ts';
import type { Alert, AlertType } from './types.ts';

function emitConsole(type: AlertType, message: string): void {
  const line = `[ALERT ${new Date().toISOString()}] ${type} :: ${message}`;
  if (type === 'TASK_FAILED' || type === 'SLA_BREACHED' || type === 'BACKFLOW_FAILED') {
    console.error(line);
  } else {
    console.warn(line);
  }
}

async function emitWebhook(type: AlertType, message: string, meta: Record<string, unknown>): Promise<void> {
  const url = process.env.SCHED_ALERT_WEBHOOK;
  if (!url) return;
  try {
    await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type, message, meta, at: now() }),
      signal: AbortSignal.timeout(3000),
    });
  } catch (err) {
    console.error(`[ALERT] webhook delivery failed: ${(err as Error).message}`);
  }
}

export function raiseAlert(
  type: AlertType,
  message: string,
  refs: { taskName?: string | null; instanceId?: number | null; logicalDate?: string | null } = {},
): Alert {
  const db = getDb();
  const info = db
    .prepare(
      `INSERT INTO alerts (type, task_name, instance_id, logical_date, message, channel, created_at)
       VALUES (?, ?, ?, ?, ?, 'console+db', ?)`,
    )
    .run(type, refs.taskName ?? null, refs.instanceId ?? null, refs.logicalDate ?? null, message, now());
  emitConsole(type, message);
  void emitWebhook(type, message, refs);
  return mapAlert(db.prepare('SELECT * FROM alerts WHERE id = ?').get(info.lastInsertRowid));
}

function mapAlert(row: unknown): Alert {
  const r = { ...(row as object) } as Record<string, unknown>;
  return {
    id: Number(r.id),
    type: String(r.type) as AlertType,
    taskName: (r.task_name as string | null) ?? null,
    instanceId: (r.instance_id as number | null) ?? null,
    logicalDate: (r.logical_date as string | null) ?? null,
    message: String(r.message),
    channel: String(r.channel),
    acked: Number(r.acked),
    createdAt: String(r.created_at),
  };
}

export function listAlerts(limit = 200, includeAcked = true): Alert[] {
  const db = getDb();
  const sql = includeAcked
    ? 'SELECT * FROM alerts ORDER BY id DESC LIMIT ?'
    : 'SELECT * FROM alerts WHERE acked = 0 ORDER BY id DESC LIMIT ?';
  return db.prepare(sql).all(limit).map((r) => mapAlert(r));
}

export function ackAlert(id: number): boolean {
  const info = getDb().prepare('UPDATE alerts SET acked = 1 WHERE id = ?').run(id);
  return info.changes > 0;
}
