/** Append-only event log helper. */
import { getDb, now } from './db.ts';

export function logEvent(
  eventType: string,
  refs: { instanceId?: number | null; taskName?: string | null; logicalDate?: string | null; detail?: string | null } = {},
): void {
  getDb()
    .prepare('INSERT INTO events (instance_id, task_name, logical_date, event_type, detail, created_at) VALUES (?, ?, ?, ?, ?, ?)')
    .run(refs.instanceId ?? null, refs.taskName ?? null, refs.logicalDate ?? null, eventType, refs.detail ?? null, now());
}
