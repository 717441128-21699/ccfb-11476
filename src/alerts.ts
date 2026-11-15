import type { Store } from './db.js';
import type { Alert, TaskInstance, TaskDef } from './types.js';
import { nowIso } from './config.js';

/**
 * 告警中心：落库 + 控制台输出 + 可选 Webhook。
 * 同一 (instance, type) 只发一次，靠标志位/去重查询保证重启不重复轰炸。
 */
export class AlertManager {
  constructor(
    private store: Store,
    private webhookUrl: string | null,
  ) {}

  private insert(
    type: Alert['type'],
    level: Alert['level'],
    message: string,
    refs: { taskId?: number | null; instanceId?: number | null; partition?: string | null },
  ) {
    const createdAt = nowIso();
    const info = this.store
      .prepare(
        `INSERT INTO alert (task_id, instance_id, partition, level, channel, type, message, acknowledged, created_at)
         VALUES (?,?,?,?,?,?,?,0,?)`,
      )
      .run(
        refs.taskId ?? null,
        refs.instanceId ?? null,
        refs.partition ?? null,
        level,
        this.webhookUrl ? 'webhook' : 'log',
        type,
        message,
        createdAt,
      );
    const id = Number(info.lastInsertRowid);
    const line = `[ALERT ${level} ${type}] ${message}`;
    if (level === 'critical') console.error(line);
    else console.warn(line);
    if (this.webhookUrl) this.postWebhook(id, level, type, message).catch(() => undefined);
    return id;
  }

  private async postWebhook(id: number, level: string, type: string, message: string) {
    try {
      await fetch(this.webhookUrl!, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ id, level, type, message, source: 'flowline', ts: nowIso() }),
      });
    } catch {
      /* webhook 不可用不影响主流程，告警已落库 */
    }
  }

  /** 重试耗尽：负责人收到 critical */
  notifyRetryExhausted(task: TaskDef, inst: TaskInstance) {
    this.insert(
      'retry_exhausted',
      'critical',
      `任务 ${task.name} 分区 ${inst.partition} 重试 ${inst.attempt} 次仍失败（负责人: ${task.owner}）: ${inst.error_message ?? ''}`,
      { taskId: task.id, instanceId: inst.id, partition: inst.partition },
    );
  }

  notifyUpstreamFailed(task: TaskDef, inst: TaskInstance, upstreamName: string) {
    // 同一实例只记一次上游失败
    const exists = this.store
      .prepare(
        `SELECT 1 FROM alert WHERE instance_id = ? AND type = 'upstream_failed'`,
      )
      .get(inst.id);
    if (exists) return;
    this.insert(
      'upstream_failed',
      'warn',
      `任务 ${task.name} 分区 ${inst.partition} 因上游 ${upstreamName} 失败被跳过`,
      { taskId: task.id, instanceId: inst.id, partition: inst.partition },
    );
  }

  /** SLA 预警：按剩余预算判断，而不是等违约后才喊 */
  notifySlaWarn(task: TaskDef, inst: TaskInstance, detail: string) {
    if (inst.sla_warned) return;
    this.store
      .prepare('UPDATE task_instance SET sla_warned = 1 WHERE id = ? AND sla_warned = 0')
      .run(inst.id);
    this.insert('sla_warn', 'warn', `SLA 预警 · ${task.name} 分区 ${inst.partition}: ${detail}`, {
      taskId: task.id,
      instanceId: inst.id,
      partition: inst.partition,
    });
  }

  notifySlaBreach(task: TaskDef, inst: TaskInstance, detail: string) {
    if (inst.sla_breached) return;
    this.store
      .prepare('UPDATE task_instance SET sla_breached = 1 WHERE id = ? AND sla_breached = 0')
      .run(inst.id);
    this.insert(
      'sla_breach',
      'critical',
      `SLA 违约 · ${task.name} 分区 ${inst.partition}: ${detail}（负责人: ${task.owner}）`,
      { taskId: task.id, instanceId: inst.id, partition: inst.partition },
    );
  }

  ack(id: number) {
    this.store.prepare('UPDATE alert SET acknowledged = 1 WHERE id = ?').run(id);
  }

  list(limit = 100, onlyUnacked = false) {
    return this.store
      .prepare(
        `SELECT a.*, t.name AS task_name FROM alert a
         LEFT JOIN task_def t ON t.id = a.task_id
         WHERE ${onlyUnacked ? 'a.acknowledged = 0 AND' : ''} 1=1
         ORDER BY a.id DESC LIMIT ?`,
      )
      .all(limit);
  }
}
