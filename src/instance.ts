import type { Store } from './db.js';
import type { Backfill, TaskDef, TaskInstance } from './types.js';
import { addDays, dateRange, nowIso, partitionDate } from './config.js';

const TERMINAL = new Set(['success', 'failed', 'upstream_failed', 'killed']);
export const isTerminal = (s: string) => TERMINAL.has(s);

/**
 * 实例生命周期管理：
 *  - 按天物化 scheduled 实例
 *  - 补数单批量创建实例
 *  - 一键重跑（任务 + 全下游，指定分区）
 *  - 上游失败级联
 */
export class InstanceManager {
  constructor(private store: Store) {}

  /**
   * 确保某分区下所有启用任务都有实例（幂等）。
   * 按拓扑序插入，SLA 截止时刻在创建时算好。
   */
  materializePartition(partition: string, trigger: TaskInstance['trigger_type'] = 'scheduled') {
    const order = this.store.topologicalSort();
    const tasks = this.store.enabledTasks();
    const enabled = new Set(tasks.map((t) => t.id as number));
    const created: number[] = [];
    const tx = this.store.db.transaction(() => {
      for (const id of order) {
        if (!enabled.has(id)) continue;
        const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(id) as TaskDef;
        created.push(...this.ensureInstance(task, partition, trigger, null));
      }
    });
    tx();
    return created;
  }

  /** 保证单实例存在，返回新创建的实例 id（已存在则返回空数组） */
  ensureInstance(
    task: TaskDef,
    partition: string,
    trigger: TaskInstance['trigger_type'],
    backfillId: number | null,
  ): number[] {
    const existing = this.store.db
      .prepare('SELECT id FROM task_instance WHERE task_id = ? AND partition = ?')
      .get(task.id, partition);
    if (existing) return [];
    const info = this.store.db
      .prepare(
        `INSERT INTO task_instance
         (task_id, partition, status, attempt, trigger_type, backfill_id, queued_at,
          sla_deadline_at, sla_warned, sla_breached, created_at)
         VALUES (?,?, 'pending', 0, ?, ?, NULL, ?, 0, 0, ?)`,
      )
      .run(
        task.id,
        partition,
        trigger,
        backfillId,
        this.deadlineFor(task, partition),
        nowIso(),
      );
    const id = Number(info.lastInsertRowid);
    this.store.logEvent({
      instanceId: id,
      taskId: task.id,
      partition,
      event: 'instance_created',
      detail: trigger,
    });
    return [id];
  }

  /** 任务 SLA 截止时刻 = 分区日期当天的 HH:MM（UTC，演示环境时区无关） */
  deadlineFor(task: TaskDef, partition: string): string | null {
    if (!task.sla_deadline) return null;
    return `${partition}T${task.sla_deadline}:00.000Z`;
  }

  /** 最近 N 个成功实例的平均时长，作为 SLA 时长缺省值 */
  avgSuccessDuration(taskId: number): number | null {
    const row = this.store.db
      .prepare(
        `SELECT AVG(duration_ms) AS avg_ms FROM (
           SELECT duration_ms FROM task_instance
           WHERE task_id = ? AND status = 'success' AND duration_ms IS NOT NULL
           ORDER BY id DESC LIMIT 20
         )`,
      )
      .get(taskId) as { avg_ms: number | null };
    return row.avg_ms ? Math.round(row.avg_ms) : null;
  }

  /**
   * 创建补数单：[dateFrom, dateTo] × 任务集合。
   * taskName 为空 => 全 DAG；否则取该任务及其全部下游（按拓扑序）。
   * 已有实例的 (任务,分区) 跳过——补数不覆盖历史结果，重跑请走 rerun。
   */
  createBackfill(opts: {
    name: string;
    dateFrom: string;
    dateTo: string;
    taskName?: string | null;
    includeDownstream?: boolean;
    concurrency: number;
    createdBy: string;
  }): Backfill {
    const dates = dateRange(opts.dateFrom, opts.dateTo);
    let taskIds = this.store.topologicalSort();
    if (opts.taskName) {
      const root = this.store.getTaskByName(opts.taskName);
      if (!root) throw new Error(`任务不存在: ${opts.taskName}`);
      taskIds = opts.includeDownstream === false
        ? [Number(root.id)]
        : this.store.collectDownstream(Number(root.id), true);
    }
    const enabled = new Set(this.store.enabledTasks().map((t) => t.id as number));
    taskIds = taskIds.filter((id) => enabled.has(id));

    const bfId = Number(
      this.store.db
        .prepare(
          `INSERT INTO backfill (name, date_from, date_to, task_name, include_downstream, concurrency, status, created_by, created_at)
           VALUES (?,?,?,?,?,?,'created',?,?)`,
        )
        .run(
          opts.name,
          opts.dateFrom,
          opts.dateTo,
          opts.taskName ?? null,
          opts.includeDownstream === false ? 0 : 1,
          opts.concurrency,
          opts.createdBy,
          nowIso(),
        )
        .lastInsertRowid,
    );

    const created: number[] = [];
    const adopted: number[] = [];
    const tx = this.store.db.transaction(() => {
      // 日期在外、拓扑序在内：同一分区内严格按依赖顺序入队
      for (const d of dates) {
        for (const id of taskIds) {
          const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(id) as TaskDef;
          const made = this.ensureInstance(task, d, 'backfill', bfId);
          if (made.length) {
            created.push(...made);
            continue;
          }
          // (任务,分区) 已存在实例：纳入本补数单。成功的保留结果，其余重置待跑
          adopted.push(
            ...this.adoptExistingInstance(task.id, d, bfId),
          );
        }
      }
      this.store.db.prepare("UPDATE backfill SET status = 'running' WHERE id = ?").run(bfId);
    });
    tx();
    this.store.logEvent({
      event: 'backfill_created',
      detail: `#${bfId} ${opts.dateFrom}~${opts.dateTo} 新建 ${created.length} 个，接管已有 ${adopted.length} 个`,
    });
    return this.getBackfill(bfId)!;
  }

  /**
   * 把已存在的同 (任务,分区) 实例纳入补数单。
   *  - success：只关联 backfill_id，结果保留（补数进度直接计入）；
   *  - 其余非终态/失败态：重置为 pending 重新参与调度。
   * 返回受影响实例 id 列表。
   */
  private adoptExistingInstance(taskId: number, partition: string, backfillId: number): number[] {
    const existing = this.store.db
      .prepare('SELECT * FROM task_instance WHERE task_id = ? AND partition = ?')
      .get(taskId, partition) as TaskInstance | undefined;
    if (!existing) return [];
    if (existing.status === 'success') {
      this.store.db
        .prepare('UPDATE task_instance SET backfill_id = ? WHERE id = ? AND backfill_id IS NULL')
        .run(backfillId, existing.id);
      return [existing.id];
    }
    this.store.db
      .prepare(
        `UPDATE task_instance
         SET backfill_id = ?, status = 'pending', attempt = 0, run_token = NULL,
             lease_heartbeat = NULL, queued_at = NULL, started_at = NULL, finished_at = NULL,
             duration_ms = NULL, next_retry_at = NULL, error_message = NULL,
             sla_warned = 0, sla_breached = 0, trigger_type = 'backfill'
         WHERE id = ?`,
      )
      .run(backfillId, existing.id);
    this.store.logEvent({
      instanceId: existing.id,
      taskId,
      partition,
      event: 'backfill_adopted',
      detail: `原状态=${existing.status}`,
    });
    return [existing.id];
  }

  getBackfill(id: number): Backfill | undefined {
    return this.store.db.prepare('SELECT * FROM backfill WHERE id = ?').get(id) as
      | Backfill
      | undefined;
  }

  listBackfills(): Backfill[] {
    return this.store.db.prepare('SELECT * FROM backfill ORDER BY id DESC').all() as Backfill[];
  }

  /** 补数单进度：实例按状态聚合 */
  backfillProgress(id: number) {
    const rows = this.store.db
      .prepare(
        `SELECT status, COUNT(*) AS c FROM task_instance WHERE backfill_id = ? GROUP BY status`,
      )
      .all(id) as { status: string; c: number }[];
    const byStatus = Object.fromEntries(rows.map((r) => [r.status, r.c]));
    const total = rows.reduce((s, r) => s + r.c, 0);
    const done = (byStatus['success'] ?? 0) + (byStatus['failed'] ?? 0) +
      (byStatus['upstream_failed'] ?? 0) + (byStatus['killed'] ?? 0);
    return { id, total, done, byStatus, backfill: this.getBackfill(id) };
  }

  /** 补数单是否已全部到终态，是则回写单据状态 */
  refreshBackfillStatus(id: number): void {
    const bf = this.getBackfill(id);
    if (!bf || bf.status !== 'running') return;
    const p = this.backfillProgress(id);
    if (p.done < p.total) return;
    const finalStatus = p.byStatus['failed'] || p.byStatus['upstream_failed'] ? 'failed' : 'success';
    this.store.db
      .prepare('UPDATE backfill SET status = ?, finished_at = ? WHERE id = ?')
      .run(finalStatus, nowIso(), id);
    this.store.logEvent({
      event: 'backfill_finished',
      detail: `#${id} => ${finalStatus}（成功 ${p.byStatus['success'] ?? 0}/${p.total}）`,
    });
  }

  /**
   * 一键重跑：任务 + 全部下游，在给定分区上重置为 pending。
   * 正在运行的实例先 kill（由调度器执行进程配合租约中止），历史记录保留在 run_event。
   */
  rerunWithDownstream(taskName: string, partitions: string[], reason: string): number {
    const root = this.store.getTaskByName(taskName);
    if (!root) throw new Error(`任务不存在: ${taskName}`);
    const ids = this.store.collectDownstream(Number(root.id), true);
    let reset = 0;
    const tx = this.store.db.transaction(() => {
      for (const partition of partitions) {
        for (const id of ids) {
          const inst = this.store.db
            .prepare('SELECT * FROM task_instance WHERE task_id = ? AND partition = ?')
            .get(id, partition) as TaskInstance | undefined;
          const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(id) as TaskDef;
          if (!inst) {
            // 该分区从未物化过（例如手动重跑一个未来分区），直接建
            this.ensureInstance(task, partition, 'rerun', null);
            reset++;
            continue;
          }
          if (inst.status === 'running' || inst.status === 'queued' || inst.status === 'pending') {
            // 让调度器的执行循环放弃它；CAS 防并发
            this.store.db
              .prepare(
                `UPDATE task_instance SET status = 'killed', finished_at = ?, run_token = NULL
                 WHERE id = ? AND status IN ('running','queued','pending')`,
              )
              .run(nowIso(), inst.id);
          }
          this.store.db
            .prepare(
              `UPDATE task_instance
               SET status = 'pending', attempt = 0, run_token = NULL, lease_heartbeat = NULL,
                   queued_at = ?, started_at = NULL, finished_at = NULL, duration_ms = NULL,
                   next_retry_at = NULL, error_message = NULL, trigger_type = 'rerun',
                   sla_warned = 0, sla_breached = 0, sla_deadline_at = ?
               WHERE id = ?`,
            )
            .run(nowIso(), this.deadlineFor(task, partition), inst.id);
          this.store.logEvent({
            instanceId: inst.id,
            taskId: id,
            partition,
            event: 'rerun_reset',
            detail: reason,
          });
          reset++;
        }
      }
    });
    tx();
    return reset;
  }

  /** 人工 kill 一个实例 */
  killInstance(instanceId: number): boolean {
    const info = this.store.db
      .prepare(
        `UPDATE task_instance SET status = 'killed', finished_at = ?, run_token = NULL
         WHERE id = ? AND status NOT IN ('success','failed','upstream_failed','killed')`,
      )
      .run(nowIso(), instanceId);
    if (info.changes > 0) {
      this.store.logEvent({ instanceId, event: 'manual_kill' });
      return true;
    }
    return false;
  }

  /**
   * 上游失败级联：某实例终态失败后，把同分区所有（传递）下游的 pending 实例
   * 标记为 upstream_failed。已是运行/终态的不动。
   */
  cascadeUpstreamFailed(failedInst: TaskInstance, failedTaskName: string) {
    const downstream = this.store.collectDownstream(failedInst.task_id, false);
    if (!downstream.length) return;
    const placeholders = downstream.map(() => '?').join(',');
    const rows = this.store.db
      .prepare(
        `SELECT i.* FROM task_instance i
         WHERE i.partition = ? AND i.status = 'pending' AND i.task_id IN (${placeholders})`,
      )
      .all(failedInst.partition, ...downstream) as TaskInstance[];
    for (const inst of rows) {
      this.store.db
        .prepare(
          `UPDATE task_instance SET status = 'upstream_failed', finished_at = ?
           WHERE id = ? AND status = 'pending'`,
        )
        .run(nowIso(), inst.id);
      this.store.logEvent({
        instanceId: inst.id,
        taskId: inst.task_id,
        partition: inst.partition,
        event: 'upstream_failed',
        detail: `root=${failedTaskName}`,
      });
    }
  }

  /** 自动物化最近 N 天 + 明天的 scheduled 实例（幂等） */
  materializeRecent(lookbackDays: number) {
    const today = new Date();
    for (let offset = -lookbackDays; offset <= 1; offset++) {
      const d = new Date(today);
      d.setUTCDate(d.getUTCDate() + offset);
      this.materializePartition(partitionDate(d), 'scheduled');
    }
    void addDays; // 保留工具函数引用（dateRange 已间接使用）
  }
}
