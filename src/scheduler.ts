import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { Response } from 'express';
import type { Store } from './db.js';
import type { SchedulerConfig, TaskDef, TaskInstance } from './types.js';
import { AlertManager } from './alerts.js';
import { InstanceManager, isTerminal } from './instance.js';
import { pickExecutor } from './executor.js';
import { nowIso, sleep } from './config.js';

interface RunningHandle {
  instanceId: number;
  token: string;
  abort: () => void;
}

/**
 * 调度器内核（单实例领导模型）。
 *
 * 不重复拉起的保证：
 *  1. scheduler_lock 领导租约：同一时刻只有一个调度器能派发；
 *  2. 启动恢复把无主的 running 实例打回 queued；
 *  3. 每次拉起用条件 UPDATE（CAS）把 queued -> running 并写入一次性 run_token，
 *     执行器心跳与完成回写都校验 token，旧进程的迟到回写会被拒绝。
 */
export class Scheduler {
  readonly instances: InstanceManager;
  readonly alerts: AlertManager;
  readonly bus = new EventEmitter();
  private token = randomUUID();
  private running = new Map<number, RunningHandle>();
  private started = false;
  private timers: NodeJS.Timeout[] = [];
  private waiters: Array<() => void> = [];
  private lastFullPromote = 0;

  constructor(
    private store: Store,
    private cfg: SchedulerConfig,
  ) {
    this.instances = new InstanceManager(store);
    this.alerts = new AlertManager(store, cfg.alertWebhookUrl);
  }

  // ---------------- 领导锁 ----------------
  /**
   * 获取领导权。
   * @param takeover 为 true 时，若锁被旧 token 持有（典型场景：旧进程崩溃后立即重启，
   * 租约尚未超时），强制接管。调用方应确保旧进程确已死亡（如本机重启/容器重建）。
   */
  private acquireLeadership(takeover = false): boolean {
    const row = this.store.db.prepare('SELECT * FROM scheduler_lock WHERE id = 1').get() as
      | { owner_token: string; heartbeat_at: string }
      | undefined;
    const now = nowIso();
    if (!row) {
      this.store.db
        .prepare('INSERT INTO scheduler_lock (id, owner_token, heartbeat_at) VALUES (1, ?, ?)')
        .run(this.token, now);
      return true;
    }
    const stale =
      Date.now() - new Date(row.heartbeat_at).getTime() > this.cfg.leaseTimeoutMs * 2;
    if (row.owner_token === this.token) return true;
    if (takeover || stale) {
      this.store.db
        .prepare('UPDATE scheduler_lock SET owner_token = ?, heartbeat_at = ? WHERE id = 1')
        .run(this.token, now);
      console.warn(
        `[scheduler] ${stale ? '检测到旧调度器租约过期' : 'takeover 模式'}，接管集群（旧 token=${row.owner_token.slice(0, 8)}）`,
      );
      return true;
    }
    throw new Error('已有调度器在运行（领导租约未过期），如需强制接管请先停掉旧进程或用 takeover 模式');
  }

  private heartbeatLeadership() {
    this.store.db
      .prepare('UPDATE scheduler_lock SET heartbeat_at = ? WHERE id = 1 AND owner_token = ?')
      .run(nowIso(), this.token);
  }

  // ---------------- 启动 / 恢复 ----------------
  async start(opts: { takeover?: boolean } = {}) {
    if (this.started) return;
    try {
      this.acquireLeadership(opts.takeover === true);
    } catch (err) {
      throw new Error((err as Error).message);
    }
    this.started = true;
    this.recoverOrphans();
    this.instances.materializeRecent(this.cfg.historyLookbackDays);

    // 领导心跳
    this.timers.push(setInterval(() => this.started && this.heartbeatLeadership(), Math.max(500, this.cfg.leaseTimeoutMs / 3)));
    // 派发循环（事件唤醒 + 兜底轮询双保险）
    this.timers.push(setInterval(() => this.started && this.kick(), this.cfg.pollIntervalMs));
    // SLA 检查
    this.timers.push(setInterval(() => this.started && this.checkSlas(), 1000));
    // 失约租约巡检（执行进程假死）
    this.timers.push(setInterval(() => this.started && this.reapLeases(), Math.max(1000, this.cfg.leaseTimeoutMs / 2)));
    // 补数单状态回写
    this.timers.push(setInterval(() => this.started && this.refreshBackfills(), 2000));

    console.log(
      `[scheduler] 已启动 token=${this.token.slice(0, 8)} 并发上限=${this.cfg.maxConcurrent} db=${this.cfg.dbPath}`,
    );
    this.kick();
  }

  /**
   * 测试/演练用：模拟进程被 kill -9。
   * 停掉本机定时器与派发循环，但【不】释放领导锁、【不】改 running 实例、
   * 【不】做优雅清理——完全靠重启后的 recoverOrphans 接管。
   */
  simulateCrash() {
    this.started = false;
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    for (const w of this.waiters.splice(0)) w();
    this.running.clear();
  }

  async stop() {
    this.started = false;
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    for (const w of this.waiters.splice(0)) w();
    // 释放领导锁；正在跑的执行器随进程退出，重启后由 recoverOrphans 接管
    try {
      this.store.db
        .prepare('DELETE FROM scheduler_lock WHERE id = 1 AND owner_token = ?')
        .run(this.token);
    } catch {
      /* db 已关闭（模拟硬杀场景），锁留给接管者处理 */
    }
    this.bus.emit('stopping');
  }

  /**
   * 重启恢复：上次进程死亡时遗留在 running/queued 的实例。
   * running -> queued（清租约，等待重新派发，绝不丢也绝不重）
   * 带 next_retry_at 的失败实例保持 failed，由重试逻辑接管。
   */
  private recoverOrphans() {
    const orphans = this.store.db
      .prepare(
        `SELECT * FROM task_instance
         WHERE status IN ('running','queued')
            OR (status = 'failed' AND next_retry_at IS NOT NULL)`,
      )
      .all() as TaskInstance[];
    let requeued = 0;
    const tx = this.store.db.transaction(() => {
      for (const inst of orphans) {
        if (inst.status === 'failed') {
          // 上次进程在退避等待中死掉：到点即重试，重置 next_retry_at 让其尽快可派发
          this.store.db
            .prepare(
              `UPDATE task_instance SET status = 'queued', next_retry_at = NULL,
                      run_token = NULL, lease_heartbeat = NULL WHERE id = ?`,
            )
            .run(inst.id);
        } else {
          this.store.db
            .prepare(
              `UPDATE task_instance SET status = 'queued', attempt = 0, run_token = NULL,
                      lease_heartbeat = NULL, started_at = NULL, queued_at = ?
               WHERE id = ?`,
            )
            .run(nowIso(), inst.id);
        }
        this.store.logEvent({
          instanceId: inst.id,
          taskId: inst.task_id,
          partition: inst.partition,
          event: 'recovered_on_boot',
          detail: `原状态=${inst.status}`,
        });
        requeued++;
      }
    });
    tx();
    if (requeued) console.log(`[scheduler] 启动恢复：${requeued} 个未完成实例已重新入队`);
  }

  // ---------------- 派发 ----------------
  /** 事件驱动唤醒（实例完成/创建后立即触发，不必等轮询节拍） */
  kick() {
    if (!this.started || !this.store.isOpen) return;
    // 全量提升只周期性兜底（处理物化/重启等无法靠增量事件覆盖的场景），
    // 热路径的就绪传播由 promoteDownstreamOf 增量完成
    const now = Date.now();
    if (now - this.lastFullPromote >= 2000) {
      this.lastFullPromote = now;
      this.promotePending();
    }
    // 唤醒所有等待中的派发循环（目前只有一个，但用队列语义最稳）
    const ws = this.waiters.splice(0);
    for (const w of ws) w();
  }

  /**
   * 就绪提升：pending -> queued（批量实现，避免逐实例 N+1 查询）。
   * 同分区所有上游实例已物化且全部 success 才可入队；
   * 任一上游处于 failed/upstream_failed/killed => 本实例标记 upstream_failed；
   * 其余情况（上游还在跑/未物化）继续等。
   */
  private promotePending() {
    const pending = this.store.db
      .prepare(
        `SELECT i.id, i.task_id, i.partition FROM task_instance i
         JOIN task_def t ON t.id = i.task_id
         WHERE i.status = 'pending' AND t.enabled = 1`,
      )
      .all() as { id: number; task_id: number; partition: string }[];
    if (!pending.length) return;

    // 一次性把所有 pending 实例同分区的依赖行捞回内存做判定
    const pendingIds = pending.map((p) => p.id);
    const statusByTaskPartition = new Map<string, string>();
    const BATCH = 400;
    for (let i = 0; i < pendingIds.length; i += BATCH) {
      const chunk = pendingIds.slice(i, i + BATCH);
      const ph = chunk.map(() => '?').join(',');
      const rows = this.store.db
        .prepare(
          `SELECT up.task_id AS task_id, up.partition AS partition, up.status AS status
           FROM task_instance p
           JOIN task_dependency d ON d.to_task_id = p.task_id
           JOIN task_instance up ON up.task_id = d.from_task_id AND up.partition = p.partition
           WHERE p.id IN (${ph})`,
        )
        .all(...chunk) as { task_id: number; partition: string; status: string }[];
      for (const r of rows) statusByTaskPartition.set(`${r.task_id}@${r.partition}`, r.status);
    }

    const BLOCKED = new Set(['failed', 'upstream_failed', 'killed']);
    const toQueue: number[] = [];
    const toBlock: { id: number; task_id: number; partition: string }[] = [];
    for (const inst of pending) {
      const upstreamIds = this.store.upstreamIds(inst.task_id);
      if (!upstreamIds.length) {
        toQueue.push(inst.id);
        continue;
      }
      let ready = true;
      let blocked = false;
      for (const upId of upstreamIds) {
        const st = statusByTaskPartition.get(`${upId}@${inst.partition}`);
        if (st === undefined) {
          ready = false; // 上游实例尚未物化
          break;
        }
        if (st !== 'success') {
          ready = false;
          if (BLOCKED.has(st)) blocked = true;
        }
      }
      if (ready) toQueue.push(inst.id);
      else if (blocked) toBlock.push(inst);
    }

    // 批量回写
    const queueMany = this.store.db.prepare(
      `UPDATE task_instance SET status = 'queued', queued_at = COALESCE(queued_at, ?)
       WHERE id = ? AND status = 'pending'`,
    );
    const blockMany = this.store.db.prepare(
      `UPDATE task_instance SET status = 'upstream_failed', finished_at = ?
       WHERE id = ? AND status = 'pending'`,
    );
    const now = nowIso();
    const tx = this.store.db.transaction((items: typeof toBlock) => {
      for (const id of toQueue) queueMany.run(now, id);
      for (const b of items) {
        const info = blockMany.run(now, b.id);
        if (info.changes) {
          this.store.logEvent({
            instanceId: b.id,
            taskId: b.task_id,
            partition: b.partition,
            event: 'upstream_failed',
            detail: '就绪提升时发现上游终态失败',
          });
        }
      }
    });
    tx(toBlock);
  }

  private toQueued(instanceId: number) {
    this.store.db
      .prepare(
        `UPDATE task_instance SET status = 'queued', queued_at = COALESCE(queued_at, ?)
         WHERE id = ? AND status = 'pending'`,
      )
      .run(nowIso(), instanceId);
  }

  /**
   * 增量就绪检查：某实例成功后，只评估同分区的直接下游。
   * 热路径上每个完成事件触发一次，集合小、走索引，避免全量扫描所有 pending。
   */
  private promoteDownstreamOf(taskId: number, partition: string) {
    const downstreamIds = this.store.downstreamIds(taskId);
    if (!downstreamIds.length) return;
    const ph = downstreamIds.map(() => '?').join(',');
    const candidates = this.store.db
      .prepare(
        `SELECT id, task_id FROM task_instance
         WHERE partition = ? AND status = 'pending' AND task_id IN (${ph})`,
      )
      .all(partition, ...downstreamIds) as { id: number; task_id: number }[];
    const BLOCKED = new Set(['failed', 'upstream_failed', 'killed']);
    for (const cand of candidates) {
      const ups = this.store.upstreamIds(cand.task_id);
      const upPh = ups.map(() => '?').join(',');
      const rows = this.store.db
        .prepare(`SELECT status FROM task_instance WHERE partition = ? AND task_id IN (${upPh})`)
        .all(partition, ...ups) as { status: string }[];
      if (rows.length < ups.length) continue;
      if (rows.every((r) => r.status === 'success')) {
        this.toQueued(cand.id);
      } else if (rows.some((r) => BLOCKED.has(r.status))) {
        this.store.db
          .prepare(
            `UPDATE task_instance SET status = 'upstream_failed', finished_at = ?
             WHERE id = ? AND status = 'pending'`,
          )
          .run(nowIso(), cand.id);
      }
    }
  }

  private runningCount(): number {
    return this.running.size;
  }

  /**
   * 某补数单当前占用的执行槽：只统计 running（queued 是排队等待，不占槽）。
   * extra 为本轮调用中已经 CAS 抢占、SQL 计数尚未体现的数量。
   */
  private backfillInFlight(backfillId: number, extra: number): number {
    const row = this.store.db
      .prepare(
        `SELECT COUNT(*) c FROM task_instance
         WHERE backfill_id = ? AND status = 'running'`,
      )
      .get(backfillId) as { c: number };
    return row.c + extra;
  }

  /**
   * 找一个可派发实例并原子抢占。
   * 可派发条件：
   *   status=queued；重试退避已到点；同分区所有上游实例 success；
   *   全局/补数并发槽均有空位。
   * 排序：分区日期 -> 拓扑层（同分区严格按依赖顺序，跨分区并行）。
   */
  private claimNext(): TaskInstance | null {
    const candidates = this.store.db
      .prepare(
        `SELECT i.* FROM task_instance i
         JOIN task_def t ON t.id = i.task_id
         WHERE i.status = 'queued' AND t.enabled = 1
           AND (i.next_retry_at IS NULL OR i.next_retry_at <= ?)
         ORDER BY i.partition ASC, i.id ASC`,
      )
      .all(nowIso()) as TaskInstance[];

    // 本轮（一次 kick 触发的连续抢占）各补数单已抢占但尚未体现在 SQL 计数里的数量
    const claimedThisRound = new Map<number, number>();
    for (const inst of candidates) {
      // 全局槽位
      if (this.runningCount() >= this.cfg.maxConcurrent) return null;
      // 补数单槽位：queued+running 全量计数，再加本轮内存中刚抢的
      if (inst.backfill_id != null) {
        const bf = this.instances.getBackfill(inst.backfill_id);
        if (
          bf &&
          this.backfillInFlight(inst.backfill_id, claimedThisRound.get(inst.backfill_id) ?? 0) >=
            bf.concurrency
        ) {
          continue;
        }
      }
      // 上游就绪检查（同分区）
      const upstreamIds = this.store.upstreamIds(inst.task_id);
      if (upstreamIds.length) {
        const placeholders = upstreamIds.map(() => '?').join(',');
        const ups = this.store.db
          .prepare(
            `SELECT status FROM task_instance
             WHERE partition = ? AND task_id IN (${placeholders})`,
          )
          .all(inst.partition, ...upstreamIds) as { status: string }[];
        if (ups.length < upstreamIds.length) continue; // 上游实例尚未物化
        if (!ups.every((u) => u.status === 'success')) continue;
      }
      // CAS 抢占：只有仍是 queued 才能成功，token 一次性
      const token = randomUUID();
      const info = this.store.db
        .prepare(
          `UPDATE task_instance
           SET status = 'running', run_token = ?, lease_heartbeat = ?,
               started_at = COALESCE(started_at, ?), attempt = attempt + 1
           WHERE id = ? AND status = 'queued'
             AND (next_retry_at IS NULL OR next_retry_at <= ?)`,
        )
        .run(token, nowIso(), nowIso(), inst.id, nowIso());
      if (info.changes === 0) continue;
      if (inst.backfill_id != null) {
        claimedThisRound.set(
          inst.backfill_id,
          (claimedThisRound.get(inst.backfill_id) ?? 0) + 1,
        );
      }
      const claimed = this.store.db
        .prepare('SELECT * FROM task_instance WHERE id = ?')
        .get(inst.id) as TaskInstance;
      this.store.logEvent({
        instanceId: claimed.id,
        taskId: claimed.task_id,
        partition: claimed.partition,
        event: 'instance_start',
        detail: `attempt=${claimed.attempt} token=${token.slice(0, 8)}`,
      });
      return claimed;
    }
    return null;
  }

  /** 一次唤醒内连续抢占，直到槽满或候选耗尽，返回本轮实际派发数 */
  private drainClaims(): number {
    // 每轮最多派发 (全局上限 - 当前在跑) 个，且设硬上限防止单 tick 过长
    const budget = Math.max(0, this.cfg.maxConcurrent - this.runningCount());
    const hardCap = Math.min(budget, 256);
    let claimedCount = 0;
    for (let n = 0; n < hardCap; n++) {
      let claimed: TaskInstance | null;
      try {
        claimed = this.claimNext();
      } catch (err) {
        console.error('[scheduler] 派发异常', err);
        break;
      }
      if (!claimed) break; // 没有能立即派发的（可能是补数并发上限挡住）
      claimedCount++;
      void this.execute(claimed);
    }
    return claimedCount;
  }

  /** 派发循环：事件唤醒时跑到没有槽位或没有候选为止 */
  async dispatchLoop() {
    while (this.started) {
      const claimedCount = this.drainClaims();
      // 只有本轮确实派发了任务才可能立刻还有活；一轮零派发必须等待，
      // 否则"候选被补数并发上限挡住"会造成 CPU 空转死循环
      if (claimedCount === 0 || this.runningCount() >= this.cfg.maxConcurrent) {
        await new Promise<void>((resolve) => {
          this.waiters.push(resolve);
          setTimeout(resolve, this.cfg.pollIntervalMs * 4); // 兜底，防止唤醒丢失
        });
      }
    }
  }

  // ---------------- 执行 ----------------
  private async execute(inst: TaskInstance) {
    const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(inst.task_id) as TaskDef;
    const token = inst.run_token!;
    const executor = pickExecutor(task);
    let aborted = false;

    const heartbeat = () => {
      // 进程崩溃模拟/正在关闭：租约无意义，中止执行体，结果交由重启恢复
      if (!this.store.isOpen || !this.started) {
        aborted = true;
        throw new Error('调度器已停止/数据库已关闭，中止执行');
      }
      // 心跳：token 不匹配说明本执行已被重启恢复/人工 kill 取代，抛错中止执行体
      const cur = this.store.db
        .prepare('SELECT run_token, status FROM task_instance WHERE id = ?')
        .get(inst.id) as { run_token: string | null; status: string };
      if (cur.run_token !== token || cur.status !== 'running') {
        aborted = true;
        throw new Error('执行租约已失效（调度器重启或实例被 kill），中止本次执行');
      }
      this.store.db
        .prepare('UPDATE task_instance SET lease_heartbeat = ? WHERE id = ? AND run_token = ?')
        .run(nowIso(), inst.id, token);
    };

    const handle: RunningHandle = {
      instanceId: inst.id,
      token,
      abort: () => {
        aborted = true;
      },
    };
    this.running.set(inst.id, handle);
    this.bus.emit('change');

    try {
      const result = await executor.run({ task, instance: inst, token }, heartbeat);
      if (aborted || !this.store.isOpen) return; // 崩溃进程的迟到结果一律作废
      this.finish(inst, task, token, result.ok, result.message);
    } catch (err) {
      if (aborted || !this.store.isOpen) return;
      this.finish(inst, task, token, false, (err as Error).message);
    } finally {
      this.running.delete(inst.id);
      this.bus.emit('change');
      this.kick();
    }
  }

  /** 执行收尾：CAS 回写，成功/重试/终态失败三分支 */
  private finish(
    inst: TaskInstance,
    task: TaskDef,
    token: string,
    ok: boolean,
    message: string,
  ) {
    const cur = this.store.db
      .prepare('SELECT * FROM task_instance WHERE id = ?')
      .get(inst.id) as TaskInstance;
    if (cur.run_token !== token || cur.status !== 'running') {
      // 迟到回写：执行体在租约失效后才结束，结果作废
      this.store.logEvent({
        instanceId: inst.id,
        event: 'stale_write_rejected',
        detail: `token=${token.slice(0, 8)}`,
      });
      return;
    }

    if (ok) {
      const finishedAt = nowIso();
      const duration = cur.started_at
        ? Date.now() - new Date(cur.started_at).getTime()
        : null;
      this.store.db
        .prepare(
          `UPDATE task_instance SET status = 'success', finished_at = ?, duration_ms = ?,
                  error_message = NULL, run_token = NULL, lease_heartbeat = NULL
           WHERE id = ? AND run_token = ?`,
        )
        .run(finishedAt, duration, inst.id, token);
      this.store.logEvent({ instanceId: inst.id, taskId: task.id, partition: inst.partition, event: 'instance_success', detail: message });
      // 增量传播：只唤醒同分区直接下游，避免全量扫描
      this.promoteDownstreamOf(task.id, inst.partition);
      if (inst.backfill_id != null) this.instances.refreshBackfillStatus(inst.backfill_id);
      this.bus.emit('change');
      this.kick();
      return;
    }

    // 失败：还有重试额度 => 退避后回到 queued；否则终态 failed + 告警 + 级联
    if (cur.attempt <= task.retry_max) {
      const backoff = task.retry_delay_ms * 2 ** (cur.attempt - 1); // 指数退避
      const retryAt = new Date(Date.now() + backoff).toISOString();
      this.store.db
        .prepare(
          `UPDATE task_instance SET status = 'queued', next_retry_at = ?, run_token = NULL,
                  lease_heartbeat = NULL, error_message = ?
           WHERE id = ? AND run_token = ?`,
        )
        .run(retryAt, message, inst.id, token);
      this.store.logEvent({
        instanceId: inst.id,
        taskId: task.id,
        partition: inst.partition,
        event: 'instance_retry_scheduled',
        detail: `attempt=${cur.attempt} 退避 ${backoff}ms: ${message}`,
      });
    } else {
      const finishedAt = nowIso();
      const duration = cur.started_at ? Date.now() - new Date(cur.started_at).getTime() : null;
      this.store.db
        .prepare(
          `UPDATE task_instance SET status = 'failed', finished_at = ?, duration_ms = ?,
                  run_token = NULL, lease_heartbeat = NULL, error_message = ?
           WHERE id = ? AND run_token = ?`,
        )
        .run(finishedAt, duration, message, inst.id, token);
      this.store.logEvent({
        instanceId: inst.id,
        taskId: task.id,
        partition: inst.partition,
        event: 'instance_failed',
        detail: message,
      });
      const fresh = this.store.db.prepare('SELECT * FROM task_instance WHERE id = ?').get(inst.id) as TaskInstance;
      this.alerts.notifyRetryExhausted(task, fresh);
      this.instances.cascadeUpstreamFailed(fresh, task.name);
    }
    // 实例到终态后立即回写所属补数单状态（成功/失败两路都覆盖）
    if (inst.backfill_id != null) this.instances.refreshBackfillStatus(inst.backfill_id);
    this.bus.emit('change');
    this.kick();
  }

  // ---------------- 租约巡检 ----------------
  private reapLeases() {
    const deadline = new Date(Date.now() - this.cfg.leaseTimeoutMs).toISOString();
    const dead = this.store.db
      .prepare(
        `SELECT * FROM task_instance
         WHERE status = 'running' AND lease_heartbeat < ?`,
      )
      .all(deadline) as TaskInstance[];
    for (const inst of dead) {
      // 本机内存里还在跑的不算失约（可能只是心跳 SQL 慢）
      if (this.running.has(inst.id)) continue;
      const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(inst.task_id) as TaskDef;
      if (inst.attempt <= task.retry_max) {
        this.store.db
          .prepare(
            `UPDATE task_instance SET status = 'queued', run_token = NULL, lease_heartbeat = NULL,
                    error_message = '执行租约失约，重新调度' WHERE id = ?`,
          )
          .run(inst.id);
        this.store.logEvent({ instanceId: inst.id, event: 'lease_reaped_requeue' });
      } else {
        this.store.db
          .prepare(
            `UPDATE task_instance SET status = 'failed', run_token = NULL, finished_at = ?,
                    error_message = '执行租约失约且重试耗尽' WHERE id = ?`,
          )
          .run(nowIso(), inst.id);
        this.alerts.notifyRetryExhausted(task, { ...inst, status: 'failed' });
      }
    }
  }

  // ---------------- SLA ----------------
  /**
   * 两条 SLA 规则（任务可只配其一）：
   *  A. sla_deadline 'HH:MM'：当天该时刻仍未 success —— 提前预警 + 到点违约
   *  B. sla_duration_ms：排队+运行总时长预算 —— 达到 warnRatio 预警，超预算违约
   *     留空时用最近成功实例的平均时长。
   */
  private checkSlas() {
    const now = Date.now();
    const active = this.store.db
      .prepare(
        `SELECT i.* FROM task_instance i JOIN task_def t ON t.id = i.task_id
         WHERE i.status IN ('pending','queued','running')`,
      )
      .all() as (TaskInstance & { sla_deadline: string | null; sla_duration_ms: number | null; sla_warn_ratio: number; name: string; owner: string })[];

    for (const inst of active) {
      const task = this.store.db.prepare('SELECT * FROM task_def WHERE id = ?').get(inst.task_id) as TaskDef;
      const ageMs = now - new Date(inst.queued_at ?? inst.created_at).getTime();

      // 规则 B：时长预算
      const budget = task.sla_duration_ms ?? this.instances.avgSuccessDuration(task.id);
      if (budget != null) {
        if (!inst.sla_breached && ageMs > budget) {
          this.alerts.notifySlaBreach(task, inst, `已耗时 ${ageMs}ms 超过预算 ${budget}ms`);
        } else if (!inst.sla_warned && ageMs > budget * task.sla_warn_ratio) {
          const eta = ageMs >= budget ? ageMs : budget;
          this.alerts.notifySlaWarn(
            task,
            inst,
            `已耗时 ${ageMs}ms，达到预算 ${budget}ms 的 ${Math.round((ageMs / budget) * 100)}%（预计 ${eta}ms）`,
          );
        }
      }

      // 规则 A：当日截止时刻
      if (inst.sla_deadline_at) {
        const deadlineTs = new Date(inst.sla_deadline_at).getTime();
        const remain = deadlineTs - now;
        if (!inst.sla_breached && remain <= 0) {
          this.alerts.notifySlaBreach(task, inst, `已过截止时刻 ${task.sla_deadline} 仍未成功（当前 ${inst.status}）`);
        } else if (!inst.sla_warned && remain > 0 && remain <= Math.max(60_000, (task.sla_duration_ms ?? 300_000) * 0.2)) {
          this.alerts.notifySlaWarn(task, inst, `距截止 ${task.sla_deadline} 仅剩 ${Math.round(remain / 1000)}s`);
        }
      }
    }
  }

  // ---------------- 补数回写 ----------------
  private refreshBackfills() {
    const ids = this.store.db
      .prepare("SELECT id FROM backfill WHERE status = 'running'")
      .all() as { id: number }[];
    for (const { id } of ids) this.instances.refreshBackfillStatus(id);
  }

  // ---------------- 查询接口（HTTP 层用） ----------------
  listInstances(filter: { partition?: string; taskId?: number; status?: string; limit?: number }) {
    const where: string[] = [];
    const params: unknown[] = [];
    if (filter.partition) {
      where.push('i.partition = ?');
      params.push(filter.partition);
    }
    if (filter.taskId) {
      where.push('i.task_id = ?');
      params.push(filter.taskId);
    }
    if (filter.status) {
      where.push('i.status = ?');
      params.push(filter.status);
    }
    const limit = Math.min(filter.limit ?? 200, 1000);
    return this.store.db
      .prepare(
        `SELECT i.*, t.name AS task_name, t.owner AS owner FROM task_instance i
         JOIN task_def t ON t.id = i.task_id
         ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
         ORDER BY i.id DESC LIMIT ?`,
      )
      .all(...params, limit);
  }

  getInstance(id: number) {
    return this.store.db
      .prepare(
        `SELECT i.*, t.name AS task_name FROM task_instance i
         JOIN task_def t ON t.id = i.task_id WHERE i.id = ?`,
      )
      .get(id);
  }

  events(instanceId?: number, limit = 200) {
    if (instanceId) {
      return this.store.db
        .prepare('SELECT * FROM run_event WHERE instance_id = ? ORDER BY id DESC LIMIT ?')
        .all(instanceId, limit);
    }
    return this.store.db.prepare('SELECT * FROM run_event ORDER BY id DESC LIMIT ?').all(limit);
  }

  /** 依赖图 + 某分区（默认今天）每节点状态/耗时，给 DAG 界面 */
  graphWithStatus(partition: string) {
    const tasks = this.store.allTasks();
    const layers = this.store.layers();
    const nameById = new Map(tasks.map((t) => [t.id as number, t.name as string]));
    const instRows = this.store.db
      .prepare(
        `SELECT task_id, status, duration_ms, attempt FROM task_instance WHERE partition = ?`,
      )
      .all(partition) as { task_id: number; status: string; duration_ms: number | null; attempt: number }[];
    const instMap = new Map(instRows.map((r) => [r.task_id, r]));
    const nodes = tasks
      .map((t) => {
        const r = instMap.get(t.id as number);
        return {
          id: t.id,
          name: t.name,
          owner: t.owner,
          layer: layers.get(t.id as number) ?? 0,
          enabled: t.enabled,
          todayStatus: r?.status ?? null,
          todayDurationMs: r?.duration_ms ?? null,
          todayAttempt: r?.attempt ?? null,
        };
      })
      .sort((a, b) => a.layer - b.layer || String(a.name).localeCompare(String(b.name)));
    const edges = this.store.allEdges().map((e) => ({
      from_task: nameById.get(e.from_task_id),
      to_task: nameById.get(e.to_task_id),
    }));
    return { partition, nodes, edges };
  }

  runningSnapshot() {
    return [...this.running.values()].map((h) => ({ instanceId: h.instanceId, token: h.token.slice(0, 8) }));
  }

  // ---------------- SSE ----------------
  attachSse(res: Response) {
    res.setHeader('content-type', 'text/event-stream');
    res.setHeader('cache-control', 'no-cache');
    res.setHeader('connection', 'keep-alive');
    res.flushHeaders?.();
    res.write(`event: hello\ndata: {"token":"${this.token.slice(8)}"}\n\n`);
    let timer: NodeJS.Timeout | undefined;
    const onChange = () => res.write(`event: change\ndata: ${JSON.stringify({ ts: nowIso() })}\n\n`);
    this.bus.on('change', onChange);
    timer = setInterval(() => res.write(': ping\n\n'), 15000);
    res.on('close', () => {
      this.bus.off('change', onChange);
      if (timer) clearInterval(timer);
    });
  }

  /** 等待所有活动实例到终态；可选只等某个补数单 */
  async waitUntilDrain(timeoutMs: number, backfillId?: number): Promise<boolean> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const row = backfillId == null
        ? (this.store.db
            .prepare(`SELECT COUNT(*) c FROM task_instance WHERE status IN ('pending','queued','running')`)
            .get() as { c: number })
        : (this.store.db
            .prepare(`SELECT COUNT(*) c FROM task_instance WHERE backfill_id = ? AND status IN ('pending','queued','running')`)
            .get(backfillId) as { c: number });
      if (row.c === 0) return true;
      this.kick();
      await sleep(200);
    }
    return false;
  }
}

