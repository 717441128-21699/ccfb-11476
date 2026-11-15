/**
 * 全局类型定义
 */

/** 实例生命周期状态 */
export type InstanceStatus =
  | 'pending'      // 已创建，等待调度（上游可能未完成）
  | 'queued'       // 上游全部成功，等待执行槽
  | 'running'      // 正在执行
  | 'success'      // 成功
  | 'failed'       // 重试耗尽后的最终失败
  | 'upstream_failed' // 上游失败导致本实例不可能再跑
  | 'killed';      // 人工终止

/** 任务定义（DAG 节点） */
export interface TaskDef {
  id: number;
  name: string;                       // 唯一任务名
  owner: string;                      // 负责人
  description: string;
  schedule: string;                   // 目前支持 'daily'
  executor_type: 'mock' | 'subprocess';
  executor_config: string;            // JSON: { script, args, mockDurationMs, failRate }
  retry_max: number;                  // 最大重试次数（不含首次）
  retry_delay_ms: number;             // 重试退避基数
  timeout_ms: number;                 // 单次执行超时
  sla_deadline: string | null;        // 当日截止时刻 'HH:MM'，晚于它仍未成功即告警
  sla_duration_ms: number | null;     // 预计时长，实际排队+运行超过它即告警（留空则用历史均值）
  sla_warn_ratio: number;             // 提前告警比例，如 0.8 = 预计将超 SLA 的 80% 阈值时预警
  enabled: number;                    // 0/1
  created_at: string;
  updated_at: string;
}

/** 任务实例：某任务 × 某分区(数据日期) 的一次运行 */
export interface TaskInstance {
  id: number;
  task_id: number;
  partition: string;                  // 'YYYY-MM-DD'
  status: InstanceStatus;
  attempt: number;                    // 已尝试次数（首次=1）
  run_token: string | null;           // 当前执行租约令牌，重启后旧令牌失效
  lease_heartbeat: string | null;     // 租约最近心跳时间
  trigger_type: 'scheduled' | 'backfill' | 'manual' | 'rerun';
  backfill_id: number | null;
  queued_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;         // started -> finished
  next_retry_at: string | null;
  error_message: string | null;
  sla_deadline_at: string | null;     // 该分区 SLA 绝对截止时刻
  sla_warned: number;                 // 0/1 预警是否已发
  sla_breached: number;               // 0/1 违约是否已发
  created_at: string;
}

/** 补数任务单 */
export interface Backfill {
  id: number;
  name: string;
  date_from: string;
  date_to: string;
  task_name: string | null;           // null 表示全 DAG；否则只跑该任务及其下游
  include_downstream: number;         // 与 task_name 配合
  concurrency: number;                // 本补数单的并发上限
  status: 'created' | 'running' | 'success' | 'failed' | 'killed';
  created_by: string;
  created_at: string;
  finished_at: string | null;
}

/** 告警 */
export interface Alert {
  id: number;
  task_id: number | null;
  instance_id: number | null;
  partition: string | null;
  level: 'warn' | 'critical';
  channel: string;                    // log / webhook
  type: 'retry_exhausted' | 'sla_warn' | 'sla_breach' | 'upstream_failed';
  message: string;
  acknowledged: number;
  created_at: string;
}

/** 运行事件（时间线 / 审计） */
export interface RunEvent {
  id: number;
  instance_id: number | null;
  task_id: number | null;
  partition: string | null;
  event: string;
  detail: string | null;
  created_at: string;
}

/** 调度器运行时配置 */
export interface SchedulerConfig {
  dbPath: string;
  maxConcurrent: number;              // 全局并发上限
  pollIntervalMs: number;             // 兜底轮询节拍
  leaseTimeoutMs: number;             // 租约超时 => 判定执行进程已死
  alertWebhookUrl: string | null;
  historyLookbackDays: number;        // 自动物化多少天的 scheduled 实例
}

/** API 返回的 DAG 节点（带当日状态） */
export interface DagNode {
  id: number;
  name: string;
  owner: string;
  layer: number;
  todayStatus: InstanceStatus | null;
  todayDurationMs: number | null;
  todayAttempt: number | null;
}

export interface DagEdge {
  from_task: string;
  to_task: string;
}

/** 执行器统一接口的输入输出 */
export interface ExecInput {
  task: TaskDef;
  instance: TaskInstance;
  token: string;
}

export interface ExecResult {
  ok: boolean;
  message: string;
  startedAt: string;
  finishedAt: string;
}
