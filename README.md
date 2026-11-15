# Flowline —— 数仓 DAG 分区任务调度系统

TypeScript / Node.js 实现，替代 crontab 的数仓任务调度：DAG 依赖、按日期分区、
失败重试、补数、一键重跑下游、SLA 告警、重启不丢状态不重复拉起，带 Web 管理界面。

## 需求与实现对照

| # | 需求 | 实现 |
|---|------|------|
| 1 | 任务按依赖成 DAG，按日期分区调度，上游分区好下游才动 | `task_def` + `task_dependency`，Kahn 拓扑排序 + 环检测；实例以 `(task_id, partition)` 唯一，就绪提升要求同分区所有上游 `success` |
| 2 | 失败按策略重试，超限通知负责人 | `retry_max` + 指数退避（`retry_delay_ms * 2^(attempt-1)`），耗尽写 critical 告警（落库 + 控制台 + 可选 Webhook），并级联标记下游 `upstream_failed` |
| 3 | 补数选日期段，依赖顺序并行，并发有上限 | `backfill` 单据；同分区按拓扑序入队、跨分区并行；每个补数单独立 `concurrency` 上限，另有全局并发上限 |
| 4 | 改逻辑后一键重跑该任务及所有下游指定分区 | `POST /api/rerun`：递归收集全下游，多分区批量重置为 pending，历史保留在 `run_event` |
| 5 | 每任务 SLA，提前告警 | 双规则：截止时刻 `sla_deadline`（临近预警/超时违约）+ 时长预算 `sla_duration_ms`（按 `sla_warn_ratio` 提前预警，缺省取历史均值） |
| 6 | 调度服务重启，实例状态不丢、不重复拉起 | SQLite WAL 全状态落盘 + `scheduler_lock` 领导租约 + 启动孤儿恢复 + 一次性 `run_token` CAS 抢占，迟到回写按 token 拒绝 |
| 7 | 依赖图看每节点当天状态和耗时 | `/api/graph?date=` 返回分层节点 + 当日状态/耗时/尝试次数，前端分层渲染 |

## 核心架构

```
src/
  types.ts       领域模型与状态机
  config.ts      环境变量配置、日期工具
  db.ts          SQLite 持久层（迁移、拓扑排序、环检测、邻接缓存）
  dag.ts         任务/依赖增删改，所有变更强制环检测
  instance.ts    实例物化、补数单、一键重跑、上游失败级联
  executor.ts    MockExecutor（零进程压测）/ SubprocessExecutor（真实脚本+超时+心跳）
  alerts.ts      告警去重、落库、Webhook
  scheduler.ts   调度内核：领导锁、孤儿恢复、就绪提升、CAS 派发、重试、SLA、租约巡检
  api.ts         Express REST API + SSE + 静态托管
  synthetic.ts   合成 DAG 与假执行脚本生成器
scripts/
  benchmark.ts    200 任务 × 30 天补数验收（依赖顺序 + 关键路径 1.2 倍）
  test-recovery.ts 两次硬杀的重启恢复验收
  jobs/           gen-dag 生成的假脚本（.cjs）
ui/               React + Vite 管理界面（依赖图/任务/实例/补数/告警五个页面）
```

### 实例状态机

```
pending ──上游全部success──▶ queued ──CAS抢占──▶ running ──成功──▶ success
   │                           ▲    │                │
   │上游终态失败               │重试退避             失败
   ▼                           └──────────────┬──────┘
upstream_failed                     重试耗尽 ▼
                                         failed
(killed 为人工终止，任意非终态可进入)
```

### 不重复拉起的三层保证

1. **领导锁** `scheduler_lock`：同一时刻只有一个调度器持有租约，心跳续租，
   崩溃后立即重启需显式 `--takeover`（或等租约超时自动接管）。
2. **启动恢复** `recoverOrphans`：遗留的 `running/queued` 原子打回 `queued`，清旧 token。
3. **CAS + 一次性 token**：`queued→running` 用条件 UPDATE 抢占并写入新 `run_token`，
   执行器每次心跳、完成回写都校验 token；旧进程的迟到结果在 `finish` 入口被拒绝
   （`stale_write_rejected` 事件）。

## 快速开始

```bash
npm install
npm run gen-dag                         # 造 200 任务 DAG + 假脚本（默认 data/flowline.db）
npm start                               # 启动调度器 + 管理界面 http://localhost:7100
```

常用 CLI：

```bash
npm run cli -- gen-dag --tasks 200 --layers 8 --fail-demo true
npm run cli -- backfill --from 2026-10-17 --to 2026-11-15 --concurrency 16
npm run cli -- rerun --task l2_task_013 2026-11-10 2026-11-11
npm run cli -- instances --date 2026-11-15
npm run cli -- alerts --unacked true
npm run start -- serve --takeover       # 崩溃后立即重启时强制接管
```

环境变量：`FLOWLINE_DB`、`FLOWLINE_PORT`、`FLOWLINE_CONCURRENCY`、
`FLOWLINE_POLL_MS`、`FLOWLINE_LEASE_MS`、`FLOWLINE_WEBHOOK_URL`、`FLOWLINE_TAKEOVER=1`。

## 验收测试

```bash
npm run test:all
```

- **重启恢复** `test-recovery.ts`：4 任务小 DAG × 3 分区，运行中两次模拟 kill -9
  （保留锁与 running 状态），第三次启动接管跑完。断言：12 实例全成功、无重复成功事件、
  失败任务 attempt 延续到 3、并发调度器被领导锁拒绝、恢复审计事件齐全。
- **补数压测** `benchmark.ts`：200 任务 / 414 边 / 8 层，30 分区共 6000 实例。断言：
  实例无丢失、`attempt` 全为 1（无重复拉起）、12420 条依赖边零违反
  （子任务 `started_at` ≥ 每个上游 `finished_at`）、总墙钟 ≤ 单分区关键路径 × 1.2。

最近一次实测结果：墙钟/关键路径 = **1.081**，平均并行度约 600，吞吐约 80–110 实例/秒。

## 管理界面

```bash
cd ui && npm install && npm run build   # 产物 ui/dist 由后端直接托管
npm run dev                             # 开发模式（:7200，代理 /api 到 :7100）
```

五个页面：**依赖图**（分层着色、节点状态/耗时、点击重跑）、**任务定义**（增删改、
依赖边、环拒绝）、**运行实例**（筛选、事件时间线、kill）、**补数管理**（建单、
进度条、一键重跑）、**告警中心**（SLA/失败告警、确认）。列表通过 SSE 实时刷新。

## 关键设计取舍

- **SQLite 而非外部 DB**：better-sqlite3 同步 API 让状态迁移天然落在事务里，WAL +
  `synchronous=FULL` 保证崩溃一致；单库文件即可演示，规模到数万实例无压力。
- **就绪传播增量 + 全量兜底**：实例成功只检查直接下游（走索引的小集合），
  全量 pending 提升降为 2 秒兜底，避免每轮扫描全表。
- **mock 与 subprocess 双执行器**：压测用进程内 mock 消除进程噪音；真实任务走
  子进程，带超时 kill 与租约心跳，脚本路径支持 `${partition}` 等占位符。
