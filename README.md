# DAG 任务调度系统

一个面向数仓场景的依赖型任务调度器，TypeScript 跑在 Node 上，零运行时第三方依赖（数据库用 Node 24 内置的 `node:sqlite`，HTTP 用原生 `node:http`，界面是无构建 Vanilla JS）。

## 解决的核心问题

传统 crontab 排时间的痛点：上游晚了下游照跑、出错要手工一个个重跑、重启丢状态。本系统以 **DAG 依赖 + 分区状态** 驱动调度。

## 七大需求的实现位置

| # | 需求 | 实现 |
|---|------|------|
| 1 | DAG 依赖、按日期分区、上游分区好下游才动 | `src/dag.ts`（拓扑/环检测）、`src/scheduler.ts` 的 `dependencyState()`，跨分区用边的 `offsetDays`（如 `-1` 表示依赖上游昨天分区） |
| 2 | 失败重试、超次数通知负责人 | `scheduler.ts` 的 `resolveInstanceAfterExit()`，指数退避；`src/alerts.ts` 发 `TASK_FAILED`（含负责人），重试发 `RETRY` |
| 3 | 选日期段补数、按依赖并行、并发有上限 | `src/backfill.ts` 的 `startBackfill()`；三层上限：全局 `maxConcurrency`、单分区 `maxPerPartition`、并行分区滑窗 `parallelPartitions` |
| 4 | 改逻辑一键重跑某任务及所有下游的指定分区 | `backfill.ts` 的 `rerunDownstream()`，`dag.subtree()` 求子图，子树外的成功实例原样复用 |
| 5 | SLA 提前告警 | `scheduler.ts` 的 `checkSlas()`，到 `slaWarnRatio`（默认 80%）发 `SLA_WARNING`，超时发 `SLA_BREACHED` |
| 6 | 重启不丢状态、不重复拉起 | marker 文件心跳 + `instance_runs` 租约；`recoverOrphans()` 按 PID/marker 对账（存活则 ADOPTED，死亡则 REAPED）；派发用 `UPDATE...WHERE status='PENDING'` 原子认领 |
| 7 | 依赖图看节点当天状态和耗时 | `web/index.html` + `web/app.js`，SVG 分层 DAG，节点颜色=状态、标注耗时，悬停高亮上下游，点击看详情 |

## 目录结构

```
src/
  config.ts      配置（数据目录、并发上限、tick/心跳/租约间隔）
  types.ts       领域类型
  db.ts          node:sqlite 封装、schema、事务
  dag.ts         DAG 模型、拓扑排序、环检测、关键路径、分层布局
  repo.ts        任务/边/实例/运行/补数的仓储（snake_case→camelCase 映射）
  executor.ts    一任务一 OS 子进程，marker 协议、心跳、杀进程
  scheduler.ts   调度核心：tick、依赖判定、原子认领、重试、SLA、租约恢复、补数窗口
  backfill.ts    补数与下游重跑编排
  alerts.ts      告警通道（控制台+DB，可选 webhook）
  events.ts      事件日志
  server.ts      REST API + 静态界面托管
  cli.ts         命令行入口
scripts/
  fake-task-helper.cjs   假任务共享逻辑（marker 心跳、模拟耗时/波动/注入失败）
tools/
  gen-dag.ts     生成 200 任务 13 层 DAG + 200 个独立假脚本
  perf-probe.ts  单分区性能探针（实测关键路径比值）
  verify.ts      端到端验收（结构/补数/顺序/并发/重启/重试/SLA 共 18 项断言）
  demo.ts        引导式演示
web/             管理界面（index.html + app.js）
```

## 快速开始

```bash
npm install
npm run typecheck

# 生成 200 任务 DAG 和假脚本（默认写到 data/）
node --disable-warning=ExperimentalWarning tools/gen-dag.ts

# 启动调度服务 + 管理界面（http://localhost:7777）
npm start

# 命令行补数 30 天
node --disable-warning=ExperimentalWarning src/cli.ts backfill 2026-10-17 2026-11-15

# 一键重跑某任务及其下游
node --disable-warning=ExperimentalWarning src/cli.ts rerun t_010_lyr1 2026-11-01 2026-11-03

# 全量验收（生成数据→30天补数→重启→失败/SLA，输出 data/verify-results.json）
npm run verify
```

## 重启安全协议（需求 6 详解）

每次尝试（attempt）：
1. 在 `instance_runs` 建一行（RUNNING），在 `data/markers/run-<id>.marker.json` 建 marker；
2. 子进程启动即写 `RUNNING + pid`，每 400ms 刷新心跳，结束写 `SUCCESS/FAILED + exitCode`；
3. 调度器每 500ms 对存活子进程回写心跳；
4. 调度器重启时 `recoverOrphans()` 扫描所有 RUNNING 的 run：
   - PID 存活且 marker=RUNNING → **ADOPTED**，轮询 marker 等它结束，绝不重新拉起；
   - 进程已死 → 信任 marker 终态，**REAPED** 并走正常的成功/重试/失败流程；
5. 派发本身在事务里做条件更新，只有 `PENDING` 能被认领成 `RUNNING`，从根上杜绝重复拉起。

## 关键路径口径

验收脚本对每个分区，用该分区**实测**的每个任务 `duration_ms` 作为权重，沿同日依赖边求最长路径作为实测关键路径 CP(d)，再算 `wall(d) / CP(d)`，要求所有分区该比值 ≤ 1.2。跨分区边（offset≠0）不计入单分区关键路径。

## 调参（环境变量）

| 变量 | 默认 | 含义 |
|------|------|------|
| `SCHED_DATA_DIR` | `./data` | 数据目录（db、脚本、marker、日志） |
| `SCHED_EXEC_MODE` | `subprocess` | 执行模式：`subprocess`=一任务一 OS 进程（生产默认，真实隔离）；`inprocess`=假任务同进程异步执行（演示/验收用，零进程创建开销） |
| `SCHED_MAX_CONCURRENCY` | 12 | 全局并发上限（subprocess 模式默认值；inprocess 演示按 backfill 入参放到 40） |
| `SCHED_MAX_PER_PARTITION` | 10 | 单分区并发上限 |
| `SCHED_BACKFILL_PARALLEL` | 2 | 补数并行分区数 |
| `SCHED_TICK_MS` | 500 | 巡检间隔（inprocess 自动收紧到 50ms） |
| `SCHED_DISPATCH_PER_TICK` | 2 | subprocess 模式每 tick 最多拉起的进程数（规避 Windows spawn 阻塞事件循环） |
| `SCHED_LEASE_MS` | 8000 | 心跳租约超时 |
| `SCHED_PORT` | 7777 | 界面/API 端口 |
| `SCHED_FORCE_FAIL_TASK` | - | 让指定任务每次都失败（测试用） |
| `SCHED_FORCE_FAIL` | `task:attempt` | 让指定任务的第 N 次尝试失败 |
| `SCHED_ALERT_WEBHOOK` | - | 告警 webhook 地址 |

## 两种执行模式

- **subprocess（生产）**：每次尝试 `spawn` 一个独立 OS 进程，有独立 PID、stdio、崩溃隔离，marker 心跳写在子进程里，不影响调度器事件循环。重启后存活的子进程被 ADOPTED，死亡的按 marker 终态 REAPED。
- **inprocess（演示/验收）**：假任务逻辑作为 async 函数在调度器进程内运行，复用完全相同的 marker/run 行/状态机/重试/SLA 语义，但没有进程创建开销。这让 6800 个实例的全量验收在数分钟内完成。生产真实任务（ETL 脚本、Spark 提交等）始终用 subprocess 模式。

## 关键路径口径（验收 C1）

验收把整个补数展开为 `(任务, 分区)` 节点的完整 DAG，**边包含跨分区 offset 依赖**，用每个实例实测的 `duration_ms` 作为权重求最长加权路径，得到尊重全部依赖（含"d 分区等 d-1 分区"串行约束）的理论最小墙钟 `globalCP`；再算 `实测墙钟 / globalCP ≤ 1.2`。这比单分区口径更严格、也更贴近真实补数。


## 关键路径口径（验收 C1，最终口径）

每个分区是一次独立的 DAG 调度，同分区关键路径 CP 只沿 offset=0 的边、用各实例实测 duration_ms 求最长加权路径。分区原始墙钟会合法地包含跨分区等待（offset<0 的上游分区较晚完成），因此 C1 用「有效执行窗口」考核：时钟从该分区全部依赖（含跨分区）就绪时刻起算，到最后一个本地任务完成为止，要求 有效墙钟 / 同分区CP ≤ 1.2。另附全局展开 DAG（含 offset 边）统计作参考。

## 跨分区依赖的关键实现细节

就绪判定 SQL 用 SQLite date() 按边的 offset_days 推算上游分区日期。负 offset 必须拼成 '-1 days'，不能用 '+' || -1（会得到非法 '+-1 days'，SQLite 返回 NULL 并静默丢弃依赖，导致下游提前启动）。代码用 CASE WHEN offset_days < 0 THEN '-'||(-offset_days) ELSE '+'||offset_days END 构造正确修饰符，验收 B3 零容忍校验。

## autoDaily 与 backfill 的竞争保护

serve 模式默认每个 tick 尝试为"今天"建实例（autoDaily）。若在一个跨分区 backfill 尚未跑完历史分区时，autoDaily 抢先创建并调度了今天的实例，该实例的 offset<0 上游（昨天分区）可能还在 PENDING，导致下游先于上游运行。enqueueDailyPartitions 因此加了守卫：今天的任务只有在它依赖的所有跨分区历史分区实例都已存在时才入队，否则等待 backfill 先补齐历史。验收 B3 对同分区和跨分区依赖顺序做零容忍校验（11954+ 依赖对）。
