import { spawn } from 'node:child_process';
import type { ExecInput, ExecResult, TaskDef } from './types.js';
import { nowIso } from './config.js';

export interface Executor {
  /**
   * 执行一次任务。onHeartbeat 周期性回调，调度器据此续租；
   * 若回调抛出（租约被重启后的新调度器夺走），执行应中止。
   */
  run(input: ExecInput, onHeartbeat: () => void): Promise<ExecResult>;
}

interface MockConfig {
  mockDurationMs?: number;
  jitterRatio?: number;
  /** 前 failTimes 次尝试失败，之后成功（用于演示重试） */
  failTimes?: number;
  failRate?: number;
}

/** 进程内模拟执行器：压测时零进程开销，可支撑上千并发 */
export class MockExecutor implements Executor {
  async run(input: ExecInput, onHeartbeat: () => void): Promise<ExecResult> {
    const cfg = JSON.parse(input.task.executor_config || '{}') as MockConfig;
    const base = Number(cfg.mockDurationMs ?? 200);
    const jitter = Number(cfg.jitterRatio ?? 0.1);
    // 确定性的抖动：用任务 id + 分区 + 尝试次数派生，避免随机数导致压测不可复现
    const seed =
      (input.task.id * 73856093) ^
      (hashStr(input.instance.partition) * 19349663) ^
      (input.instance.attempt * 83492791);
    const ratio = 1 + jitter * (Math.abs(seed) % 100) / 50 - jitter; // [1-j, 1+j]
    const duration = Math.max(5, Math.round(base * ratio));

    const failTimes = Number(cfg.failTimes ?? 0);
    const failRate = Number(cfg.failRate ?? 0);
    const willFail =
      input.instance.attempt <= failTimes ||
      (failRate > 0 && Math.abs(seed) % 1000 < failRate * 1000);

    const startedAt = nowIso();
    await interruptibleSleep(duration, onHeartbeat, input.task.timeout_ms);
    const finishedAt = nowIso();
    if (willFail) {
      return {
        ok: false,
        message: `mock 失败（attempt=${input.instance.attempt}，配置 failTimes=${failTimes} failRate=${failRate}）`,
        startedAt,
        finishedAt,
      };
    }
    return { ok: true, message: `mock 成功，模拟耗时 ${duration}ms`, startedAt, finishedAt };
  }
}

/** 子进程执行器：跑真实脚本（scripts/jobs/*.js），带超时与心跳 */
export class SubprocessExecutor implements Executor {
  run(input: ExecInput, onHeartbeat: () => Promise<void> | void): Promise<ExecResult> {
    const cfg = JSON.parse(input.task.executor_config || '{}') as {
      command?: string;
      args?: string[];
      cwd?: string;
    };
    const command = cfg.command ?? 'node';
    const args = (cfg.args ?? []).map((a) =>
      a
        .replaceAll('${partition}', input.instance.partition)
        .replaceAll('${task}', input.task.name)
        .replaceAll('${attempt}', String(input.instance.attempt)),
    );
    const startedAt = nowIso();
    return new Promise((resolve) => {
      const child = spawn(command, args, {
        cwd: cfg.cwd ?? process.cwd(),
        env: {
          ...process.env,
          FLOWLINE_TASK: input.task.name,
          FLOWLINE_PARTITION: input.instance.partition,
          FLOWLINE_ATTEMPT: String(input.instance.attempt),
        },
      });
      let stdout = '';
      let stderr = '';
      let timedOut = false;
      child.stdout.on('data', (d) => (stdout += d.toString()));
      child.stderr.on('data', (d) => (stderr += d.toString()));

      const beat = setInterval(() => {
        try {
          onHeartbeat();
        } catch {
          timedOut = true;
          child.kill();
        }
      }, 2000);
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill();
      }, input.task.timeout_ms);

      child.on('close', (code) => {
        clearInterval(beat);
        clearTimeout(timer);
        const finishedAt = nowIso();
        if (timedOut && code !== 0) {
          resolve({
            ok: false,
            message: `执行超时（${input.task.timeout_ms}ms）已 kill。stderr: ${stderr.slice(-500)}`,
            startedAt,
            finishedAt,
          });
        } else if (code === 0) {
          resolve({ ok: true, message: stdout.trim().slice(-1000) || 'exit 0', startedAt, finishedAt });
        } else {
          resolve({
            ok: false,
            message: `exit=${code}。stderr: ${stderr.slice(-500)}`,
            startedAt,
            finishedAt,
          });
        }
      });
      child.on('error', (err) => {
        clearInterval(beat);
        clearTimeout(timer);
        resolve({ ok: false, message: `进程拉起失败: ${err.message}`, startedAt, finishedAt: nowIso() });
      });
    });
  }
}

export function pickExecutor(task: TaskDef): Executor {
  return task.executor_type === 'subprocess' ? new SubprocessExecutor() : new MockExecutor();
}

function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h << 5) - h + s.charCodeAt(i);
  return h;
}

/** 可被心跳异常打断的 sleep；同时承担执行超时 */
async function interruptibleSleep(ms: number, heartbeat: () => void, hardTimeout: number) {
  const step = 500;
  let elapsed = 0;
  const wait = Math.min(ms, hardTimeout);
  while (elapsed < wait) {
    await new Promise((r) => setTimeout(r, Math.min(step, wait - elapsed)));
    elapsed += step;
    heartbeat(); // 租约丢失时这里会抛
  }
}
