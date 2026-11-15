#!/usr/bin/env node
import { Store } from './db.js';
import { Scheduler } from './scheduler.js';
import { createApp } from './api.js';
import { loadConfig } from './config.js';
import { runCli } from './cli.js';

/**
 * 入口：
 *   tsx src/main.ts serve [--port 7100]   启动调度器 + HTTP/界面
 *   tsx src/main.ts <cli-command> ...      管理命令（见 cli.ts）
 */
async function main() {
  const cfg = loadConfig();
  const [cmd, ...rest] = process.argv.slice(2);
  const store = new Store(cfg.dbPath);

  if (!cmd || cmd === 'serve') {
    const portIdx = rest.indexOf('--port');
    const port = Number(portIdx >= 0 ? rest[portIdx + 1] : process.env.FLOWLINE_PORT ?? 7100);
    // 崩溃后立即重启、旧租约尚未超时时，显式 --takeover 强制接管
    const takeover = rest.includes('--takeover') || process.env.FLOWLINE_TAKEOVER === '1';
    const scheduler = new Scheduler(store, cfg);
    await scheduler.start({ takeover });
    // 派发循环作为主后台协程
    void scheduler.dispatchLoop();
    const app = createApp(scheduler, store);
    const server = app.listen(port, () => {
      console.log(`[flowline] 管理界面与 API: http://localhost:${port}`);
    });
    const shutdown = async () => {
      console.log('\n[flowline] 收到退出信号，正在优雅停止…');
      server.close();
      await scheduler.stop();
      store.close();
      process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
    return;
  }

  // 离线 CLI 也需要调度器内核的部分能力（如补数创建），但不启动派发循环
  const scheduler = new Scheduler(store, cfg);
  try {
    await runCli(scheduler, store, cmd, rest);
  } finally {
    store.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
