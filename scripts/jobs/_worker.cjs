// 由 gen-dag 自动生成的假执行脚本运行器（真实子进程模式可用）
// 读取环境变量 FLOWLINE_TASK / FLOWLINE_PARTITION / FLOWLINE_ATTEMPT
const task = process.env.FLOWLINE_TASK || 'unknown';
const partition = process.env.FLOWLINE_PARTITION || 'unknown';
const attempt = Number(process.env.FLOWLINE_ATTEMPT || '1');
const duration = Number(process.argv[2] || 200);
const failTimes = Number(process.argv[3] || 0);
const started = Date.now();
const timer = setInterval(() => {
  process.stdout.write(`[${task}] ${partition} attempt=${attempt} running ${Date.now() - started}ms\n`);
}, Math.max(50, duration / 4));
setTimeout(() => {
  clearInterval(timer);
  if (attempt <= failTimes) {
    console.error(`[${task}] 模拟失败 attempt=${attempt}`);
    process.exit(1);
  }
  console.log(JSON.stringify({ task, partition, attempt, durationMs: Date.now() - started, status: 'ok' }));
  process.exit(0);
}, duration);
