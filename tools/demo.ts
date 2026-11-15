/**
 * Demo bootstrap. ESM static imports are hoisted, so the execution-mode env var must be
 * set before any module that reads config is evaluated. This file sets it, then loads the
 * real demo implementation dynamically.
 *
 * Run: npm run demo
 */
process.env.SCHED_EXEC_MODE ??= 'inprocess';
const { runDemo } = await import('./demo-run.ts');
await runDemo();
