import path from 'node:path';
import type { SchedulerConfig } from './types.js';

/** 环境变量 -> 配置，全部带默认值，开箱即跑 */
export function loadConfig(): SchedulerConfig {
  const root = process.cwd();
  return {
    dbPath: process.env.FLOWLINE_DB
      ? path.resolve(process.env.FLOWLINE_DB)
      : path.join(root, 'data', 'flowline.db'),
    maxConcurrent: Number(process.env.FLOWLINE_CONCURRENCY ?? 16),
    pollIntervalMs: Number(process.env.FLOWLINE_POLL_MS ?? 250),
    leaseTimeoutMs: Number(process.env.FLOWLINE_LEASE_MS ?? 15_000),
    alertWebhookUrl: process.env.FLOWLINE_WEBHOOK_URL || null,
    historyLookbackDays: Number(process.env.FLOWLINE_LOOKBACK ?? 3),
  };
}

export function nowIso(): string {
  return new Date().toISOString();
}

/** 'YYYY-MM-DD'（UTC，分区只标识数据日期，与时区解耦） */
export function partitionDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export function addDays(iso: string, days: number): string {
  const d = new Date(iso + 'T00:00:00.000Z');
  d.setUTCDate(d.getUTCDate() + days);
  return partitionDate(d);
}

/** 含首尾的日期区间 */
export function dateRange(from: string, to: string): string[] {
  const out: string[] = [];
  let cur = from;
  if (cur > to) throw new Error(`dateRange: from(${from}) > to(${to})`);
  while (cur <= to) {
    out.push(cur);
    cur = addDays(cur, 1);
  }
  return out;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
