/** SQLite persistence layer built on node:sqlite (Node 24+, zero native deps). */
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.ts';

let dbInstance: Database | null = null;

export type Database = DatabaseSync;

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS tasks (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  name                TEXT NOT NULL UNIQUE,
  owner               TEXT NOT NULL,
  command             TEXT NOT NULL,
  retries             INTEGER NOT NULL DEFAULT 2,
  retry_backoff_ms    INTEGER NOT NULL DEFAULT 1000,
  expected_duration_ms INTEGER NOT NULL DEFAULT 500,
  sla_ms              INTEGER,
  sla_warn_ratio      REAL NOT NULL DEFAULT 0.8,
  created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS edges (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  upstream_task     TEXT NOT NULL REFERENCES tasks(name) ON DELETE CASCADE,
  downstream_task   TEXT NOT NULL REFERENCES tasks(name) ON DELETE CASCADE,
  offset_days       INTEGER NOT NULL DEFAULT 0,
  UNIQUE(upstream_task, downstream_task, offset_days)
);
CREATE INDEX IF NOT EXISTS idx_edges_down ON edges(downstream_task);
CREATE INDEX IF NOT EXISTS idx_edges_up ON edges(upstream_task);

CREATE TABLE IF NOT EXISTS instances (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  task_name         TEXT NOT NULL REFERENCES tasks(name) ON DELETE CASCADE,
  logical_date      TEXT NOT NULL,
  status            TEXT NOT NULL,
  attempt           INTEGER NOT NULL DEFAULT 0,
  retry_after       TEXT,
  enqueued_at       TEXT NOT NULL DEFAULT (datetime('now')),
  started_at        TEXT,
  finished_at       TEXT,
  duration_ms       INTEGER,
  total_runtime_ms  INTEGER NOT NULL DEFAULT 0,
  backfill_id       INTEGER,
  note              TEXT,
  UNIQUE(task_name, logical_date)
);
CREATE INDEX IF NOT EXISTS idx_inst_date ON instances(logical_date);
CREATE INDEX IF NOT EXISTS idx_inst_status ON instances(status);
CREATE INDEX IF NOT EXISTS idx_inst_bf ON instances(backfill_id);

CREATE TABLE IF NOT EXISTS instance_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id   INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
  attempt       INTEGER NOT NULL,
  pid           INTEGER,
  status        TEXT NOT NULL,
  marker_path   TEXT,
  started_at    TEXT NOT NULL DEFAULT (datetime('now')),
  heartbeat_at  TEXT,
  finished_at   TEXT,
  exit_code     INTEGER,
  stdout_tail   TEXT,
  stderr_tail   TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_inst ON instance_runs(instance_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON instance_runs(status);

CREATE TABLE IF NOT EXISTS events (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id   INTEGER,
  task_name     TEXT,
  logical_date  TEXT,
  event_type    TEXT NOT NULL,
  detail        TEXT,
  created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_inst ON events(instance_id);

CREATE TABLE IF NOT EXISTS alerts (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  type          TEXT NOT NULL,
  task_name     TEXT,
  instance_id   INTEGER,
  logical_date  TEXT,
  message       TEXT NOT NULL,
  channel       TEXT NOT NULL DEFAULT 'console',
  acked         INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_alerts_acked ON alerts(acked);

CREATE TABLE IF NOT EXISTS backfills (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  start_date          TEXT NOT NULL,
  end_date            TEXT NOT NULL,
  root_task           TEXT,
  rerun_mode          INTEGER NOT NULL DEFAULT 0,
  max_concurrency     INTEGER NOT NULL,
  max_per_partition   INTEGER NOT NULL,
  parallel_partitions INTEGER NOT NULL,
  status              TEXT NOT NULL,
  created_at          TEXT NOT NULL DEFAULT (datetime('now')),
  finished_at         TEXT
);

CREATE TABLE IF NOT EXISTS scheduler_meta (
  id              INTEGER PRIMARY KEY CHECK (id = 1),
  singleton_lock  TEXT,
  last_tick_at    TEXT
);
INSERT OR IGNORE INTO scheduler_meta (id, singleton_lock, last_tick_at) VALUES (1, NULL, NULL);
`;

export function getDb(): Database {
  if (dbInstance) return dbInstance;
  fs.mkdirSync(config.dataDir, { recursive: true });
  const db = new DatabaseSync(config.dbPath);
  db.exec(SCHEMA);
  dbInstance = db;
  return db;
}

/** node:sqlite returns null-prototype objects; convert to plain objects for JSON. */
export function plain<T>(row: unknown): T {
  return (row && typeof row === 'object') ? { ...(row as object) } as T : row as T;
}

export function now(): string {
  return new Date().toISOString();
}

export function beginTx(db: Database): void {
  db.exec('BEGIN IMMEDIATE');
}
export function commit(db: Database): void {
  db.exec('COMMIT');
}
export function rollback(db: Database): void {
  try { db.exec('ROLLBACK'); } catch { /* already rolled back */ }
}

export function resetDatabase(): void {
  if (dbInstance) {
    dbInstance.close();
    dbInstance = null;
  }
  for (const suffix of ['', '-wal', '-shm']) {
    const p = config.dbPath + suffix;
    if (fs.existsSync(p)) fs.rmSync(p, { force: true });
  }
}

export function ensureRuntimeDirs(): void {
  for (const d of [config.dataDir, config.scriptsDir, config.markersDir, config.logDir]) {
    fs.mkdirSync(d, { recursive: true });
  }
}

export function dbFilePath(): string {
  return path.resolve(config.dbPath);
}
