import { useEffect, useState } from 'react';
import { get, post, type BackfillRow, type TaskRow } from '../api';
import { StatusBadge } from '../components/StatusBadge';

interface Progress {
  id: number;
  total: number;
  done: number;
  byStatus: Record<string, number>;
  backfill: BackfillRow;
}

export function BackfillPage({ tick }: { tick: number }) {
  const [rows, setRows] = useState<BackfillRow[]>([]);
  const [tasks, setTasks] = useState<TaskRow[]>([]);
  const [progress, setProgress] = useState<Record<number, Progress>>({});
  const [form, setForm] = useState({
    name: '', dateFrom: '', dateTo: '', taskName: '', concurrency: 8, includeDownstream: true,
  });
  const [rerunForm, setRerunForm] = useState({ taskName: '', partitions: '' });
  const [msg, setMsg] = useState('');

  async function load() {
    const bfs = await get<BackfillRow[]>('/api/backfills');
    setRows(bfs);
    setTasks(await get<TaskRow[]>('/api/tasks'));
    const ps: Record<number, Progress> = {};
    await Promise.all(
      bfs.map(async (b) => {
        ps[b.id] = await get<Progress>(`/api/backfills/${b.id}`);
      }),
    );
    setProgress(ps);
  }
  useEffect(() => { load().catch((e) => setMsg(e.message)); }, [tick]);

  async function createBackfill() {
    try {
      const bf = await post<BackfillRow>('/api/backfills', {
        name: form.name || `bf-${form.dateFrom}_${form.dateTo}`,
        dateFrom: form.dateFrom,
        dateTo: form.dateTo,
        taskName: form.taskName || null,
        includeDownstream: form.includeDownstream,
        concurrency: Number(form.concurrency),
        createdBy: 'web',
      });
      setMsg(`补数单 #${bf.id} 已创建并开始调度`);
      setForm({ name: '', dateFrom: '', dateTo: '', taskName: '', concurrency: 8, includeDownstream: true });
      load();
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  async function rerun() {
    const partitions = rerunForm.partitions.split(',').map((s) => s.trim()).filter(Boolean);
    if (!rerunForm.taskName || !partitions.length) {
      setMsg('请填写任务名和至少一个分区');
      return;
    }
    if (!confirm(`重跑 ${rerunForm.taskName} 及全部下游 × ${partitions.length} 个分区？`)) return;
    try {
      const r = await post<{ resetInstances: number }>('/api/rerun', {
        taskName: rerunForm.taskName,
        partitions,
        reason: '补数页一键重跑',
      });
      setMsg(`已重置 ${r.resetInstances} 个实例`);
      setRerunForm({ taskName: '', partitions: '' });
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  return (
    <div>
      <h2 className="page-title">补数与重跑</h2>
      {msg && <div className="error">{msg}</div>}

      <div className="card">
        <h3 style={{ marginTop: 0 }}>创建补数单（按日期区间，依赖顺序并行 + 并发上限）</h3>
        <div className="form-row">
          <div><label>名称（可选）</label><input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
          <div><label>起始日期</label><input type="date" value={form.dateFrom} onChange={(e) => setForm({ ...form, dateFrom: e.target.value })} /></div>
          <div><label>结束日期</label><input type="date" value={form.dateTo} onChange={(e) => setForm({ ...form, dateTo: e.target.value })} /></div>
        </div>
        <div className="form-row" style={{ marginTop: 10 }}>
          <div><label>起始任务（留空=全 DAG）</label>
            <select value={form.taskName} onChange={(e) => setForm({ ...form, taskName: e.target.value })}>
              <option value="">全 DAG</option>
              {tasks.map((t) => <option key={t.id} value={t.name}>{t.name}</option>)}
            </select>
          </div>
          <div style={{ flex: '0 0 120px' }}><label>并发上限</label>
            <input type="number" value={form.concurrency} onChange={(e) => setForm({ ...form, concurrency: Number(e.target.value) })} />
          </div>
          <div style={{ flex: '0 0 auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <input id="incds" type="checkbox" checked={form.includeDownstream}
              onChange={(e) => setForm({ ...form, includeDownstream: e.target.checked })} />
            <label htmlFor="incds" style={{ margin: 0 }}>包含全部下游</label>
          </div>
          <div style={{ flex: '0 0 auto' }}>
            <button onClick={createBackfill} disabled={!form.dateFrom || !form.dateTo}>创建并运行</button>
          </div>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>一键重跑（任务 + 全下游 × 指定分区）</h3>
        <div className="form-row">
          <div><label>任务名</label>
            <select value={rerunForm.taskName} onChange={(e) => setRerunForm({ ...rerunForm, taskName: e.target.value })}>
              <option value="">选择任务…</option>
              {tasks.map((t) => <option key={t.id} value={t.name}>{t.name}</option>)}
            </select>
          </div>
          <div style={{ flex: 2 }}><label>分区（逗号分隔，可多选日期）</label>
            <input placeholder="2026-11-01, 2026-11-02" value={rerunForm.partitions}
              onChange={(e) => setRerunForm({ ...rerunForm, partitions: e.target.value })} />
          </div>
          <div style={{ flex: '0 0 auto' }}><button className="danger" onClick={rerun}>重跑</button></div>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>补数单（{rows.length}）</h3>
        <table>
          <thead><tr><th>ID</th><th>名称</th><th>区间</th><th>任务</th><th>并发</th><th>状态</th><th>进度</th></tr></thead>
          <tbody>
            {rows.map((b) => {
              const p = progress[b.id];
              const pct = p && p.total ? Math.round((p.done / p.total) * 100) : 0;
              return (
                <tr key={b.id}>
                  <td>#{b.id}</td>
                  <td>{b.name}</td>
                  <td className="mono">{b.date_from} ~ {b.date_to}</td>
                  <td>{b.task_name ?? '全 DAG'}</td>
                  <td>{b.concurrency}</td>
                  <td><StatusBadge status={b.status} /></td>
                  <td style={{ minWidth: 200 }}>
                    <div>{p ? `${p.done}/${p.total}（${pct}%）` : '-'}</div>
                    <div style={{ height: 6, background: 'var(--panel-2)', borderRadius: 3, marginTop: 4 }}>
                      <div style={{ width: `${pct}%`, height: '100%', background: 'var(--accent)', borderRadius: 3 }} />
                    </div>
                    {p && <div className="muted" style={{ fontSize: 11, marginTop: 3 }}>{JSON.stringify(p.byStatus)}</div>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
