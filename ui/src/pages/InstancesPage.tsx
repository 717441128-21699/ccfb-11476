import { useEffect, useState } from 'react';
import { get, type InstanceRow } from '../api';
import { StatusBadge } from '../components/StatusBadge';
import { InstanceModal } from '../components/InstanceModal';

const STATUSES = ['', 'pending', 'queued', 'running', 'success', 'failed', 'upstream_failed', 'killed'];

export function InstancesPage({ tick }: { tick: number }) {
  const [date, setDate] = useState('');
  const [status, setStatus] = useState('');
  const [rows, setRows] = useState<InstanceRow[]>([]);
  const [selected, setSelected] = useState<InstanceRow | null>(null);

  useEffect(() => {
    const qs = new URLSearchParams();
    if (date) qs.set('date', date);
    if (status) qs.set('status', status);
    qs.set('limit', '500');
    get<InstanceRow[]>(`/api/instances?${qs}`).then(setRows).catch(() => undefined);
  }, [date, status, tick]);

  return (
    <div>
      <h2 className="page-title">运行实例（{rows.length}）</h2>
      <div className="toolbar">
        <div><label style={{ margin: 0 }}>分区日期</label>
          <input type="date" value={date} onChange={(e) => setDate(e.target.value)} />
        </div>
        <div><label style={{ margin: 0 }}>状态</label>
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            {STATUSES.map((s) => <option key={s} value={s}>{s || '全部'}</option>)}
          </select>
        </div>
        <button className="secondary" onClick={() => { setDate(''); setStatus(''); }}>重置筛选</button>
      </div>

      <div className="card">
        <table>
          <thead>
            <tr><th>ID</th><th>任务</th><th>分区</th><th>状态</th><th>尝试</th><th>触发</th><th>耗时</th><th>开始时间</th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} style={{ cursor: 'pointer' }} onClick={() => setSelected(r)}>
                <td>#{r.id}</td>
                <td className="mono">{r.task_name}</td>
                <td>{r.partition}</td>
                <td><StatusBadge status={r.status} /></td>
                <td>{r.attempt}</td>
                <td>{r.trigger_type}</td>
                <td>{r.duration_ms != null ? `${r.duration_ms}ms` : '-'}</td>
                <td className="muted">{r.started_at ? r.started_at.slice(11, 19) : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!rows.length && <p className="muted">没有匹配的实例</p>}
      </div>
      {selected && <InstanceModal instance={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}
