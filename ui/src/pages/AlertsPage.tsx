import { useEffect, useState } from 'react';
import { get, post, type AlertRow } from '../api';

export function AlertsPage({ tick }: { tick: number }) {
  const [rows, setRows] = useState<AlertRow[]>([]);
  const [onlyUnacked, setOnlyUnacked] = useState(false);

  async function load() {
    setRows(await get<AlertRow[]>(`/api/alerts?limit=200${onlyUnacked ? '&unacked=1' : ''}`));
  }
  useEffect(() => { load().catch(() => undefined); }, [tick, onlyUnacked]);

  async function ack(id: number) {
    await post(`/api/alerts/${id}/ack`);
    load();
  }

  return (
    <div>
      <h2 className="page-title">告警中心</h2>
      <div className="toolbar">
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, margin: 0 }}>
          <input type="checkbox" checked={onlyUnacked} onChange={(e) => setOnlyUnacked(e.target.checked)} />
          只看未确认
        </label>
      </div>
      <div className="card">
        <table>
          <thead><tr><th>级别</th><th>类型</th><th>任务</th><th>分区</th><th>内容</th><th>时间</th><th>状态</th><th></th></tr></thead>
          <tbody>
            {rows.map((a) => (
              <tr key={a.id}>
                <td><span className={`badge ${a.level}`}>{a.level}</span></td>
                <td>{a.type}</td>
                <td className="mono">{a.task_name ?? '-'}</td>
                <td>{a.partition ?? '-'}</td>
                <td style={{ maxWidth: 420 }}>{a.message}</td>
                <td className="muted">{a.created_at.slice(5, 19).replace('T', ' ')}</td>
                <td>{a.acknowledged ? '已确认' : <span style={{ color: 'var(--warn)' }}>待处理</span>}</td>
                <td>{!a.acknowledged && <button className="secondary" onClick={() => ack(a.id)}>确认</button>}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!rows.length && <p className="muted">暂无告警</p>}
      </div>
    </div>
  );
}
