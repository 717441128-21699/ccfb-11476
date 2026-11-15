import { useEffect, useMemo, useState } from 'react';
import { get, post, type GraphData, type DagNode } from '../api';
import { StatusBadge } from '../components/StatusBadge';
import { InstanceModal } from '../components/InstanceModal';

const today = () => new Date().toISOString().slice(0, 10);

export function GraphPage({ tick }: { tick: number }) {
  const [date, setDate] = useState(today());
  const [graph, setGraph] = useState<GraphData | null>(null);
  const [selected, setSelected] = useState<DagNode | null>(null);
  const [msg, setMsg] = useState('');

  useEffect(() => {
    get<GraphData>(`/api/graph?date=${date}`).then(setGraph).catch((e) => setMsg(e.message));
  }, [date, tick]);

  const cols = useMemo(() => {
    if (!graph) return [];
    const maxLayer = Math.max(...graph.nodes.map((n) => n.layer));
    return Array.from({ length: maxLayer + 1 }, (_, l) => graph.nodes.filter((n) => n.layer === l));
  }, [graph]);

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    graph?.nodes.forEach((n) => {
      if (n.todayStatus) c[n.todayStatus] = (c[n.todayStatus] ?? 0) + 1;
    });
    return c;
  }, [graph]);

  async function rerun(node: DagNode) {
    if (!confirm(`重跑 ${node.name} 及其所有下游在 ${date} 的分区？`)) return;
    try {
      const r = await post<{ resetInstances: number }>('/api/rerun', {
        taskName: node.name,
        partition: date,
        reason: '依赖图一键重跑',
      });
      setMsg(`已重置 ${r.resetInstances} 个实例`);
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  return (
    <div>
      <h2 className="page-title">依赖图（按分区查看节点状态）</h2>
      <div className="toolbar">
        <label style={{ margin: 0 }}>分区日期</label>
        <input type="date" value={date} onChange={(e) => setDate(e.target.value)} />
        <div className="legend">
          {Object.entries(counts).map(([s, c]) => (
            <span key={s}>
              <StatusBadge status={s} /> {c}
            </span>
          ))}
        </div>
      </div>
      {msg && <div className="error">{msg}</div>}
      <div className="dag-wrap">
        <div className="dag-layer">
          {cols.map((col, li) => (
            <div className="dag-col" key={li}>
              <h4>第 {li} 层 · {col.length}</h4>
              {col.map((n) => (
                <div key={n.id} className="dag-node" onClick={() => setSelected(n)}>
                  <div className="nm">
                    <span className={`dot`} style={{ background: `var(--${n.todayStatus ?? 'pending'})` }} />
                    {n.name}
                  </div>
                  <div className="meta">
                    {n.todayStatus ? <StatusBadge status={n.todayStatus} /> : <span className="muted">无实例</span>}
                    {n.todayDurationMs != null && ` · ${n.todayDurationMs}ms`}
                    {n.todayAttempt != null && n.todayAttempt > 1 && ` · 重试${n.todayAttempt - 1}次`}
                  </div>
                  <div className="meta">{n.owner}</div>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
      {selected && (
        <NodeModal
          node={selected}
          date={date}
          onClose={() => setSelected(null)}
          onRerun={() => rerun(selected)}
        />
      )}
    </div>
  );
}

function NodeModal({
  node,
  date,
  onClose,
  onRerun,
}: {
  node: DagNode;
  date: string;
  onClose: () => void;
  onRerun: () => void;
}) {
  return (
    <div className="modal-bg" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>{node.name}</h3>
        <div className="kv">
          <div className="k">负责人</div><div>{node.owner}</div>
          <div className="k">DAG 层级</div><div>{node.layer}</div>
          <div className="k">分区</div><div>{date}</div>
          <div className="k">当日状态</div><div>{node.todayStatus ? <StatusBadge status={node.todayStatus} /> : '无实例'}</div>
          <div className="k">耗时</div><div>{node.todayDurationMs != null ? `${node.todayDurationMs} ms` : '-'}</div>
          <div className="k">尝试次数</div><div>{node.todayAttempt ?? '-'}</div>
        </div>
        <div style={{ display: 'flex', gap: 10, marginTop: 18 }}>
          <button onClick={onRerun}>一键重跑（含下游）</button>
          <button className="secondary" onClick={onClose}>关闭</button>
        </div>
        <InstanceModalHint name={node.name} date={date} />
      </div>
    </div>
  );
}

function InstanceModalHint({ name, date }: { name: string; date: string }) {
  const [inst, setInst] = useState<unknown>(null);
  useEffect(() => {
    get<unknown[]>(`/api/instances?date=${date}&limit=1000`)
      .then((rows) => setInst((rows as { task_name: string }[]).find((r) => r.task_name === name) ?? null))
      .catch(() => undefined);
  }, [name, date]);
  if (!inst) return null;
  return <InstanceModal instance={inst as never} onClose={() => undefined} embedded />;
}
