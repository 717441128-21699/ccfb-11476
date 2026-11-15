import { useEffect, useState } from 'react';
import { get, post, type InstanceRow } from '../api';
import { StatusBadge } from './StatusBadge';

interface Evt {
  id: number;
  event: string;
  detail: string | null;
  created_at: string;
}

export function InstanceModal({
  instance,
  onClose,
  embedded = false,
}: {
  instance: InstanceRow;
  onClose: () => void;
  embedded?: boolean;
}) {
  const [events, setEvents] = useState<Evt[]>([]);

  useEffect(() => {
    get<Evt[]>(`/api/events?instanceId=${instance.id}`).then(setEvents).catch(() => undefined);
  }, [instance.id]);

  async function kill() {
    if (!confirm(`确认 kill 实例 #${instance.id}（${instance.task_name}）？`)) return;
    await post(`/api/instances/${instance.id}/kill`);
    onClose();
  }

  const body = (
    <>
      <div className="kv">
        <div className="k">实例 ID</div><div>#{instance.id}</div>
        <div className="k">任务</div><div>{instance.task_name}</div>
        <div className="k">分区</div><div>{instance.partition}</div>
        <div className="k">状态</div><div><StatusBadge status={instance.status} /></div>
        <div className="k">尝试次数</div><div>{instance.attempt}</div>
        <div className="k">触发方式</div><div>{instance.trigger_type}</div>
        <div className="k">开始</div><div>{instance.started_at ?? '-'}</div>
        <div className="k">结束</div><div>{instance.finished_at ?? '-'}</div>
        <div className="k">耗时</div><div>{instance.duration_ms != null ? `${instance.duration_ms} ms` : '-'}</div>
        {instance.error_message && (
          <>
            <div className="k">错误</div>
            <div className="mono" style={{ color: 'var(--failed)' }}>{instance.error_message}</div>
          </>
        )}
      </div>
      <h4 style={{ margin: '16px 0 8px' }}>事件时间线</h4>
      <div className="event-log">
        {[...events].reverse().map((e) => (
          <div key={e.id}>
            <span className="muted">{e.created_at.slice(11, 19)}</span>{' '}
            <strong>{e.event}</strong>{' '}
            {e.detail && <span className="muted">{e.detail}</span>}
          </div>
        ))}
        {!events.length && <div className="muted">暂无事件</div>}
      </div>
      {!embedded && (
        <div style={{ display: 'flex', gap: 10, marginTop: 16 }}>
          {['pending', 'queued', 'running'].includes(instance.status) && (
            <button className="danger" onClick={kill}>Kill 实例</button>
          )}
          <button className="secondary" onClick={onClose}>关闭</button>
        </div>
      )}
    </>
  );

  if (embedded) return <div style={{ marginTop: 14 }}>{body}</div>;
  return (
    <div className="modal-bg" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>实例详情</h3>
        {body}
      </div>
    </div>
  );
}
