import { useEffect, useState } from 'react';
import { get, post, patch, del, type TaskRow } from '../api';

const EMPTY = {
  name: '', owner: '', description: '', executor_type: 'mock',
  executor_config: '{"mockDurationMs":200,"jitterRatio":0.1}',
  retry_max: 2, retry_delay_ms: 1000, timeout_ms: 3600000,
  sla_deadline: '', sla_duration_ms: '', upstreamNames: '',
};

export function TasksPage({ tick }: { tick: number }) {
  const [tasks, setTasks] = useState<TaskRow[]>([]);
  const [form, setForm] = useState({ ...EMPTY });
  const [editName, setEditName] = useState<string | null>(null);
  const [msg, setMsg] = useState('');
  const [edgeForm, setEdgeForm] = useState({ from: '', to: '' });

  async function load() {
    setTasks(await get<TaskRow[]>('/api/tasks'));
  }
  useEffect(() => { load().catch((e) => setMsg(e.message)); }, [tick]);

  async function submit() {
    try {
      const body = {
        name: form.name,
        owner: form.owner,
        description: form.description,
        executor_type: form.executor_type,
        executor_config: JSON.parse(form.executor_config),
        retry_max: Number(form.retry_max),
        retry_delay_ms: Number(form.retry_delay_ms),
        timeout_ms: Number(form.timeout_ms),
        sla_deadline: form.sla_deadline || null,
        sla_duration_ms: form.sla_duration_ms ? Number(form.sla_duration_ms) : null,
        upstreamNames: form.upstreamNames.split(',').map((s) => s.trim()).filter(Boolean),
      };
      if (editName) {
        await patch(`/api/tasks/${editName}`, body);
        setMsg(`已更新 ${editName}`);
      } else {
        await post('/api/tasks', body);
        setMsg(`已创建 ${form.name}`);
      }
      setForm({ ...EMPTY });
      setEditName(null);
      load();
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  function edit(t: TaskRow) {
    setEditName(t.name);
    setForm({
      name: t.name, owner: t.owner, description: t.description,
      executor_type: t.executor_type, executor_config: t.executor_config,
      retry_max: t.retry_max, retry_delay_ms: 0, timeout_ms: t.timeout_ms,
      sla_deadline: t.sla_deadline ?? '', sla_duration_ms: t.sla_duration_ms ? String(t.sla_duration_ms) : '',
      upstreamNames: '',
    });
  }

  async function remove(name: string) {
    if (!confirm(`删除任务 ${name}？其依赖边会一并删除`)) return;
    await del(`/api/tasks/${name}`);
    load();
  }

  async function addEdge() {
    try {
      await post('/api/dependencies', edgeForm);
      setMsg(`已加依赖 ${edgeForm.from} -> ${edgeForm.to}`);
      setEdgeForm({ from: '', to: '' });
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  const field = (k: keyof typeof EMPTY) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.value }));

  return (
    <div>
      <h2 className="page-title">任务定义（{tasks.length}）</h2>
      {msg && <div className="error">{msg}</div>}

      <div className="card">
        <h3 style={{ marginTop: 0 }}>{editName ? `编辑任务：${editName}` : '新建任务'}</h3>
        <div className="form-row">
          <div><label>任务名（英文标识）</label><input value={form.name} onChange={field('name')} disabled={!!editName} /></div>
          <div><label>负责人</label><input value={form.owner} onChange={field('owner')} /></div>
          <div><label>执行器</label>
            <select value={form.executor_type} onChange={field('executor_type')}>
              <option value="mock">mock（模拟）</option>
              <option value="subprocess">subprocess（真实脚本）</option>
            </select>
          </div>
        </div>
        <div className="form-row" style={{ marginTop: 10 }}>
          <div style={{ flex: 2 }}><label>描述</label><input value={form.description} onChange={field('description')} /></div>
          <div style={{ flex: 2 }}><label>上游任务（逗号分隔，仅新建时生效）</label><input value={form.upstreamNames} onChange={field('upstreamNames')} placeholder="a, b" /></div>
        </div>
        <div className="form-row" style={{ marginTop: 10 }}>
          <div><label>最大重试次数</label><input type="number" value={form.retry_max} onChange={field('retry_max')} /></div>
          <div><label>重试退避基数(ms)</label><input type="number" value={form.retry_delay_ms} onChange={field('retry_delay_ms')} /></div>
          <div><label>超时(ms)</label><input type="number" value={form.timeout_ms} onChange={field('timeout_ms')} /></div>
        </div>
        <div className="form-row" style={{ marginTop: 10 }}>
          <div><label>SLA 截止时刻 HH:MM（可选）</label><input value={form.sla_deadline} onChange={field('sla_deadline')} placeholder="09:00" /></div>
          <div><label>SLA 时长预算(ms，可选)</label><input type="number" value={form.sla_duration_ms} onChange={field('sla_duration_ms')} /></div>
          <div style={{ flex: 2 }}><label>执行器配置 JSON</label><input value={form.executor_config} onChange={field('executor_config')} /></div>
        </div>
        <div style={{ marginTop: 14, display: 'flex', gap: 10 }}>
          <button onClick={submit}>{editName ? '保存修改' : '创建任务'}</button>
          {editName && <button className="secondary" onClick={() => { setEditName(null); setForm({ ...EMPTY }); }}>取消编辑</button>}
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>添加依赖边（成环会被拒绝）</h3>
        <div className="form-row">
          <div><label>上游 from</label>
            <select value={edgeForm.from} onChange={(e) => setEdgeForm((s) => ({ ...s, from: e.target.value }))}>
              <option value="">选择…</option>
              {tasks.map((t) => <option key={t.id} value={t.name}>{t.name}</option>)}
            </select>
          </div>
          <div><label>下游 to</label>
            <select value={edgeForm.to} onChange={(e) => setEdgeForm((s) => ({ ...s, to: e.target.value }))}>
              <option value="">选择…</option>
              {tasks.map((t) => <option key={t.id} value={t.name}>{t.name}</option>)}
            </select>
          </div>
          <div style={{ flex: '0 0 auto' }}><button onClick={addEdge} disabled={!edgeForm.from || !edgeForm.to}>加依赖</button></div>
        </div>
      </div>

      <div className="card">
        <table>
          <thead><tr><th>ID</th><th>名称</th><th>负责人</th><th>执行器</th><th>重试</th><th>SLA</th><th>启用</th><th>操作</th></tr></thead>
          <tbody>
            {tasks.map((t) => (
              <tr key={t.id}>
                <td>{t.id}</td>
                <td className="mono">{t.name}</td>
                <td>{t.owner}</td>
                <td>{t.executor_type}</td>
                <td>{t.retry_max}</td>
                <td className="mono">{t.sla_deadline ?? (t.sla_duration_ms ? `${t.sla_duration_ms}ms` : '-')}</td>
                <td>{t.enabled ? '是' : '否'}</td>
                <td style={{ display: 'flex', gap: 6 }}>
                  <button className="secondary" onClick={() => edit(t)}>编辑</button>
                  <button className="danger" onClick={() => remove(t.name)}>删除</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
