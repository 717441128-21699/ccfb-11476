/* Management UI logic — vanilla JS, no build step. */
const $ = (sel) => document.querySelector(sel);
const today = () => new Date().toISOString().slice(0, 10);
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'content-type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json();
}

function toast(msg) {
  const t = $('#toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2600);
}

function fmtTime(s) {
  if (!s) return '—';
  return s.replace('T', ' ').slice(0, 19);
}
function fmtMs(ms) {
  if (ms === null || ms === undefined) return '—';
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(2)}s`;
}
function badge(status) {
  return `<span class="badge b-${status}">${status}</span>`;
}
function esc(s) {
  return String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

/* ---------------- tabs ---------------- */
document.querySelectorAll('nav button').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('nav button').forEach((b) => b.classList.toggle('active', b === btn));
    document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.id === `tab-${btn.dataset.tab}`));
    loaders[btn.dataset.tab]?.();
  });
});

const loaders = {
  graph: loadGraph,
  tasks: loadTasks,
  instances: loadInstances,
  backfill: loadBackfills,
  alerts: loadAlerts,
};

/* ---------------- DAG graph ---------------- */
let graphData = null;
let nodePos = new Map();

async function loadGraph() {
  const date = $('#graphDate').value || today();
  $('#graphDate').value = date;
  graphData = await api(`/api/dag`);
  const insts = await api(`/api/instances?date=${encodeURIComponent(date)}&limit=500`);
  const instMap = new Map(insts.map((i) => [i.taskName, i]));
  $('#graphMeta').textContent =
    `${graphData.nodeCount} 节点 / ${graphData.edgeCount} 边 / ${graphData.layerCount} 层 / 预计关键路径 ${fmtMs(graphData.criticalPathExpectedMs)} · 分区 ${date}`;
  renderGraph(graphData, instMap);
}

function renderGraph(dag, instMap) {
  const NODE_W = 92, NODE_H = 34, GAP_X = 28, GAP_Y = 26, PAD = 30;
  const layerCounts = dag.layers.map((l) => l.length);
  const maxRows = Math.max(...layerCounts);
  const width = PAD * 2 + dag.layerCount * NODE_W + (dag.layerCount - 1) * GAP_X;
  const height = PAD * 2 + maxRows * NODE_H + (maxRows - 1) * GAP_Y;
  nodePos = new Map();

  const svg = [`<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">`];
  svg.push('<defs><marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="var(--line)"/></marker></defs>');

  // positions
  dag.layers.forEach((names, li) => {
    const layerH = names.length * NODE_H + (names.length - 1) * GAP_Y;
    const y0 = (height - layerH) / 2;
    names.forEach((name, ri) => {
      nodePos.set(name, { x: PAD + li * (NODE_W + GAP_X), y: y0 + ri * (NODE_H + GAP_Y), li });
    });
  });

  // edges
  for (const e of dag.edges) {
    const a = nodePos.get(e.upstreamTask), b = nodePos.get(e.downstreamTask);
    if (!a || !b) continue;
    const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2;
    const x2 = b.x, y2 = b.y + NODE_H / 2;
    const mx = (x1 + x2) / 2;
    svg.push(`<path class="edge" data-u="${esc(e.upstreamTask)}" data-d="${esc(e.downstreamTask)}"
      d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2 - 2},${y2}" marker-end="url(#arrow)"
      ${e.offsetDays ? `stroke-dasharray="4 3"` : ''}/>`);
  }

  // nodes
  for (const n of dag.nodes) {
    const p = nodePos.get(n.name);
    const inst = instMap.get(n.name);
    const status = inst?.status ?? 'PENDING';
    const fill = statusColor(status, n.name, inst);
    svg.push(`<g class="node" data-name="${esc(n.name)}" transform="translate(${p.x},${p.y})">
      <rect width="${NODE_W}" height="${NODE_H}" rx="7" fill="${fill}" fill-opacity="0.18" stroke="${fill}"/>
      <text x="7" y="13">${esc(n.name.slice(0, 12))}</text>
      <text x="7" y="27" fill="${css('--muted')}">${inst ? `${status.slice(0, 4)} ${fmtMs(inst.durationMs)}` : `L${n.layer}`}</text>
    </g>`);
  }
  svg.push('</svg>');
  $('#graphWrap').innerHTML = svg.join('');

  $('#graphWrap').querySelectorAll('.node').forEach((g) => {
    g.addEventListener('click', () => openTaskDrawer(g.dataset.name));
    g.addEventListener('mouseenter', () => highlight(g.dataset.name, true));
    g.addEventListener('mouseleave', () => highlight(g.dataset.name, false));
  });
}

function statusColor(status) {
  return {
    SUCCESS: css('--ok'), RUNNING: css('--run'), PENDING: css('--pend'),
    FAILED: css('--fail'), UPSTREAM_FAILED: css('--upfail'), KILLED: css('--kill'),
  }[status] || css('--pend');
}

function highlight(name, on) {
  const related = new Set([name]);
  for (const e of graphData.edges) {
    if (e.upstreamTask === name) related.add(e.downstreamTask);
    if (e.downstreamTask === name) related.add(e.upstreamTask);
  }
  document.querySelectorAll('#graphWrap .edge').forEach((p) => {
    p.classList.toggle('hot', on && (p.dataset.u === name || p.dataset.d === name));
  });
  document.querySelectorAll('#graphWrap .node').forEach((g) => {
    g.style.opacity = on && !related.has(g.dataset.name) ? '.25' : '1';
  });
}

/* ---------------- task table ---------------- */
async function loadTasks() {
  const [dag, tasks] = await Promise.all([api('/api/dag'), api('/api/tasks')]);
  const byName = new Map(tasks.map((t) => [t.name, t]));
  $('#taskCount').textContent = `共 ${dag.nodeCount} 个`;
  $('#taskTable').querySelector('tbody').innerHTML = dag.nodes.map((n) => {
    const t = byName.get(n.name);
    return `<tr style="cursor:pointer" data-name="${esc(n.name)}">
    <td class="mono">${esc(n.name)}</td><td>${esc(n.owner)}</td>
    <td class="mono muted" title="${esc(t?.command ?? '')}">${esc((t?.command ?? '').slice(0, 46))}</td>
    <td>${t?.retries ?? ''}</td>
    <td>${fmtMs(n.expectedDurationMs)}</td>
    <td>${n.slaMs ? fmtMs(n.slaMs) : '—'}</td>
    <td>${dag.edges.filter((e) => e.downstreamTask === n.name).length}</td>
    <td>${dag.edges.filter((e) => e.upstreamTask === n.name).length}</td>
    <td>${n.layer}</td>
  </tr>`;
  }).join('');
  $('#taskTable').querySelectorAll('tbody tr').forEach((tr) =>
    tr.addEventListener('click', () => openTaskDrawer(tr.dataset.name)));
}

/* ---------------- instances ---------------- */
async function loadInstances() {
  if (!$('#instDate').value) $('#instDate').value = today();
  const q = new URLSearchParams();
  if ($('#instDate').value) q.set('date', $('#instDate').value);
  if ($('#instStatus').value) q.set('status', $('#instStatus').value);
  if ($('#instTask').value.trim()) q.set('task', $('#instTask').value.trim());
  q.set('limit', '500');
  const rows = await api(`/api/instances?${q}`);
  const counts = {};
  rows.forEach((r) => { counts[r.status] = (counts[r.status] ?? 0) + 1; });
  $('#instStats').innerHTML = Object.entries(counts).map(([k, v]) =>
    `<div class="stat"><div class="n">${v}</div><div class="l">${k}</div></div>`).join('') ||
    '<div class="muted">无实例</div>';
  $('#instTable').querySelector('tbody').innerHTML = rows.map((i) => `<tr>
    <td>${i.id}</td><td class="mono">${esc(i.taskName)}</td><td>${i.logicalDate}</td>
    <td>${badge(i.status)}</td><td>${i.attempt}</td>
    <td class="muted">${fmtTime(i.enqueuedAt)}</td><td class="muted">${fmtTime(i.startedAt)}</td>
    <td class="muted">${fmtTime(i.finishedAt)}</td><td>${fmtMs(i.durationMs)}</td>
    <td>
      <button class="btn" data-detail="${i.id}">详情</button>
      ${['FAILED', 'KILLED', 'UPSTREAM_FAILED', 'PENDING'].includes(i.status) ? `<button class="btn primary" data-retry="${i.id}">重试</button>` : ''}
      ${i.status === 'RUNNING' ? `<button class="btn danger" data-kill="${i.id}">杀进程</button>` : ''}
    </td>
  </tr>`).join('') || '<tr><td colspan="10" class="muted">无数据</td></tr>';

  $('#instTable').querySelectorAll('[data-detail]').forEach((b) =>
    b.addEventListener('click', () => openInstanceDrawer(Number(b.dataset.detail))));
  $('#instTable').querySelectorAll('[data-retry]').forEach((b) =>
    b.addEventListener('click', async () => {
      await api(`/api/instances/${b.dataset.retry}/retry`, { method: 'POST' });
      toast('已重新入队'); loadInstances();
    }));
  $('#instTable').querySelectorAll('[data-kill]').forEach((b) =>
    b.addEventListener('click', async () => {
      await api(`/api/instances/${b.dataset.kill}/kill`, { method: 'POST' });
      toast('已终止'); loadInstances();
    }));
}

/* ---------------- drawer ---------------- */
function closeDrawer() { $('#drawer').classList.remove('open'); }

async function openTaskDrawer(name) {
  const [tasks, dag, insts] = await Promise.all([
    api('/api/tasks'), api('/api/dag'), api(`/api/instances?task=${encodeURIComponent(name)}&limit=20`),
  ]);
  const t = tasks.find((x) => x.name === name);
  const ups = dag.edges.filter((e) => e.downstreamTask === name).map((e) => `${e.upstreamTask}${e.offsetDays ? ` (offset ${e.offsetDays}d)` : ''}`);
  const downs = dag.edges.filter((e) => e.upstreamTask === name).map((e) => e.downstreamTask);
  $('#drawer').innerHTML = `<span class="close" onclick="document.getElementById('drawer').classList.remove('open')">✕</span>
    <h3>${esc(name)}</h3>
    <p class="muted">负责人 <b>${esc(t.owner)}</b> · 层 ${dag.nodes.find((n) => n.name === name)?.layer}</p>
    <pre>${esc(t.command)}</pre>
    <p>重试 ${t.retries} 次 · 退避 ${t.retryBackoffMs}ms · 预计 ${fmtMs(t.expectedDurationMs)} · SLA ${t.slaMs ? fmtMs(t.slaMs) : '无'}</p>
    <p><b>上游 (${ups.length})</b><br><span class="mono muted">${ups.map(esc).join('<br>') || '—'}</span></p>
    <p><b>下游 (${downs.length})</b><br><span class="mono muted">${downs.slice(0, 40).map(esc).join(', ')}${downs.length > 40 ? ' ...' : '' || '—'}</span></p>
    <h4>最近实例</h4>
    <table>${insts.map((i) => `<tr><td>${i.logicalDate}</td><td>${badge(i.status)}</td><td>${fmtMs(i.durationMs)}</td><td><button class="btn" data-iid="${i.id}">详情</button></td></tr>`).join('')}</table>`;
  $('#drawer').classList.add('open');
  $('#drawer').querySelectorAll('[data-iid]').forEach((b) =>
    b.addEventListener('click', () => openInstanceDrawer(Number(b.dataset.iid))));
}

async function openInstanceDrawer(id) {
  const data = await api(`/api/instances/${id}`);
  const events = await api(`/api/events?instanceId=${id}&limit=50`);
  const i = data.instance;
  $('#drawer').innerHTML = `<span class="close" onclick="document.getElementById('drawer').classList.remove('open')">✕</span>
    <h3>${esc(i.taskName)} @ ${i.logicalDate}</h3>
    <p>${badge(i.status)} · 尝试 ${i.attempt} 次 · 总运行 ${fmtMs(i.totalRuntimeMs)}${i.note ? `<br><span class="muted">${esc(i.note)}</span>` : ''}</p>
    <h4>运行记录 (attempts)</h4>
    <table><thead><tr><th>#</th><th>PID</th><th>状态</th><th>退出码</th><th>开始</th><th>结束</th></tr></thead><tbody>
    ${data.runs.map((r) => `<tr><td>${r.attempt}</td><td>${r.pid ?? '—'}</td><td>${badge(r.status)}</td>
      <td>${r.exitCode ?? '—'}</td><td class="muted">${fmtTime(r.startedAt)}</td><td class="muted">${fmtTime(r.finishedAt)}</td></tr>
      ${r.stderrTail ? `<tr><td colspan="6"><pre>${esc(r.stderrTail)}</pre></td></tr>` : ''}`).join('')}
    </tbody></table>
    <h4>事件</h4>
    <table>${events.map((e) => `<tr><td class="muted">${fmtTime(e.created_at)}</td><td>${esc(e.event_type)}</td><td class="muted">${esc(e.detail ?? '')}</td></tr>`).join('')}</table>`;
  $('#drawer').classList.add('open');
}

/* ---------------- backfill ---------------- */
async function submitBackfill(rerun = false) {
  const body = reranBody(rerun);
  const path = rerun ? '/api/rerun-downstream' : '/api/backfills';
  const res = await api(path, { method: 'POST', body });
  toast(`补数 #${res.job.id} 已提交`);
  loadBackfills();
}
function reranBody(rerun) {
  if (rerun) return { rootTask: $('#rrTask').value.trim(), dates: dateRange($('#rrStart').value, $('#rrEnd').value) };
  return {
    startDate: $('#bfStart').value, endDate: $('#bfEnd').value,
    maxConcurrency: Number($('#bfMax').value), maxPerPartition: Number($('#bfPerPart').value),
    parallelPartitions: Number($('#bfWindows').value),
  };
}
function dateRange(a, b) {
  const out = []; let d = a;
  while (d <= b) { out.push(d); d = new Date(new Date(d + 'T00:00:00Z').getTime() + 864e5).toISOString().slice(0, 10); }
  return out;
}

async function loadBackfills() {
  const rows = await api('/api/backfills');
  $('#bfTable').querySelector('tbody').innerHTML = rows.map((b) => `<tr>
    <td>${b.id}</td><td>${b.startDate} ~ ${b.endDate}</td>
    <td class="mono">${b.rootTask ?? '全量'}</td>
    <td>${b.rerunMode ? '子树重跑' : '普通补数'}</td>
    <td>${b.maxConcurrency}/${b.maxPerPartition}/窗${b.parallelPartitions}</td>
    <td>${badge(b.status === 'DONE' ? 'SUCCESS' : b.status === 'FAILED' ? 'FAILED' : 'RUNNING')}</td>
    <td class="muted">${fmtTime(b.createdAt)}</td><td class="muted">${fmtTime(b.finishedAt)}</td>
  </tr>`).join('') || '<tr><td colspan="8" class="muted">暂无补数</td></tr>';
}

/* ---------------- alerts ---------------- */
async function loadAlerts() {
  const unacked = $('#alertUnacked').checked ? '&acked=0' : '';
  const rows = await api(`/api/alerts?limit=300${unacked}`);
  $('#alertTable').querySelector('tbody').innerHTML = rows.map((a) => `<tr>
    <td>${a.id}</td><td><span class="badge b-${a.type}">${a.type}</span></td>
    <td class="mono">${esc(a.taskName ?? '—')}</td><td>${esc(a.logicalDate ?? '—')}</td>
    <td>${esc(a.message)}</td><td class="muted">${fmtTime(a.createdAt)}</td>
    <td>${a.acked ? '已确认' : '未确认'}</td>
    <td>${a.acked ? '' : `<button class="btn" data-ack="${a.id}">确认</button>`}</td>
  </tr>`).join('') || '<tr><td colspan="8" class="muted">无告警</td></tr>';
  $('#alertTable').querySelectorAll('[data-ack]').forEach((b) =>
    b.addEventListener('click', async () => { await api(`/api/alerts/${b.dataset.ack}/ack`, { method: 'POST' }); loadAlerts(); }));
}

/* ---------------- wiring & polling ---------------- */
$('#graphRefresh').addEventListener('click', loadGraph);
$('#instQuery').addEventListener('click', loadInstances);
$('#bfSubmit').addEventListener('click', () => submitBackfill(false));
$('#rrSubmit').addEventListener('click', () => submitBackfill(true));
$('#alertUnacked').addEventListener('change', loadAlerts);
$('#graphDate').addEventListener('change', loadGraph);
$('#instDate').addEventListener('change', loadInstances);

// defaults
$('#bfStart').value = $('#rrStart').value = today();
$('#bfEnd').value = $('#rrEnd').value = today();
$('#instDate').value = today();
$('#graphDate').value = today();

loadGraph();
setInterval(() => {
  const active = document.querySelector('nav button.active').dataset.tab;
  if (active === 'graph' || active === 'instances' || active === 'backfill' || active === 'alerts') {
    loaders[active]();
  }
}, 5000);
setInterval(async () => {
  const h = await api('/api/health');
  $('#tickInfo').textContent = `服务在线 · ${h.ts.slice(11, 19)}`;
}, 5000);
