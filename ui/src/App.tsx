import { useEffect, useState } from 'react';
import { subscribeChanges } from './api';
import { GraphPage } from './pages/GraphPage';
import { TasksPage } from './pages/TasksPage';
import { InstancesPage } from './pages/InstancesPage';
import { BackfillPage } from './pages/BackfillPage';
import { AlertsPage } from './pages/AlertsPage';

type Tab = 'graph' | 'tasks' | 'instances' | 'backfill' | 'alerts';

const TABS: { key: Tab; label: string }[] = [
  { key: 'graph', label: '依赖图' },
  { key: 'tasks', label: '任务定义' },
  { key: 'instances', label: '运行实例' },
  { key: 'backfill', label: '补数管理' },
  { key: 'alerts', label: '告警中心' },
];

export function App() {
  const [tab, setTab] = useState<Tab>('graph');
  const [tick, setTick] = useState(0);

  // SSE 变更推送 -> 触发当前页刷新
  useEffect(() => subscribeChanges(() => setTick((t) => t + 1)), []);

  return (
    <div className="app">
      <aside className="sidebar">
        <h1>
          Flowline
          <small>数仓任务调度</small>
        </h1>
        {TABS.map((t) => (
          <div
            key={t.key}
            className={`nav-item ${tab === t.key ? 'active' : ''}`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </div>
        ))}
      </aside>
      <main className="main">
        {tab === 'graph' && <GraphPage tick={tick} />}
        {tab === 'tasks' && <TasksPage tick={tick} />}
        {tab === 'instances' && <InstancesPage tick={tick} />}
        {tab === 'backfill' && <BackfillPage tick={tick} />}
        {tab === 'alerts' && <AlertsPage tick={tick} />}
      </main>
    </div>
  );
}
