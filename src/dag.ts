/** In-memory DAG model: cycle check, topo order, descendants, layered layout. */
import type { EdgeDef, TaskDef } from './types.ts';
import { getDb } from './db.ts';
import { listEdges, listTasks } from './repo.ts';

export interface DagNode {
  task: TaskDef;
  upstreams: EdgeDef[];   // edges where this task is downstream
  downstreams: EdgeDef[]; // edges where this task is upstream
  layer: number;
}

export class DagCycleError extends Error {
  cycle: string[];
  constructor(cycle: string[]) {
    super(`DAG cycle detected: ${cycle.join(' -> ')}`);
    this.name = 'DagCycleError';
    this.cycle = cycle;
  }
}

export class Dag {
  readonly nodes = new Map<string, DagNode>();
  readonly topo: string[];
  readonly layers: string[][];

  constructor(tasks: TaskDef[], edges: EdgeDef[]) {
    for (const t of tasks) {
      this.nodes.set(t.name, { task: t, upstreams: [], downstreams: [], layer: 0 });
    }
    for (const e of edges) {
      const u = this.nodes.get(e.upstreamTask);
      const d = this.nodes.get(e.downstreamTask);
      if (!u || !d) throw new Error(`edge references unknown task: ${e.upstreamTask}->${e.downstreamTask}`);
      u.downstreams.push(e);
      d.upstreams.push(e);
    }
    this.topo = this.computeTopo();
    this.layers = this.computeLayers();
  }

  private computeTopo(): string[] {
    const indeg = new Map<string, number>();
    for (const name of this.nodes.keys()) indeg.set(name, 0);
    for (const n of this.nodes.values()) {
      indeg.set(n.task.name, n.upstreams.length);
    }
    // deterministic Kahn: process names in sorted order
    const ready = [...indeg.entries()].filter(([, d]) => d === 0).map(([n]) => n).sort();
    const order: string[] = [];
    while (ready.length) {
      const name = ready.shift()!;
      order.push(name);
      for (const e of this.nodes.get(name)!.downstreams) {
        const left = (indeg.get(e.downstreamTask) ?? 1) - 1;
        indeg.set(e.downstreamTask, left);
        if (left === 0) {
          ready.push(e.downstreamTask);
          ready.sort();
        }
      }
    }
    if (order.length !== this.nodes.size) {
      const remaining = [...indeg.entries()].filter(([, d]) => d > 0).map(([n]) => n);
      throw new DagCycleError(remaining);
    }
    return order;
  }

  private computeLayers(): string[][] {
    const layerOf = new Map<string, number>();
    for (const name of this.topo) {
      const n = this.nodes.get(name)!;
      let layer = 0;
      for (const e of n.upstreams) {
        layer = Math.max(layer, (layerOf.get(e.upstreamTask) ?? 0) + 1);
      }
      layerOf.set(name, layer);
      n.layer = layer;
    }
    const maxLayer = Math.max(0, ...layerOf.values());
    const layers: string[][] = Array.from({ length: maxLayer + 1 }, () => []);
    for (const [name, l] of layerOf) layers[l]!.push(name);
    for (const l of layers) l.sort();
    return layers;
  }

  /** all descendants of a task (not including itself), in topological order */
  descendants(name: string): string[] {
    const seen = new Set<string>();
    const stack = [name];
    while (stack.length) {
      const cur = stack.pop()!;
      for (const e of this.nodes.get(cur)!.downstreams) {
        if (!seen.has(e.downstreamTask)) {
          seen.add(e.downstreamTask);
          stack.push(e.downstreamTask);
        }
      }
    }
    return this.topo.filter((n) => seen.has(n));
  }

  /** task + all descendants in topo order */
  subtree(name: string): string[] {
    return [name, ...this.descendants(name)];
  }

  /**
   * Resolve the concrete upstream (task, logicalDate) pairs for an instance.
   * offsetDays semantics: an edge with offset -k means instance (d, date) needs
   * upstream partition (u, date-k).
   */
  upstreamPartitions(taskName: string, logicalDate: string, addDays: (d: string, k: number) => string): { task: string; date: string }[] {
    return this.nodes.get(taskName)!.upstreams.map((e) => ({
      task: e.upstreamTask,
      date: addDays(logicalDate, e.offsetDays),
    }));
  }

  get length(): number {
    return this.nodes.size;
  }
}

let cached: { dag: Dag; sig: string } | null = null;

export function loadDag(): Dag {
  const db = getDb();
  const tasks = listTasks();
  const edges = listEdges();
  const sig = `${tasks.length}:${edges.length}:${tasks.map((t) => t.name).join(',')}`;
  if (!cached || cached.sig !== sig) {
    cached = { dag: new Dag(tasks, edges), sig };
  }
  return cached.dag;
}

export function invalidateDagCache(): void {
  cached = null;
}

/** Longest weighted path through the DAG for a single partition (ms). */
export function criticalPathMs(dag: Dag, weight: (taskName: string) => number): number {
  const dist = new Map<string, number>();
  for (const name of dag.topo) {
    const n = dag.nodes.get(name)!;
    let best = 0;
    for (const e of n.upstreams) {
      if (e.offsetDays !== 0) continue; // cross-partition edges belong to another partition's CP
      best = Math.max(best, dist.get(e.upstreamTask) ?? 0);
    }
    dist.set(name, best + weight(name));
  }
  return Math.max(0, ...dist.values());
}
