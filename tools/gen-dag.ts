/**
 * Generate the demo workload:
 *   - exactly 200 tasks in 13 layers (1 source layer, 11 processing layers, 1 terminal layer)
 *   - edges only go from an earlier layer to a later layer => guaranteed DAG, verified afterwards
 *   - a few offset edges (downstream@d depends on upstream@d-1) to exercise cross-partition scheduling
 *   - every task gets a standalone fake executable under data/fake-scripts
 *
 * Run: npm run gen-dag
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { config, } from '../src/config.ts';
import { resetDatabase, ensureRuntimeDirs } from '../src/db.ts';
import { addEdge, upsertTask } from '../src/repo.ts';
import { loadDag, DagCycleError } from '../src/dag.ts';
import { seededRandom } from '../src/util/dates.ts';

const TOTAL_TASKS = 200;
const LAYER_SIZES = [4, 12, 18, 20, 22, 22, 20, 18, 16, 14, 12, 10, 12];
// sums to exactly 200 across 13 layers

interface GenTask {
  name: string;
  layer: number;
  owner: string;
  durationMs: number;
  retries: number;
  flakyRate: number;
  slaMs: number | null;
}

function pick<T>(arr: T[], r: number): T {
  return arr[Math.floor(r * arr.length) % arr.length]!;
}

export function generateDag(): { tasks: GenTask[]; edgeCount: number; offsetEdgeCount: number } {
  if (LAYER_SIZES.reduce((a, b) => a + b, 0) !== TOTAL_TASKS) {
    throw new Error('LAYER_SIZES must sum to TOTAL_TASKS');
  }
  const tasks: GenTask[] = [];
  const owners = ['alice', 'bob', 'carol', 'dave', 'erin', 'frank'];
  let idx = 0;
  for (let layer = 0; layer < LAYER_SIZES.length; layer++) {
    for (let k = 0; k < LAYER_SIZES[layer]!; k++) {
      const r1 = seededRandom(`task-${idx}-dur`);
      const r2 = seededRandom(`task-${idx}-owner`);
      const r3 = seededRandom(`task-${idx}-flaky`);
      const name = `t_${String(idx).padStart(3, '0')}_${layer === 0 ? 'src' : layer === LAYER_SIZES.length - 1 ? 'sink' : 'lyr' + layer}`;
      // Durations are deliberately well above the scheduler tick (~50ms) so the measured
      // critical path reflects task work rather than scheduling granularity, while still
      // being fast enough for a 6800-instance verification to finish in a few minutes.
      const durationMs = 40 + Math.floor(r1 * 120) + (layer === LAYER_SIZES.length - 1 ? 25 : 0);
      const flakyRate = r3 < 0.12 ? 0.25 : 0; // ~12% of tasks are flaky on first attempt, retries make them pass
      // SLA measures execution time from the instance's own RUNNING start.
      // Generous by default (healthy runs never alert); the verify harness tightens a
      // specific task's SLA to deterministically exercise WARNING/BREACHED alerts.
      const slaMs = r2 > 0.55 ? Math.round(durationMs * 200) : null;
      tasks.push({
        name,
        layer,
        owner: pick(owners, r2),
        durationMs,
        retries: 2,
        flakyRate,
        slaMs,
      });
      idx++;
    }
  }

  // layer boundaries
  const bounds: number[] = [0];
  for (const s of LAYER_SIZES) bounds.push(bounds[bounds.length - 1]! + s);
  const at = (layer: number, k: number): GenTask => tasks[bounds[layer]! + k]!;

  const edgeSet = new Set<string>();
  let offsetEdgeCount = 0;
  const add = (u: GenTask, d: GenTask, offset: number) => {
    const key = `${u.name}->${d.name}:${offset}`;
    if (edgeSet.has(key) || u.name === d.name) return;
    edgeSet.add(key);
    if (offset !== 0) offsetEdgeCount++;
  };

  for (let layer = 1; layer < LAYER_SIZES.length; layer++) {
    const size = LAYER_SIZES[layer]!;
    for (let k = 0; k < size; k++) {
      const downstream = at(layer, k);
      const r = seededRandom(`edge-${downstream.name}`);
      // 1..3 parents, mostly from the previous layer; ~15% reach one layer further back
      const parentCount = 1 + Math.floor(r * 3);
      const parents = new Set<GenTask>();
      for (let p = 0; p < parentCount; p++) {
        const rp = seededRandom(`edge-${downstream.name}-${p}`);
        const srcLayer = layer >= 2 && rp > 0.85 ? layer - 2 : layer - 1;
        const parent = at(srcLayer, Math.floor(rp * LAYER_SIZES[srcLayer]!));
        parents.add(parent);
      }
      for (const parent of parents) {
        const ro = seededRandom(`offset-${parent.name}-${downstream.name}`);
        // ~4% of edges are cross-partition dependencies (offset -1), only past layer 1
        const offset = layer > 1 && ro > 0.96 ? -1 : 0;
        add(parent, downstream, offset);
      }
    }
  }

  // guarantee connectivity: every non-source node has at least one parent, every non-sink has a child
  const incoming = new Set<string>();
  const outgoing = new Set<string>();
  for (const key of edgeSet) {
    const [pair] = key.split(':');
    const [up, down] = pair!.split('->');
    outgoing.add(up!);
    incoming.add(down!);
  }
  for (let layer = 1; layer < LAYER_SIZES.length; layer++) {
    for (let k = 0; k < LAYER_SIZES[layer]!; k++) {
      const node = at(layer, k);
      if (!incoming.has(node.name)) add(at(layer - 1, k % LAYER_SIZES[layer - 1]!), node, 0);
    }
  }
  for (let layer = 0; layer < LAYER_SIZES.length - 1; layer++) {
    for (let k = 0; k < LAYER_SIZES[layer]!; k++) {
      const node = at(layer, k);
      if (!outgoing.has(node.name)) add(node, at(layer + 1, k % LAYER_SIZES[layer + 1]!), 0);
    }
  }

  // persist
  resetDatabase();
  ensureRuntimeDirs();
  fs.mkdirSync(config.scriptsDir, { recursive: true });
  const helperRel = path.relative(config.scriptsDir, path.join(config.workspaceRoot, 'scripts', 'fake-task-helper.cjs'));

  for (const t of tasks) {
    const scriptPath = path.join(config.scriptsDir, `${t.name}.cjs`);
    fs.writeFileSync(
      scriptPath,
      `// generated fake task — standalone executable, protocol implemented in shared helper\nrequire(${JSON.stringify(helperRel)});\n`,
    );
    upsertTask({
      name: t.name,
      owner: t.owner,
      command: `node fake-scripts/${t.name}.cjs {date} ${t.durationMs} ${t.flakyRate}`,
      retries: t.retries,
      retryBackoffMs: 200,
      expectedDurationMs: t.durationMs,
      slaMs: t.slaMs,
      slaWarnRatio: 0.8,
    });
  }

  for (const key of edgeSet) {
    const [pair, offsetStr] = key.split(':');
    const [up, down] = pair!.split('->');
    addEdge(up!, down!, Number(offsetStr));
  }

  // verify: topo sort must succeed
  try {
    const dag = loadDag();
    if (dag.length !== TOTAL_TASKS) throw new Error(`task count mismatch: ${dag.length}`);
    return { tasks, edgeCount: edgeSet.size, offsetEdgeCount };
  } catch (err) {
    if (err instanceof DagCycleError) throw err;
    throw err;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const result = generateDag();
  console.log(`[gen-dag] tasks=${result.tasks.length} edges=${result.edgeCount} offsetEdges=${result.offsetEdgeCount}`);
  console.log(`[gen-dag] layers=${LAYER_SIZES.length} sizes=${LAYER_SIZES.join(',')}`);
  console.log(`[gen-dag] db=${path.resolve(config.dbPath)}`);
  console.log(`[gen-dag] scripts=${path.resolve(config.scriptsDir)}`);
}
