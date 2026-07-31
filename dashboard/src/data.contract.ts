import { createDemoSnapshot, isDashboardSnapshot } from './data'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`Contract assertion failed: ${message}`)
}

const validSnapshot = createDemoSnapshot()
assert(isDashboardSnapshot(validSnapshot), 'demo snapshot is accepted')

const invalidNode = structuredClone(validSnapshot)
Reflect.set(invalidNode.dags.order_stats.nodes[0], 'kind', 'unknown')
assert(!isDashboardSnapshot(invalidNode), 'unknown DAG node kind is rejected')

const danglingEdge = structuredClone(validSnapshot)
Reflect.set(danglingEdge.dags.order_stats.edges[0], 'target', 'missing-node')
assert(!isDashboardSnapshot(danglingEdge), 'DAG edges must reference existing nodes')

const missingDag = structuredClone(validSnapshot)
delete missingDag.dags.order_stats
assert(!isDashboardSnapshot(missingDag), 'every dataflow must have a DAG')

const invalidRuntime = structuredClone(validSnapshot)
Reflect.set(invalidRuntime.runtime, 'state', 'unknown')
assert(!isDashboardSnapshot(invalidRuntime), 'unknown runtime state is rejected')

const emptySnapshot = structuredClone(validSnapshot)
emptySnapshot.dataflows = []
emptySnapshot.dags = {}
emptySnapshot.pipeline = {
  nodes: emptySnapshot.pipeline.nodes.filter((node) => node.kind !== 'dataflow'),
  edges: emptySnapshot.pipeline.edges.filter((edge) => edge.target !== 'mv-order-stats' && edge.target !== 'mv-customer-rank' && edge.target !== 'mv-product-summary'),
}
assert(isDashboardSnapshot(emptySnapshot), 'empty dataflow snapshots are accepted')

console.log('Dashboard data contract checks passed')
