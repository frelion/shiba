import type {
  DagEdge,
  DagNode,
  DashboardSnapshot,
  DataflowDag,
  DataflowSummary,
  DagNodeKind,
  PipelineEdge,
  PipelineGraph,
  PipelineNodeKind,
  PipelineNode,
  RuntimeSnapshot,
  StatusTone,
} from './types'

const configuredApiPath = import.meta.env.VITE_OBSERVABILITY_API
const API_PATH = typeof configuredApiPath === 'string' && configuredApiPath.trim().length > 0
  ? configuredApiPath.trim()
  : '/api/observability/snapshot'

const configuredDemoFallback = import.meta.env.VITE_ENABLE_DEMO_FALLBACK
export const DEMO_FALLBACK_ENABLED = import.meta.env.DEV || configuredDemoFallback === 'true'

const now = new Date()

function minutesAgo(minutes: number): string {
  return new Date(now.getTime() - minutes * 60_000).toISOString()
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isString(value: unknown): value is string {
  return typeof value === 'string'
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function isNonNegativeNumber(value: unknown): value is number {
  return isFiniteNumber(value) && value >= 0
}

function isNonNegativeInteger(value: unknown): value is number {
  return isNonNegativeNumber(value) && Number.isInteger(value)
}

function isTimestamp(value: unknown): value is string {
  return isString(value) && !Number.isNaN(Date.parse(value))
}

function isStatusTone(value: unknown): value is StatusTone {
  return value === 'healthy' || value === 'active' || value === 'warning' || value === 'danger' || value === 'idle'
}

function isRuntimeState(value: unknown): value is RuntimeSnapshot['state'] {
  return value === 'running' || value === 'starting' || value === 'stale' || value === 'missing' || value === 'inactive'
}

function isPipelineNodeKind(value: unknown): value is PipelineNodeKind {
  return value === 'wal' || value === 'ingress' || value === 'stream' || value === 'dataflow'
}

function isDagNodeKind(value: unknown): value is DagNodeKind {
  return value === 'source' || value === 'operator' || value === 'sink'
}

function hasUniqueIds(values: Array<{ id: string }>): boolean {
  return new Set(values.map((value) => value.id)).size === values.length
}

function isRuntimeSnapshot(value: unknown): value is RuntimeSnapshot {
  return isRecord(value)
    && isRuntimeState(value.state)
    && isNonNegativeNumber(value.heartbeatAgeSeconds)
    && isNonNegativeNumber(value.pendingWalBytes)
    && isNonNegativeNumber(value.retainedWalBytes)
    && isString(value.persistedLsn)
    && isString(value.publishedLsn)
    && isString(value.appliedLsn)
    && typeof value.slotActive === 'boolean'
    && isNonNegativeInteger(value.backpressuredStreams)
    && isTimestamp(value.observedAt)
}

function isPipelineNode(value: unknown): value is PipelineNode {
  return isRecord(value)
    && isString(value.id)
    && isString(value.label)
    && isString(value.subtitle)
    && isPipelineNodeKind(value.kind)
    && isStatusTone(value.tone)
    && isString(value.metric)
}

function isPipelineEdge(value: unknown): value is PipelineEdge {
  return isRecord(value)
    && isString(value.id)
    && isString(value.source)
    && isString(value.target)
    && isString(value.label)
    && isStatusTone(value.tone)
}

function isPipelineGraph(value: unknown): value is PipelineGraph {
  if (!isRecord(value) || !Array.isArray(value.nodes) || !Array.isArray(value.edges)) return false
  const nodes = value.nodes
  const edges = value.edges
  if (!nodes.every(isPipelineNode) || !edges.every(isPipelineEdge) || !hasUniqueIds(nodes) || !hasUniqueIds(edges)) return false
  const nodeIds = new Set(nodes.map((node) => node.id))
  return edges.every((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target) && edge.source !== edge.target)
}

function isDataflowSummary(value: unknown): value is DataflowSummary {
  return isRecord(value)
    && isString(value.id)
    && isString(value.name)
    && Array.isArray(value.sources)
    && value.sources.every(isString)
    && isStatusTone(value.status)
    && isNonNegativeNumber(value.lagSeconds)
    && isNonNegativeInteger(value.pendingChunks)
    && isNonNegativeNumber(value.outputBytes)
    && typeof value.backpressured === 'boolean'
    && isNonNegativeInteger(value.stageCount)
    && isNonNegativeInteger(value.readyStages)
    && value.readyStages <= value.stageCount
    && isTimestamp(value.lastProgress)
}

function isDagNodeMetrics(value: unknown): value is DagNode['metrics'] {
  return isRecord(value)
    && isNonNegativeInteger(value.pendingChunks)
    && isNonNegativeNumber(value.bufferedBytes)
    && isNonNegativeInteger(value.checkpointRevision)
    && isTimestamp(value.lastUpdated)
}

function isDagNode(value: unknown): value is DagNode {
  return isRecord(value)
    && isString(value.id)
    && isString(value.label)
    && isDagNodeKind(value.kind)
    && isStatusTone(value.tone)
    && isNonNegativeNumber(value.x)
    && isNonNegativeNumber(value.y)
    && isDagNodeMetrics(value.metrics)
    && (value.operator === undefined || isString(value.operator))
    && (value.stageId === undefined || isNonNegativeInteger(value.stageId))
}

function isDagEdge(value: unknown): value is DagEdge {
  return isRecord(value)
    && isString(value.id)
    && isString(value.source)
    && isString(value.target)
    && isString(value.label)
    && isStatusTone(value.tone)
    && typeof value.backpressured === 'boolean'
}

function isDataflowDag(value: unknown): value is DataflowDag {
  if (!isRecord(value) || !isString(value.resultTable) || !Array.isArray(value.nodes) || !Array.isArray(value.edges)) return false
  const nodes = value.nodes
  const edges = value.edges
  if (!nodes.every(isDagNode) || !edges.every(isDagEdge) || !hasUniqueIds(nodes) || !hasUniqueIds(edges)) return false
  const nodeIds = new Set(nodes.map((node) => node.id))
  return edges.every((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target) && edge.source !== edge.target)
}

function isAlertItem(value: unknown): value is DashboardSnapshot['alerts'][number] {
  return isRecord(value)
    && isString(value.id)
    && (value.severity === 'info' || value.severity === 'warning' || value.severity === 'critical')
    && isString(value.title)
    && isString(value.description)
    && isString(value.time)
}

function isHistory(value: unknown): value is DashboardSnapshot['history'] {
  return isRecord(value)
    && Array.isArray(value.pendingWal)
    && value.pendingWal.every(isFiniteNumber)
    && Array.isArray(value.appliedLag)
    && value.appliedLag.every(isFiniteNumber)
    && Array.isArray(value.bufferedBytes)
    && value.bufferedBytes.every(isFiniteNumber)
}

export function isDashboardSnapshot(value: unknown): value is DashboardSnapshot {
  if (!isRecord(value)
    || (value.source !== 'live' && value.source !== 'demo')
    || !isTimestamp(value.observedAt)
    || !isRuntimeSnapshot(value.runtime)
    || !isPipelineGraph(value.pipeline)
    || !Array.isArray(value.dataflows)
    || !Array.isArray(value.alerts)
    || !isRecord(value.dags)
    || !isHistory(value.history)) {
    return false
  }

  const dataflows = value.dataflows
  const dags = value.dags
  if (!dataflows.every(isDataflowSummary) || !hasUniqueIds(dataflows)) return false
  if (!value.alerts.every(isAlertItem) || !hasUniqueIds(value.alerts)) return false
  if (Object.keys(dags).length !== dataflows.length) return false
  if (!Object.values(dags).every(isDataflowDag)) return false

  return dataflows.every((dataflow) => {
    if (!Object.prototype.hasOwnProperty.call(dags, dataflow.id)) return false
    return isDataflowDag(dags[dataflow.id])
  })
}

export async function fetchSnapshot(signal?: AbortSignal): Promise<DashboardSnapshot> {
  const response = await fetch(API_PATH, {
    headers: { Accept: 'application/json' },
    signal,
  })

  if (!response.ok) {
    throw new Error(`Observation API returned ${response.status}`)
  }

  const payload: unknown = await response.json()
  if (!isDashboardSnapshot(payload)) {
    throw new TypeError('Observation API returned an invalid snapshot')
  }

  return { ...payload, source: 'live', feedError: undefined }
}

function pipelineFixture(): PipelineGraph {
  const nodes: PipelineNode[] = [
    { id: 'wal', label: 'PostgreSQL WAL', subtitle: 'logical stream', kind: 'wal', tone: 'healthy', metric: '0/2A01' },
    { id: 'ingress', label: 'Ingress', subtitle: 'bounded decode', kind: 'ingress', tone: 'active', metric: '1.2k rows/s' },
    { id: 'streams', label: 'Source streams', subtitle: 'fanout + frontier', kind: 'stream', tone: 'healthy', metric: '8.4 MB buffered' },
    { id: 'mv-order-stats', label: 'order_stats', subtitle: '4 stages · healthy', kind: 'dataflow', tone: 'active', metric: '2.4 s lag' },
    { id: 'mv-customer-rank', label: 'customer_rank', subtitle: '5 stages · backpressure', kind: 'dataflow', tone: 'warning', metric: '18.2 s lag' },
    { id: 'mv-product-summary', label: 'product_summary', subtitle: '3 stages · healthy', kind: 'dataflow', tone: 'healthy', metric: '0.8 s lag' },
  ]

  const edges: PipelineEdge[] = [
    { id: 'wal-ingress', source: 'wal', target: 'ingress', label: '12.6 MB/s', tone: 'healthy' },
    { id: 'ingress-streams', source: 'ingress', target: 'streams', label: '1,248 rows/s', tone: 'active' },
    { id: 'streams-order', source: 'streams', target: 'mv-order-stats', label: '42 chunks', tone: 'healthy' },
    { id: 'streams-customer', source: 'streams', target: 'mv-customer-rank', label: '311 chunks', tone: 'warning' },
    { id: 'streams-product', source: 'streams', target: 'mv-product-summary', label: '18 chunks', tone: 'healthy' },
  ]

  return { nodes, edges }
}

function dagNode(
  id: string,
  label: string,
  kind: DagNode['kind'],
  x: number,
  y: number,
  tone: StatusTone,
  metrics: DagNode['metrics'],
  operator?: string,
  stageId?: number,
): DagNode {
  return { id, label, kind, x, y, tone, metrics, operator, stageId }
}

function dagEdge(id: string, source: string, target: string, label: string, tone: StatusTone, backpressured = false): DagEdge {
  return { id, source, target, label, tone, backpressured }
}

function orderStatsDag(): DataflowDag {
  const nodes = [
    dagNode('orders', 'public.orders', 'source', 30, 135, 'healthy', { pendingChunks: 0, bufferedBytes: 0, checkpointRevision: 0, lastUpdated: minutesAgo(0) }),
    dagNode('products', 'public.products', 'source', 30, 305, 'healthy', { pendingChunks: 0, bufferedBytes: 0, checkpointRevision: 0, lastUpdated: minutesAgo(0) }),
    dagNode('scan-orders', 'Scan', 'operator', 280, 92, 'active', { pendingChunks: 42, bufferedBytes: 1_800_000, checkpointRevision: 1842, lastUpdated: minutesAgo(0.2) }, 'scan', 0),
    dagNode('scan-products', 'Scan', 'operator', 280, 348, 'healthy', { pendingChunks: 8, bufferedBytes: 340_000, checkpointRevision: 923, lastUpdated: minutesAgo(0.4) }, 'scan', 1),
    dagNode('join', 'Join', 'operator', 550, 218, 'active', { pendingChunks: 13, bufferedBytes: 2_100_000, checkpointRevision: 1644, lastUpdated: minutesAgo(0.4) }, 'join', 2),
    dagNode('aggregate', 'Aggregate', 'operator', 820, 218, 'warning', { pendingChunks: 86, bufferedBytes: 4_900_000, checkpointRevision: 802, lastUpdated: minutesAgo(2.1) }, 'aggregate', 3),
    dagNode('sink', 'order_stats', 'sink', 1090, 218, 'active', { pendingChunks: 86, bufferedBytes: 0, checkpointRevision: 802, lastUpdated: minutesAgo(2.1) }, 'sink', 4),
  ]

  const edges = [
    dagEdge('orders-scan', 'orders', 'scan-orders', '42 chunks', 'active'),
    dagEdge('products-scan', 'products', 'scan-products', '8 chunks', 'healthy'),
    dagEdge('scan-join-a', 'scan-orders', 'join', '1.8 MB', 'active'),
    dagEdge('scan-join-b', 'scan-products', 'join', '340 KB', 'healthy'),
    dagEdge('join-aggregate', 'join', 'aggregate', '2.1 MB', 'warning'),
    dagEdge('aggregate-sink', 'aggregate', 'sink', '86 chunks', 'danger', true),
  ]

  return { resultTable: 'shiba.order_stats', nodes, edges }
}

function customerRankDag(): DataflowDag {
  const nodes = [
    dagNode('customers', 'public.customers', 'source', 30, 175, 'healthy', { pendingChunks: 0, bufferedBytes: 0, checkpointRevision: 0, lastUpdated: minutesAgo(0) }),
    dagNode('orders-rank', 'public.orders', 'source', 30, 360, 'healthy', { pendingChunks: 0, bufferedBytes: 0, checkpointRevision: 0, lastUpdated: minutesAgo(0) }),
    dagNode('customer-scan', 'Scan', 'operator', 280, 150, 'active', { pendingChunks: 311, bufferedBytes: 5_800_000, checkpointRevision: 2410, lastUpdated: minutesAgo(6.4) }, 'scan', 0),
    dagNode('order-scan', 'Scan', 'operator', 280, 385, 'warning', { pendingChunks: 520, bufferedBytes: 9_300_000, checkpointRevision: 2312, lastUpdated: minutesAgo(7.2) }, 'scan', 1),
    dagNode('join-rank', 'Join', 'operator', 550, 265, 'warning', { pendingChunks: 311, bufferedBytes: 15_100_000, checkpointRevision: 1910, lastUpdated: minutesAgo(6.8) }, 'join', 2),
    dagNode('window-rank', 'Window', 'operator', 820, 265, 'danger', { pendingChunks: 311, bufferedBytes: 0, checkpointRevision: 1844, lastUpdated: minutesAgo(18.2) }, 'window', 3),
    dagNode('rank-sink', 'customer_rank', 'sink', 1090, 265, 'danger', { pendingChunks: 311, bufferedBytes: 0, checkpointRevision: 1844, lastUpdated: minutesAgo(18.2) }, 'sink', 4),
  ]

  const edges = [
    dagEdge('customers-scan', 'customers', 'customer-scan', '112 chunks', 'warning'),
    dagEdge('orders-rank-scan', 'orders-rank', 'order-scan', '199 chunks', 'warning'),
    dagEdge('customer-join', 'customer-scan', 'join-rank', '5.8 MB', 'warning'),
    dagEdge('order-join', 'order-scan', 'join-rank', '9.3 MB', 'warning'),
    dagEdge('join-window', 'join-rank', 'window-rank', '15.1 MB', 'danger', true),
    dagEdge('window-sink', 'window-rank', 'rank-sink', 'stalled 18.2 s', 'danger', true),
  ]

  return { resultTable: 'shiba.customer_rank', nodes, edges }
}

function productSummaryDag(): DataflowDag {
  const nodes = [
    dagNode('product-source', 'public.products', 'source', 30, 220, 'healthy', { pendingChunks: 0, bufferedBytes: 0, checkpointRevision: 0, lastUpdated: minutesAgo(0) }),
    dagNode('product-scan', 'Scan', 'operator', 320, 220, 'active', { pendingChunks: 18, bufferedBytes: 700_000, checkpointRevision: 921, lastUpdated: minutesAgo(0.6) }, 'scan', 0),
    dagNode('product-aggregate', 'Aggregate', 'operator', 650, 220, 'active', { pendingChunks: 18, bufferedBytes: 700_000, checkpointRevision: 788, lastUpdated: minutesAgo(0.8) }, 'aggregate', 1),
    dagNode('product-sink', 'product_summary', 'sink', 980, 220, 'healthy', { pendingChunks: 18, bufferedBytes: 0, checkpointRevision: 788, lastUpdated: minutesAgo(0.8) }, 'sink', 2),
  ]

  const edges = [
    dagEdge('product-source-scan', 'product-source', 'product-scan', '18 chunks', 'active'),
    dagEdge('product-scan-aggregate', 'product-scan', 'product-aggregate', '700 KB', 'active'),
    dagEdge('product-aggregate-sink', 'product-aggregate', 'product-sink', '18 chunks', 'healthy'),
  ]

  return { resultTable: 'shiba.product_summary', nodes, edges }
}

function dataflowFixtures(): DataflowSummary[] {
  return [
    { id: 'order_stats', name: 'shiba.order_stats', sources: ['public.orders', 'public.products'], status: 'active', lagSeconds: 2.4, pendingChunks: 86, outputBytes: 4_900_000, backpressured: false, stageCount: 5, readyStages: 3, lastProgress: minutesAgo(2.1) },
    { id: 'customer_rank', name: 'shiba.customer_rank', sources: ['public.customers', 'public.orders'], status: 'danger', lagSeconds: 18.2, pendingChunks: 311, outputBytes: 15_100_000, backpressured: true, stageCount: 5, readyStages: 1, lastProgress: minutesAgo(18.2) },
    { id: 'product_summary', name: 'shiba.product_summary', sources: ['public.products'], status: 'healthy', lagSeconds: 0.8, pendingChunks: 18, outputBytes: 700_000, backpressured: false, stageCount: 3, readyStages: 2, lastProgress: minutesAgo(0.8) },
  ]
}

export function createDemoSnapshot(): DashboardSnapshot {
  const runtime: RuntimeSnapshot = {
    state: 'running', heartbeatAgeSeconds: 1.4, pendingWalBytes: 18_400_000, retainedWalBytes: 134_200_000,
    persistedLsn: '0/2A01B8C0', publishedLsn: '0/2A00E4A0', appliedLsn: '0/29F8D2A0', slotActive: true,
    backpressuredStreams: 1, observedAt: now.toISOString(),
  }
  const dags = {
    order_stats: orderStatsDag(),
    customer_rank: customerRankDag(),
    product_summary: productSummaryDag(),
  }

  return {
    source: 'demo',
    observedAt: now.toISOString(),
    runtime,
    pipeline: pipelineFixture(),
    dataflows: dataflowFixtures(),
    dags,
    alerts: [
      { id: 'alert-backpressure', severity: 'warning', title: 'customer_rank output backpressure', description: 'Window stage has not advanced its Sink frontier for 18.2 seconds.', time: '18 sec ago' },
      { id: 'alert-wal', severity: 'info', title: 'WAL retention is elevated', description: 'Logical slot is retaining 134 MB. Current threshold is 256 MB.', time: '2 min ago' },
      { id: 'alert-recovery', severity: 'info', title: 'Runtime recovered cleanly', description: 'Last owner handoff completed without an incomplete checkpoint.', time: '14 min ago' },
    ],
    history: {
      pendingWal: [12, 18, 15, 23, 19, 26, 22, 31, 28, 34, 29, 36],
      appliedLag: [4, 4, 5, 4, 6, 7, 6, 8, 11, 12, 16, 18],
      bufferedBytes: [4, 5, 4, 7, 6, 8, 7, 9, 11, 12, 14, 15],
    },
  }
}

export function withDemoFallback(error: unknown): DashboardSnapshot {
  const fallback = createDemoSnapshot()
  return {
    ...fallback,
    feedError: error instanceof Error ? error.message : 'Observation API is unavailable',
  }
}
