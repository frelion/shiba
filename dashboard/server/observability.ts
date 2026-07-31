import type {
  AlertItem,
  DagNode,
  DashboardSnapshot,
  DataflowDag,
  DataflowSummary,
  PipelineGraph,
  RuntimeSnapshot,
  StatusTone,
} from '../src/types'

export type DbRow = Record<string, unknown>

export interface SnapshotRows {
  runtime?: DbRow
  dataflows: DbRow[]
  dags: DbRow[]
  sourceNames: Map<number, string>
}

function isRecord(value: unknown): value is DbRow {
  return typeof value === 'object' && value !== null
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : value === null || value === undefined ? fallback : String(value)
}

function asNumber(value: unknown, fallback = 0): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'bigint') {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : fallback
  }
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return fallback
}

function asInteger(value: unknown, fallback = 0): number {
  if (value === undefined || value === null || value === '') return fallback
  const parsed = asNumber(value, Number.NaN)
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : fallback
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === 'boolean' ? value : fallback
}

function asTimestamp(value: unknown, fallback: Date): string {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString()
  if (typeof value === 'string') {
    const parsed = new Date(value)
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString()
  }
  return fallback.toISOString()
}

function asJson(value: unknown): unknown {
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value) as unknown
  } catch {
    return undefined
  }
}

function arrayValue(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function objectValue(value: unknown): DbRow {
  const parsed = asJson(value)
  return isRecord(parsed) ? parsed : {}
}

function operatorName(value: unknown): string {
  const name = asString(value, 'operator')
  return name.length > 0 ? name : 'operator'
}

function displayOperator(value: string): string {
  return value.length === 0 ? 'Operator' : value[0].toUpperCase() + value.slice(1)
}

function runtimeState(value: unknown): RuntimeSnapshot['state'] {
  return value === 'running' || value === 'starting' || value === 'stale' || value === 'missing' || value === 'inactive'
    ? value
    : 'missing'
}

function formatBytes(value: number): string {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)} GB`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`
  return `${Math.round(value)} B`
}

function relativeTime(value: string, now: Date): string {
  const seconds = Math.max(0, Math.round((now.getTime() - new Date(value).getTime()) / 1_000))
  if (seconds < 60) return `${seconds} sec ago`
  return `${Math.floor(seconds / 60)} min ago`
}

function statusForDataflow(active: boolean, lagSeconds: number, backpressured: boolean): StatusTone {
  if (!active) return 'idle'
  if (backpressured || lagSeconds >= 30) return 'danger'
  if (lagSeconds >= 10) return 'warning'
  return lagSeconds > 0 ? 'active' : 'healthy'
}

function planFromExplanation(value: unknown): DbRow {
  return objectValue(objectValue(value).plan)
}

function stagesFromExplanation(value: unknown): DbRow[] {
  return arrayValue(planFromExplanation(value).stages).filter(isRecord)
}

export function extractSourceOids(explanation: unknown): number[] {
  const sourceOids = new Set<number>()
  for (const stage of stagesFromExplanation(explanation)) {
    const spec = objectValue(stage.spec)
    if (operatorName(spec.operator) !== 'scan') continue
    const sourceOid = asInteger(objectValue(spec.config).source_oid, -1)
    if (sourceOid > 0) sourceOids.add(sourceOid)
  }
  return [...sourceOids]
}

function operatorMetrics(explanation: unknown): Map<number, DbRow> {
  const metrics = new Map<number, DbRow>()
  for (const value of arrayValue(objectValue(explanation).operators)) {
    if (!isRecord(value)) continue
    const stageId = asInteger(value.stage_id, -1)
    if (stageId >= 0) metrics.set(stageId, value)
  }
  return metrics
}

function stageInputs(stage: DbRow): number[] {
  return arrayValue(stage.inputs)
    .filter(isRecord)
    .map((input) => asInteger(input.upstream_stage_id, -1))
    .filter((stageId) => stageId >= 0)
}

function stageMetric(metrics: DbRow | undefined): { checkpointRevision: number; hasContinuation: boolean; backpressured: boolean } {
  const output = objectValue(metrics?.output)
  return {
    checkpointRevision: asInteger(metrics?.checkpoint_revision),
    hasContinuation: asBoolean(metrics?.has_continuation),
    backpressured: asBoolean(output.backpressured),
  }
}

function buildDag(row: DbRow, dataflow: DataflowSummary, sourceNames: Map<number, string>, observedAt: Date): DataflowDag {
  const resultTable = asString(row.result_table, dataflow.name)
  const stages = stagesFromExplanation(row.explanation)
  if (stages.length === 0) throw new Error(`No stages found for ${resultTable}`)

  const metricsByStage = operatorMetrics(row.explanation)
  const nodes: DagNode[] = []
  const edges: DataflowDag['edges'] = []
  const stageNodeByIndex = new Map<number, DagNode>()
  const sourceCountByName = new Map<string, number>()

  for (const [stageId, stage] of stages.entries()) {
    const spec = objectValue(stage.spec)
    const operator = operatorName(spec.operator)
    const metrics = stageMetric(metricsByStage.get(stageId))
    const tone: StatusTone = metrics.backpressured ? 'danger' : metrics.hasContinuation ? 'warning' : 'active'
    const node: DagNode = {
      id: `stage-${stageId}`,
      label: displayOperator(operator),
      kind: operator === 'sink' ? 'sink' : 'operator',
      operator,
      stageId,
      tone,
      x: 280 + (stageId % 5) * 250,
      y: 80 + Math.floor(stageId / 5) * 130,
      metrics: {
        pendingChunks: metrics.hasContinuation ? dataflow.pendingChunks : 0,
        bufferedBytes: metrics.backpressured ? dataflow.outputBytes : 0,
        checkpointRevision: metrics.checkpointRevision,
        lastUpdated: asTimestamp(row.last_stage_update, observedAt),
      },
    }
    nodes.push(node)
    stageNodeByIndex.set(stageId, node)

    if (operator === 'scan') {
      const sourceOid = asInteger(objectValue(spec.config).source_oid, -1)
      const sourceName = sourceNames.get(sourceOid) ?? (sourceOid > 0 ? `source oid ${sourceOid}` : 'source stream')
      const occurrence = sourceCountByName.get(sourceName) ?? 0
      sourceCountByName.set(sourceName, occurrence + 1)
      const sourceNode: DagNode = {
        id: `source-${stageId}-${sourceOid > 0 ? sourceOid : occurrence}`,
        label: sourceName,
        kind: 'source',
        tone: 'healthy',
        x: 30,
        y: 80 + nodes.filter((item) => item.kind === 'source').length * 130,
        metrics: {
          pendingChunks: 0,
          bufferedBytes: 0,
          checkpointRevision: 0,
          lastUpdated: asTimestamp(row.last_stage_update, observedAt),
        },
      }
      nodes.push(sourceNode)
      edges.push({ id: `${sourceNode.id}-stage-${stageId}`, source: sourceNode.id, target: node.id, label: 'source stream', tone, backpressured: false })
    }
  }

  for (const [stageId, stage] of stages.entries()) {
    const target = stageNodeByIndex.get(stageId)
    if (!target) continue
    for (const [inputIndex, upstreamStageId] of stageInputs(stage).entries()) {
      const source = stageNodeByIndex.get(upstreamStageId)
      if (!source) continue
      const metrics = stageMetric(metricsByStage.get(stageId))
      edges.push({
        id: `${source.id}-${target.id}-${inputIndex}`,
        source: source.id,
        target: target.id,
        label: metrics.hasContinuation ? `${dataflow.pendingChunks} chunks` : 'stage output',
        tone: metrics.backpressured ? 'danger' : metrics.hasContinuation ? 'warning' : 'healthy',
        backpressured: metrics.backpressured,
      })
    }
  }

  return { resultTable, nodes, edges }
}

function summaryFromRow(row: DbRow, dag: DataflowDag, observedAt: Date): DataflowSummary {
  const lastProgress = asTimestamp(row.last_stage_update, observedAt)
  const lagSeconds = Math.max(0, (observedAt.getTime() - new Date(lastProgress).getTime()) / 1_000)
  const pendingChunks = asNumber(row.pending_input_chunks) + asNumber(row.buffered_output_chunks)
  const backpressured = asInteger(row.backpressured_output_streams) > 0
  const active = asBoolean(row.active, true)
  const stageCount = asInteger(row.stage_count)
  return {
    id: asString(row.result_table),
    name: asString(row.result_table),
    sources: dag.nodes.filter((node) => node.kind === 'source').map((node) => node.label),
    status: statusForDataflow(active, lagSeconds, backpressured),
    lagSeconds,
    pendingChunks,
    outputBytes: asNumber(row.buffered_output_bytes),
    backpressured,
    stageCount,
    readyStages: Math.min(stageCount, asInteger(row.ready_stage_count)),
    lastProgress,
  }
}

function buildPipeline(runtime: RuntimeSnapshot, dataflows: DataflowSummary[]): PipelineGraph {
  const nodes: PipelineGraph['nodes'] = [
    { id: 'wal', label: 'PostgreSQL WAL', subtitle: 'logical stream', kind: 'wal', tone: runtime.slotActive ? 'healthy' : 'danger', metric: `${formatBytes(runtime.pendingWalBytes)} pending` },
    { id: 'ingress', label: 'Ingress', subtitle: 'bounded decode', kind: 'ingress', tone: runtime.state === 'running' ? 'active' : 'danger', metric: `${runtime.heartbeatAgeSeconds.toFixed(1)}s heartbeat` },
    { id: 'streams', label: 'Source streams', subtitle: 'fanout + frontier', kind: 'stream', tone: runtime.backpressuredStreams > 0 ? 'warning' : 'healthy', metric: `${formatBytes(runtime.pendingWalBytes)} retained` },
    ...dataflows.map((dataflow) => ({
      id: `mv-${dataflow.id}`,
      label: dataflow.name,
      subtitle: `${dataflow.stageCount} stages · ${dataflow.status}`,
      kind: 'dataflow' as const,
      tone: dataflow.status,
      metric: `${dataflow.lagSeconds.toFixed(1)}s lag`,
    })),
  ]
  const edges: PipelineGraph['edges'] = [
    { id: 'wal-ingress', source: 'wal', target: 'ingress', label: 'WAL', tone: runtime.slotActive ? 'healthy' : 'danger' },
    { id: 'ingress-streams', source: 'ingress', target: 'streams', label: 'decode', tone: runtime.state === 'running' ? 'active' : 'danger' },
    ...dataflows.map((dataflow) => ({
      id: `streams-${dataflow.id}`,
      source: 'streams',
      target: `mv-${dataflow.id}`,
      label: `${dataflow.pendingChunks} chunks`,
      tone: dataflow.status,
    })),
  ]
  return { nodes, edges }
}

function buildAlerts(runtime: RuntimeSnapshot, dataflows: DataflowSummary[], now: Date): AlertItem[] {
  const alerts: AlertItem[] = []
  if (runtime.state !== 'running') {
    alerts.push({ id: 'runtime-state', severity: 'critical', title: `Runtime is ${runtime.state}`, description: 'The Shiba worker is not reporting a healthy heartbeat.', time: relativeTime(runtime.observedAt, now) })
  }
  if (runtime.pendingWalBytes >= 256 * 1_000_000) {
    alerts.push({ id: 'wal-retention', severity: 'warning', title: 'WAL retention is elevated', description: `${formatBytes(runtime.pendingWalBytes)} of WAL is waiting behind the observation frontier.`, time: relativeTime(runtime.observedAt, now) })
  }
  for (const dataflow of dataflows.filter((item) => item.backpressured || item.status === 'danger')) {
    alerts.push({ id: `dataflow-${dataflow.id}`, severity: dataflow.status === 'danger' ? 'critical' : 'warning', title: `${dataflow.name} needs attention`, description: `${dataflow.pendingChunks} chunks are pending with ${dataflow.lagSeconds.toFixed(1)}s applied lag.`, time: relativeTime(dataflow.lastProgress, now) })
  }
  return alerts
}

export class SnapshotHistory {
  private readonly pendingWal: number[] = []
  private readonly appliedLag: number[] = []
  private readonly bufferedBytes: number[] = []

  constructor(private readonly limit: number) {}

  append(pendingWal: number, appliedLag: number, bufferedBytes: number): void {
    this.pendingWal.push(pendingWal)
    this.appliedLag.push(appliedLag)
    this.bufferedBytes.push(bufferedBytes)
    while (this.pendingWal.length > this.limit) this.pendingWal.shift()
    while (this.appliedLag.length > this.limit) this.appliedLag.shift()
    while (this.bufferedBytes.length > this.limit) this.bufferedBytes.shift()
  }

  snapshot(): DashboardSnapshot['history'] {
    return {
      pendingWal: [...this.pendingWal],
      appliedLag: [...this.appliedLag],
      bufferedBytes: [...this.bufferedBytes],
    }
  }
}

export function buildSnapshot(rows: SnapshotRows, history: SnapshotHistory, observedAt = new Date()): DashboardSnapshot {
  if (!rows.runtime) throw new Error('runtime_status returned no row')

  const runtime: RuntimeSnapshot = {
    state: runtimeState(rows.runtime.worker_state),
    heartbeatAgeSeconds: asNumber(rows.runtime.heartbeat_age_seconds),
    pendingWalBytes: asNumber(rows.runtime.pending_wal_bytes),
    retainedWalBytes: asNumber(rows.runtime.slot_retained_wal_bytes),
    persistedLsn: asString(rows.runtime.persisted_lsn),
    publishedLsn: asString(rows.runtime.published_lsn),
    appliedLsn: asString(rows.runtime.replay_safe_lsn, asString(rows.runtime.confirmed_lsn)),
    slotActive: asBoolean(rows.runtime.slot_active),
    backpressuredStreams: asInteger(rows.runtime.backpressured_streams),
    observedAt: asTimestamp(rows.runtime.observed_at, observedAt),
  }

  const dagRowsByResult = new Map(rows.dags.map((row) => [asString(row.result_table), row]))
  const dags: Record<string, DataflowDag> = {}
  const dataflows: DataflowSummary[] = []
  for (const row of rows.dataflows) {
    const resultTable = asString(row.result_table)
    const dagRow = dagRowsByResult.get(resultTable)
    if (!dagRow) throw new Error(`explain_dataflow returned no graph for ${resultTable}`)
    const stageCount = asInteger(row.stage_count)
    const placeholder: DataflowSummary = {
      id: resultTable,
      name: resultTable,
      sources: [],
      status: 'idle',
      lagSeconds: 0,
      pendingChunks: asNumber(row.pending_input_chunks) + asNumber(row.buffered_output_chunks),
      outputBytes: asNumber(row.buffered_output_bytes),
      backpressured: asInteger(row.backpressured_output_streams) > 0,
      stageCount,
      readyStages: Math.min(stageCount, asInteger(row.ready_stage_count)),
      lastProgress: asTimestamp(row.last_stage_update, observedAt),
    }
    const dag = buildDag(dagRow, placeholder, rows.sourceNames, observedAt)
    const summary = summaryFromRow(row, dag, observedAt)
    dags[summary.id] = dag
    dataflows.push(summary)
  }

  const maxLag = dataflows.reduce((maximum, dataflow) => Math.max(maximum, dataflow.lagSeconds), 0)
  history.append(runtime.pendingWalBytes, maxLag, dataflows.reduce((total, dataflow) => total + dataflow.outputBytes, 0))

  return {
    source: 'live',
    observedAt: runtime.observedAt,
    runtime,
    pipeline: buildPipeline(runtime, dataflows),
    dataflows,
    dags,
    alerts: buildAlerts(runtime, dataflows, observedAt),
    history: history.snapshot(),
  }
}
