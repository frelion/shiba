export type StatusTone = 'healthy' | 'active' | 'warning' | 'danger' | 'idle'

export type PipelineNodeKind = 'wal' | 'ingress' | 'stream' | 'dataflow'
export type DagNodeKind = 'source' | 'operator' | 'sink'

export interface RuntimeSnapshot {
  state: 'running' | 'starting' | 'stale' | 'missing' | 'inactive'
  heartbeatAgeSeconds: number
  pendingWalBytes: number
  retainedWalBytes: number
  persistedLsn: string
  publishedLsn: string
  appliedLsn: string
  slotActive: boolean
  backpressuredStreams: number
  observedAt: string
}

export interface PipelineNode {
  id: string
  label: string
  subtitle: string
  kind: PipelineNodeKind
  tone: StatusTone
  metric: string
}

export interface PipelineEdge {
  id: string
  source: string
  target: string
  label: string
  tone: StatusTone
}

export interface PipelineGraph {
  nodes: PipelineNode[]
  edges: PipelineEdge[]
}

export interface DagNodeMetrics {
  pendingChunks: number
  bufferedBytes: number
  checkpointRevision: number
  lastUpdated: string
}

export interface DagNode {
  id: string
  label: string
  kind: DagNodeKind
  operator?: string
  stageId?: number
  tone: StatusTone
  x: number
  y: number
  metrics: DagNodeMetrics
}

export interface DagEdge {
  id: string
  source: string
  target: string
  label: string
  tone: StatusTone
  backpressured: boolean
}

export interface DataflowDag {
  resultTable: string
  nodes: DagNode[]
  edges: DagEdge[]
}

export interface DataflowSummary {
  id: string
  name: string
  sources: string[]
  status: StatusTone
  lagSeconds: number
  pendingChunks: number
  outputBytes: number
  backpressured: boolean
  stageCount: number
  readyStages: number
  lastProgress: string
}

export interface AlertItem {
  id: string
  severity: 'info' | 'warning' | 'critical'
  title: string
  description: string
  time: string
}

export interface DashboardSnapshot {
  source: 'live' | 'demo'
  observedAt: string
  runtime: RuntimeSnapshot
  pipeline: PipelineGraph
  dataflows: DataflowSummary[]
  dags: Record<string, DataflowDag>
  alerts: AlertItem[]
  history: {
    pendingWal: number[]
    appliedLag: number[]
    bufferedBytes: number[]
  }
  feedError?: string
}
