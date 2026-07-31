import { useQuery } from '@tanstack/react-query'
import { useMemo, useState, type CSSProperties } from 'react'
import { createDemoSnapshot, DEMO_FALLBACK_ENABLED, fetchSnapshot, withDemoFallback } from './data'
import type { DagNode, DashboardSnapshot, DataflowSummary, StatusTone } from './types'

type View = 'overview' | 'mvs' | 'alerts'

const toneText: Record<StatusTone, string> = {
  healthy: 'Healthy',
  active: 'Processing',
  warning: 'Warning',
  danger: 'Blocked',
  idle: 'Idle',
}

function formatBytes(value: number): string {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)} GB`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`
  return `${Math.round(value)} B`
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format(value)
}

function formatTime(value: string): string {
  return new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value))
}

function formatAge(value: string): string {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1_000))
  if (seconds < 60) return `${seconds}s ago`
  return `${Math.floor(seconds / 60)}m ago`
}

function StatusPill({ tone, label }: { tone: StatusTone; label?: string }) {
  return (
    <span className={`status-pill status-pill--${tone}`}>
      <span className="status-dot" aria-hidden="true" />
      {label ?? toneText[tone]}
    </span>
  )
}

function MetricCard({ label, value, detail, tone = 'healthy', icon, series }: {
  label: string
  value: string
  detail: string
  tone?: StatusTone
  icon: string
  series?: number[]
}) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <div className="metric-card__topline">
        <span className="metric-card__icon" aria-hidden="true">{icon}</span>
        <span className="metric-card__label">{label}</span>
        {series ? <Sparkline values={series} tone={tone} /> : null}
      </div>
      <strong className="metric-card__value">{value}</strong>
      <span className="metric-card__detail">{detail}</span>
    </article>
  )
}

function Sparkline({ values, tone }: { values: number[]; tone: StatusTone }) {
  if (values.length === 0) return null

  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = Math.max(max - min, 1)
  const points = values.map((value, index) => {
    const x = (index / Math.max(values.length - 1, 1)) * 100
    const y = 100 - ((value - min) / span) * 78 - 11
    return `${x},${y}`
  }).join(' ')

  return (
    <svg className={`sparkline sparkline--${tone}`} viewBox="0 0 100 100" aria-hidden="true" preserveAspectRatio="none">
      <polyline points={points} fill="none" vectorEffect="non-scaling-stroke" />
    </svg>
  )
}

function PipelineMap({ snapshot, onSelectDataflow }: { snapshot: DashboardSnapshot; onSelectDataflow: (id: string) => void }) {
  const systemNodes = snapshot.pipeline.nodes.filter((node) => node.kind !== 'dataflow')
  const dataflowNodes = snapshot.pipeline.nodes.filter((node) => node.kind === 'dataflow')

  return (
    <section className="panel pipeline-panel" aria-labelledby="pipeline-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">LIVE TOPOLOGY</p>
          <h2 id="pipeline-title">Current data pipeline</h2>
        </div>
        <span className="panel-caption">Source to materialized views</span>
      </div>

      <div className="pipeline-system" role="img" aria-label="PostgreSQL WAL through Shiba ingress and source streams">
        {systemNodes.map((node, index) => (
          <div className="pipeline-system__group" key={node.id}>
            <div className={`pipeline-stage pipeline-stage--${node.tone}`}>
              <span className="pipeline-stage__kind">{node.kind}</span>
              <strong>{node.label}</strong>
              <span>{node.subtitle}</span>
              <b>{node.metric}</b>
            </div>
            {index < systemNodes.length - 1 ? <span className="pipeline-arrow" aria-hidden="true">→</span> : null}
          </div>
        ))}
      </div>

      <div className="pipeline-branches">
        <div className="pipeline-branches__label">
          <span className="section-marker" aria-hidden="true" />
          <span>MV consumers</span>
          <span className="muted">{dataflowNodes.length} active graphs</span>
        </div>
        <div className="pipeline-branches__grid">
          {dataflowNodes.map((node) => {
            const id = node.id.replace('mv-', '')
            return (
              <button className={`branch-card branch-card--${node.tone}`} key={node.id} onClick={() => onSelectDataflow(id)}>
                <span className="branch-card__line" aria-hidden="true" />
                <span className="branch-card__main">
                  <strong>{node.label}</strong>
                  <span>{node.subtitle}</span>
                </span>
                <span className="branch-card__metric">{node.metric}</span>
                <span className="branch-card__chevron" aria-hidden="true">↗</span>
              </button>
            )
          })}
        </div>
      </div>
    </section>
  )
}

function DataflowList({ dataflows, selectedId, onSelect }: { dataflows: DataflowSummary[]; selectedId?: string; onSelect: (id: string) => void }) {
  return (
    <section className="panel dataflow-panel" aria-labelledby="dataflow-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">MATERIALIZED VIEWS</p>
          <h2 id="dataflow-title">Dataflow health</h2>
        </div>
        <button className="quiet-button" type="button">View all <span aria-hidden="true">→</span></button>
      </div>
      <div className="dataflow-table" role="table" aria-label="Materialized view health">
        <div className="dataflow-table__header" role="row">
          <span>Result table</span><span>Lag</span><span>Stages</span><span>Queue</span><span>State</span>
        </div>
        {dataflows.length === 0 ? <div className="table-empty" role="row">No materialized views are reporting yet.</div> : dataflows.map((dataflow) => (
          <button
            className={`dataflow-row ${selectedId === dataflow.id ? 'dataflow-row--selected' : ''}`}
            key={dataflow.id}
            onClick={() => onSelect(dataflow.id)}
            role="row"
          >
            <span className="dataflow-row__name">
              <span className={`mini-status mini-status--${dataflow.status}`} aria-hidden="true" />
              <span><strong>{dataflow.name}</strong><small>{dataflow.sources.join(' · ')}</small></span>
            </span>
            <span className={`table-number ${dataflow.lagSeconds > 10 ? 'table-number--danger' : ''}`}>{dataflow.lagSeconds.toFixed(1)}s</span>
            <span className="table-number">{dataflow.readyStages}<small> / {dataflow.stageCount} ready</small></span>
            <span className="table-number">{formatNumber(dataflow.pendingChunks)}<small> chunks</small></span>
            <StatusPill tone={dataflow.status} label={dataflow.backpressured ? 'Backpressure' : toneText[dataflow.status]} />
          </button>
        ))}
      </div>
    </section>
  )
}

function AlertPanel({ alerts, full = false }: { alerts: DashboardSnapshot['alerts']; full?: boolean }) {
  return (
    <section className={`panel alert-panel ${full ? 'alert-panel--full' : ''}`} aria-labelledby="alert-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">SIGNALS</p>
          <h2 id="alert-title">Recent alerts</h2>
        </div>
        <span className="alert-count">{alerts.length} open</span>
      </div>
      <div className="alert-list">
        {alerts.length === 0 ? <p className="empty-copy">No open alerts in the current observation window.</p> : alerts.map((alert) => (
          <article className={`alert-item alert-item--${alert.severity}`} key={alert.id}>
            <span className="alert-item__mark" aria-hidden="true">{alert.severity === 'critical' ? '!' : alert.severity === 'warning' ? '△' : 'i'}</span>
            <div><strong>{alert.title}</strong><p>{alert.description}</p></div>
            <time>{alert.time}</time>
          </article>
        ))}
      </div>
    </section>
  )
}

function DagCanvas({ dag, selectedNodeId, onSelectNode }: { dag: DashboardSnapshot['dags'][string]; selectedNodeId: string | null; onSelectNode: (node: DagNode) => void }) {
  const nodeMap = useMemo(() => new Map(dag.nodes.map((node) => [node.id, node])), [dag.nodes])

  return (
    <section className="panel dag-panel" aria-labelledby="dag-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">SELECTED DATAFLOW</p>
          <h2 id="dag-title">{dag.resultTable} <span className="heading-slash">/</span> DAG</h2>
        </div>
        <div className="dag-legend" aria-label="DAG legend">
          <span><i className="legend-dot legend-dot--healthy" /> healthy</span>
          <span><i className="legend-dot legend-dot--warning" /> queued</span>
          <span><i className="legend-dot legend-dot--danger" /> blocked</span>
        </div>
      </div>
      <div className="dag-scroll">
        <div className="dag-canvas" style={{ width: 1330, height: 510 }}>
          <svg className="dag-links" viewBox="0 0 1330 510" aria-hidden="true">
            <defs>
              <marker id="arrow-healthy" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M 0 0 L 8 4 L 0 8 z" /></marker>
              <marker id="arrow-warning" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M 0 0 L 8 4 L 0 8 z" /></marker>
              <marker id="arrow-danger" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M 0 0 L 8 4 L 0 8 z" /></marker>
            </defs>
            {dag.edges.map((edge) => {
              const source = nodeMap.get(edge.source)
              const target = nodeMap.get(edge.target)
              if (!source || !target) return null
              const x1 = source.x + 184
              const y1 = source.y + 42
              const x2 = target.x
              const y2 = target.y + 42
              const curve = Math.max((x2 - x1) * 0.45, 42)
              return (
                <g key={edge.id} className={`dag-edge dag-edge--${edge.tone}`}>
                  <path d={`M ${x1} ${y1} C ${x1 + curve} ${y1}, ${x2 - curve} ${y2}, ${x2} ${y2}`} markerEnd={`url(#arrow-${edge.tone === 'danger' ? 'danger' : edge.tone === 'warning' ? 'warning' : 'healthy'})`} />
                  <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 8}>{edge.label}</text>
                </g>
              )
            })}
          </svg>
          {dag.nodes.map((node) => {
            const style: CSSProperties = { left: node.x, top: node.y }
            return (
              <button
                className={`dag-node dag-node--${node.kind} dag-node--${node.tone} ${selectedNodeId === node.id ? 'dag-node--selected' : ''}`}
                key={node.id}
                style={style}
                onClick={() => onSelectNode(node)}
                aria-label={`${node.label}, ${toneText[node.tone]}`}
              >
                <span className="dag-node__topline"><span>{node.kind === 'operator' ? `STAGE ${node.stageId}` : node.kind.toUpperCase()}</span><span>↗</span></span>
                <strong>{node.label}</strong>
                <span className="dag-node__meta">{node.kind === 'operator' ? node.operator : node.kind === 'source' ? 'source relation' : 'result table'}</span>
                <span className="dag-node__footer"><span className="mini-status" /> {node.metrics.pendingChunks > 0 ? `${formatNumber(node.metrics.pendingChunks)} chunks` : 'caught up'} </span>
              </button>
            )
          })}
        </div>
      </div>
    </section>
  )
}

function DagEmptyState({ dataflow }: { dataflow?: DataflowSummary }) {
  return (
    <section className="panel dag-empty" aria-live="polite">
      <p className="eyebrow">SELECTED DATAFLOW</p>
      <h2>{dataflow ? `${dataflow.name} / DAG` : 'No dataflow selected'}</h2>
      <p>{dataflow ? 'The observation API has not published a stage graph for this materialized view yet.' : 'Once a materialized view appears, its stage topology will be shown here.'}</p>
    </section>
  )
}

function Inspector({ dataflow, node }: { dataflow?: DataflowSummary; node: DagNode | null }) {
  if (!dataflow) {
    return (
      <aside className="inspector" aria-label="Selected node details">
        <div className="inspector__header"><div><p className="eyebrow">INSPECTOR</p><h2>No selection</h2></div></div>
        <p className="empty-copy">There is no materialized view or stage node to inspect.</p>
      </aside>
    )
  }

  return (
    <aside className="inspector" aria-label="Selected node details">
      <div className="inspector__header">
        <div><p className="eyebrow">INSPECTOR</p><h2>{node ? node.label : dataflow.name}</h2></div>
        <span className="inspector__kebab">•••</span>
      </div>
      <StatusPill tone={node?.tone ?? dataflow.status} label={node ? toneText[node.tone] : (dataflow.backpressured ? 'Backpressure' : toneText[dataflow.status])} />
      <div className="inspector__section">
        <span className="inspector__label">Identity</span>
        <dl className="detail-list">
          <div><dt>Dataflow</dt><dd>{dataflow.name}</dd></div>
          <div><dt>Node type</dt><dd>{node?.kind ?? 'dataflow'}</dd></div>
          {node?.stageId !== undefined ? <div><dt>Stage</dt><dd>{node.stageId}</dd></div> : null}
          <div><dt>Last progress</dt><dd>{formatAge(node?.metrics.lastUpdated ?? dataflow.lastProgress)}</dd></div>
        </dl>
      </div>
      <div className="inspector__section">
        <span className="inspector__label">Runtime signals</span>
        <div className="signal-list">
          <div><span>Pending chunks</span><strong>{formatNumber(node?.metrics.pendingChunks ?? dataflow.pendingChunks)}</strong></div>
          <div><span>Buffered bytes</span><strong>{formatBytes(node?.metrics.bufferedBytes ?? dataflow.outputBytes)}</strong></div>
          <div><span>Checkpoint</span><strong>{formatNumber(node?.metrics.checkpointRevision ?? dataflow.readyStages)}</strong></div>
        </div>
      </div>
      <div className="inspector__callout">
        <span className="callout-icon" aria-hidden="true">⌁</span>
        <p>{node?.tone === 'danger' || dataflow.backpressured ? 'Downstream pressure is holding this path. Inspect the next blocked edge before changing batch limits.' : 'This path is advancing. Select another node or edge to inspect its durable cursor.'}</p>
      </div>
      <button className="inspector-button" type="button">Open explain plan <span aria-hidden="true">↗</span></button>
    </aside>
  )
}

function FeedState({ error, loading, onRetry }: { error?: string; loading: boolean; onRetry: () => void }) {
  return (
    <main className="feed-state" aria-live="polite">
      <div className="feed-state__card">
        <p className="eyebrow">SHIBA / OBSERVABILITY</p>
        <h1>{loading ? 'Connecting to observation API' : 'Observation feed unavailable'}</h1>
        <p>{loading ? 'Waiting for the first runtime snapshot.' : error ?? 'The observation API did not return a usable snapshot.'}</p>
        {!loading ? <button className="inspector-button" type="button" onClick={onRetry}>Retry connection <span aria-hidden="true">↻</span></button> : null}
      </div>
    </main>
  )
}

function Sidebar({ activeView, onChange, snapshot }: { activeView: View; onChange: (view: View) => void; snapshot: DashboardSnapshot }) {
  return (
    <aside className="sidebar">
      <div className="brand"><span className="brand-mark">S</span><span><strong>SHIBA</strong><small>OPS CONSOLE</small></span></div>
      <div className="sidebar__rule" />
      <nav className="sidebar-nav" aria-label="Primary navigation">
        <button className={activeView === 'overview' ? 'sidebar-nav__item sidebar-nav__item--active' : 'sidebar-nav__item'} onClick={() => onChange('overview')}><span>◈</span> Overview</button>
        <button className={activeView === 'mvs' ? 'sidebar-nav__item sidebar-nav__item--active' : 'sidebar-nav__item'} onClick={() => onChange('mvs')}><span>⌘</span> Materialized views <b>{snapshot.dataflows.length}</b></button>
        <button className={activeView === 'alerts' ? 'sidebar-nav__item sidebar-nav__item--active' : 'sidebar-nav__item'} onClick={() => onChange('alerts')}><span>△</span> Alerts <b className="sidebar-nav__alert">{snapshot.alerts.length}</b></button>
      </nav>
      <div className="sidebar__footer">
        <div className="connection-card"><span className="status-dot status-dot--healthy" /><span><strong>shiba_local</strong><small>PostgreSQL 17 · primary</small></span><span className="connection-card__more">•••</span></div>
        <div className="sidebar-footnote"><span className="pulse" /> Auto-refresh 10s</div>
      </div>
    </aside>
  )
}

export default function App() {
  const [activeView, setActiveView] = useState<View>('overview')
  const [selectedDataflowId, setSelectedDataflowId] = useState('order_stats')
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>('aggregate')
  const query = useQuery<DashboardSnapshot>({
    queryKey: ['observability-snapshot'],
    queryFn: async ({ signal }) => {
      try {
        return await fetchSnapshot(signal)
      } catch (error) {
        if (DEMO_FALLBACK_ENABLED) return withDemoFallback(error)
        throw error
      }
    },
    initialData: DEMO_FALLBACK_ENABLED ? createDemoSnapshot() : undefined,
  })
  const snapshot = query.data
  if (!snapshot) {
    return <FeedState loading={query.isPending} error={query.error instanceof Error ? query.error.message : undefined} onRetry={() => void query.refetch()} />
  }

  const selectedDataflow = snapshot.dataflows.find((dataflow) => dataflow.id === selectedDataflowId) ?? snapshot.dataflows[0]
  const selectedDag = selectedDataflow ? snapshot.dags[selectedDataflow.id] : undefined
  const selectedNode = selectedDag?.nodes.find((node) => node.id === selectedNodeId) ?? null
  const maxAppliedLag = snapshot.dataflows.length > 0 ? Math.max(...snapshot.dataflows.map((item) => item.lagSeconds)) : 0
  const underFiveCount = snapshot.dataflows.filter((item) => item.lagSeconds < 5).length

  function selectDataflow(id: string) {
    setSelectedDataflowId(id)
    setSelectedNodeId(null)
    setActiveView('mvs')
  }

  return (
    <div className="app-shell">
      <Sidebar activeView={activeView} onChange={setActiveView} snapshot={snapshot} />
      <main className="workspace">
        <header className="topbar">
          <div className="breadcrumb"><span>Shiba</span><span>/</span><strong>{activeView === 'alerts' ? 'Signals' : activeView === 'mvs' ? 'Materialized views' : 'Runtime topology'}</strong></div>
          <div className="topbar__actions">
            <span className={`feed-badge feed-badge--${snapshot.source}`}><span className="status-dot" /> {snapshot.source === 'live' ? 'LIVE FEED' : 'DEMO FEED'}</span>
            <span className="last-refresh">Updated {formatTime(snapshot.observedAt)}</span>
            <button className="icon-button" type="button" onClick={() => void query.refetch()} aria-label="Refresh observation data">↻</button>
            <button className="avatar" type="button" aria-label="Open account menu">Z</button>
          </div>
        </header>

        <div className="workspace__body">
          <div className="page-intro">
            <div><p className="eyebrow">DATABASE / PRIMARY</p><h1>Runtime topology</h1><p>See where data is flowing, where it is waiting, and which MV needs attention.</p></div>
            <div className="intro-status"><StatusPill tone={snapshot.runtime.state === 'running' ? 'healthy' : 'danger'} label={`Runtime ${snapshot.runtime.state}`} /><span>Heartbeat {snapshot.runtime.heartbeatAgeSeconds.toFixed(1)}s ago</span></div>
          </div>

          <div className="metric-grid">
            <MetricCard label="Runtime health" value={snapshot.runtime.state === 'running' ? 'Operational' : 'Needs attention'} detail={`Heartbeat ${snapshot.runtime.heartbeatAgeSeconds.toFixed(1)}s ago`} tone={snapshot.runtime.state === 'running' ? 'healthy' : 'danger'} icon="◉" />
            <MetricCard label="Pending WAL" value={formatBytes(snapshot.runtime.pendingWalBytes)} detail={`Retained ${formatBytes(snapshot.runtime.retainedWalBytes)}`} tone={snapshot.runtime.pendingWalBytes > 100_000_000 ? 'warning' : 'active'} icon="⌁" series={snapshot.history.pendingWal} />
            <MetricCard label="Applied lag" value={snapshot.dataflows.length > 0 ? `${maxAppliedLag.toFixed(1)}s` : '—'} detail={snapshot.dataflows.length > 0 ? `${underFiveCount} / ${snapshot.dataflows.length} MVs under 5s` : 'No materialized views reporting'} tone={snapshot.dataflows.some((item) => item.lagSeconds > 10) ? 'warning' : 'healthy'} icon="↘" series={snapshot.history.appliedLag} />
            <MetricCard label="Backpressure" value={formatNumber(snapshot.runtime.backpressuredStreams)} detail="streams currently holding work" tone={snapshot.runtime.backpressuredStreams > 0 ? 'warning' : 'healthy'} icon="⊙" series={snapshot.history.bufferedBytes} />
          </div>

          {activeView === 'alerts' ? <AlertPanel alerts={snapshot.alerts} full /> : (
            <>
              {activeView === 'overview' ? <PipelineMap snapshot={snapshot} onSelectDataflow={selectDataflow} /> : null}
              <div className="dashboard-grid" id="mvs">
                <DataflowList dataflows={snapshot.dataflows} selectedId={selectedDataflow?.id} onSelect={selectDataflow} />
                {activeView === 'overview' ? <AlertPanel alerts={snapshot.alerts} /> : <div className="panel mvs-summary"><p className="eyebrow">MV WORKSPACE</p><h2>Choose a graph</h2><p>Select a result table to inspect its full stage topology and durable work signals.</p><div className="summary-stat"><strong>{snapshot.dataflows.length}</strong><span>materialized views tracked</span></div></div>}
              </div>
              {selectedDag ? <DagCanvas dag={selectedDag} selectedNodeId={selectedNodeId} onSelectNode={(node) => setSelectedNodeId(node.id)} /> : <DagEmptyState dataflow={selectedDataflow} />}
            </>
          )}
          {snapshot.feedError ? <div className="feed-notice"><span>i</span><span>Live observation API unavailable: {snapshot.feedError}. Showing demo topology so the interface remains inspectable.</span></div> : null}
          {query.isError && !snapshot.feedError ? <div className="feed-notice"><span>i</span><span>Live observation API unavailable: {query.error instanceof Error ? query.error.message : 'unknown error'}. Showing the last successful snapshot.</span></div> : null}
        </div>
      </main>
      <Inspector dataflow={selectedDataflow} node={selectedNode} />
    </div>
  )
}
