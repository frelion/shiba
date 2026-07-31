import { SQL } from 'bun'
import { loadConfig } from './config'
import { buildSnapshot, extractSourceOids, SnapshotHistory, type DbRow, type SnapshotRows } from './observability'

const config = loadConfig()
const database = new SQL(config.databaseUrl, {
  connectionTimeout: 5,
  idleTimeout: 30,
  max: 4,
})
const history = new SnapshotHistory(config.historyLimit)

const jsonHeaders = new Headers({
  'cache-control': 'no-store',
  'content-type': 'application/json; charset=utf-8',
})

function headersFor(request: Request): Headers {
  const headers = new Headers(jsonHeaders)
  const origin = request.headers.get('origin')
  if (config.corsOrigin && origin === config.corsOrigin) {
    headers.set('access-control-allow-origin', origin)
    headers.set('access-control-allow-methods', 'GET, OPTIONS')
    headers.set('access-control-allow-headers', 'Accept, Content-Type')
    headers.set('vary', 'Origin')
  }
  return headers
}

function jsonResponse(request: Request, body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: headersFor(request) })
}

function problemResponse(request: Request, requestId: string, status: number, title: string, detail: string): Response {
  return jsonResponse(request, {
    type: `https://shiba.dev/problems/${status === 503 ? 'observability-unavailable' : 'method-not-allowed'}`,
    title,
    status,
    detail,
    requestId,
  }, status)
}

async function sourceNamesFor(dagRows: DbRow[]): Promise<Map<number, string>> {
  const sourceOids = [...new Set(dagRows.flatMap((row) => extractSourceOids(row.explanation)))]
  const names = new Map<number, string>()
  await Promise.all(sourceOids.map(async (sourceOid) => {
    const rows = await database<DbRow[]>`
      SELECT format('%I.%I', namespace.nspname, relation.relname) AS source_name
      FROM pg_catalog.pg_class AS relation
      JOIN pg_catalog.pg_namespace AS namespace
        ON namespace.oid = relation.relnamespace
      WHERE relation.oid = ${sourceOid}::oid
    `
    const name = rows[0]?.source_name
    if (typeof name === 'string') names.set(sourceOid, name)
  }))
  return names
}

async function readSnapshotRows(): Promise<SnapshotRows> {
  const [runtimeRows, dataflowRows, dagRows] = await Promise.all([
    database<DbRow[]>`
      SELECT worker_state,
             EXTRACT(EPOCH FROM runtime_heartbeat_age) AS heartbeat_age_seconds,
             slot_retained_wal_bytes,
             pending_wal_bytes,
             persisted_lsn::text AS persisted_lsn,
             published_lsn::text AS published_lsn,
             confirmed_lsn::text AS confirmed_lsn,
             replay_safe_lsn::text AS replay_safe_lsn,
             slot_active,
             backpressured_streams,
             observed_at
      FROM shiba.runtime_status()
    `,
    database<DbRow[]>`
      SELECT result_table::text AS result_table,
             active,
             stage_count,
             ready_stage_count,
             checkpoint_revision,
             admitted_rows,
             admitted_bytes,
             pending_input_chunks,
             buffered_output_chunks,
             buffered_output_bytes,
             backpressured_output_streams,
             applied_lsn::text AS applied_lsn,
             last_stage_update,
             observed_at
      FROM shiba.dataflow_status()
      ORDER BY result_table::text
    `,
    database<DbRow[]>`
      SELECT status.result_table::text AS result_table,
             shiba.explain_dataflow(status.result_table) AS explanation
      FROM shiba.dataflow_status() AS status
      ORDER BY status.result_table::text
    `,
  ])

  return {
    runtime: runtimeRows[0],
    dataflows: dataflowRows,
    dags: dagRows,
    sourceNames: await sourceNamesFor(dagRows),
  }
}

async function snapshotResponse(request: Request): Promise<Response> {
  const requestId = crypto.randomUUID()
  try {
    return jsonResponse(request, buildSnapshot(await readSnapshotRows(), history))
  } catch (error) {
    console.error(`[${requestId}] observation snapshot failed`, error instanceof Error ? error.message : 'unknown error')
    return problemResponse(request, requestId, 503, 'Observation feed unavailable', 'The observation API could not read a complete Shiba snapshot.')
  }
}

async function healthResponse(request: Request): Promise<Response> {
  const requestId = crypto.randomUUID()
  try {
    await database`SELECT 1`
    return jsonResponse(request, { status: 'ok' })
  } catch (error) {
    console.error(`[${requestId}] observation health check failed`, error instanceof Error ? error.message : 'unknown error')
    return problemResponse(request, requestId, 503, 'Database unavailable', 'The observation API cannot reach PostgreSQL.')
  }
}

function createFetch(): (request: Request) => Response | Promise<Response> {
  return async (request) => {
    const url = new URL(request.url)
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: headersFor(request) })
    if (request.method !== 'GET') return problemResponse(request, crypto.randomUUID(), 405, 'Method not allowed', 'Use GET for observation resources.')
    if (url.pathname === '/healthz') return healthResponse(request)
    if (url.pathname === '/api/observability/snapshot') return snapshotResponse(request)
    return jsonResponse(request, { type: 'about:blank', title: 'Not found', status: 404 }, 404)
  }
}

const server = Bun.serve({
  fetch: createFetch(),
  hostname: config.host,
  port: config.port,
})

console.log(`Shiba observability API listening on http://${server.hostname}:${server.port}`)
