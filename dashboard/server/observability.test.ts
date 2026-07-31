import { expect, test } from 'bun:test'
import { isDashboardSnapshot } from '../src/data'
import { buildSnapshot, SnapshotHistory, type SnapshotRows } from './observability'

const observedAt = new Date('2026-07-31T10:00:00.000Z')

const rows: SnapshotRows = {
  runtime: {
    worker_state: 'running',
    heartbeat_age_seconds: 1.2,
    slot_retained_wal_bytes: 1024,
    pending_wal_bytes: 512,
    persisted_lsn: '0/100',
    published_lsn: '0/110',
    confirmed_lsn: '0/120',
    replay_safe_lsn: '0/118',
    slot_active: true,
    backpressured_streams: 0,
    observed_at: observedAt,
  },
  dataflows: [{
    result_table: 'shiba.order_stats',
    active: true,
    stage_count: 2,
    ready_stage_count: 2,
    pending_input_chunks: 4,
    buffered_output_chunks: 1,
    buffered_output_bytes: 2048,
    backpressured_output_streams: 0,
    last_stage_update: observedAt,
  }],
  dags: [{
    result_table: 'shiba.order_stats',
    explanation: {
      plan: {
        stages: [
          { spec: { operator: 'scan', config: { source_oid: 42 } }, inputs: [] },
          { spec: { operator: 'sink' }, inputs: [{ upstream_stage_id: 0 }] },
        ],
      },
      operators: [
        { stage_id: 0, checkpoint_revision: 10, has_continuation: false, output: { backpressured: false } },
        { stage_id: 1, checkpoint_revision: 11, has_continuation: false, output: { backpressured: false } },
      ],
    },
  }],
  sourceNames: new Map([[42, 'public.orders']]),
}

test('buildSnapshot returns the dashboard contract', () => {
  const snapshot = buildSnapshot(rows, new SnapshotHistory(4), observedAt)

  expect(snapshot.source).toBe('live')
  expect(snapshot.dataflows[0]?.sources).toEqual(['public.orders'])
  expect(snapshot.dags['shiba.order_stats']?.edges.length).toBe(2)
  expect(isDashboardSnapshot(snapshot)).toBe(true)
})

test('history is bounded and keeps the latest values', () => {
  const history = new SnapshotHistory(2)
  history.append(1, 2, 3)
  history.append(4, 5, 6)
  history.append(7, 8, 9)

  expect(history.snapshot()).toEqual({
    pendingWal: [4, 7],
    appliedLag: [5, 8],
    bufferedBytes: [6, 9],
  })
})
