CREATE FUNCTION shiba.explain_dataflow(result_table regclass)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    explanation jsonb;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM shiba_internal.dataflows AS dataflow
    WHERE dataflow.result_oid = result_table::oid
  ) THEN
    RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
      USING ERRCODE = 'wrong_object_type';
  END IF;
  IF NOT has_table_privilege(
    shiba.invoker_oid(), result_table, 'SELECT'
  ) THEN
    RAISE EXCEPTION 'SELECT privilege on % is required', result_table
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  SELECT jsonb_build_object(
    'plan', dataflow.plan,
    'operators', coalesce((
      SELECT jsonb_agg(
        jsonb_build_object(
          'stage_id', stage.ordinality - 1,
          'operator', stage.value -> 'spec' ->> 'operator',
          'checkpoint_revision', checkpoint.revision,
          'has_continuation', checkpoint.has_continuation,
          'admitted_rows', checkpoint.admitted_rows,
          'admitted_bytes', checkpoint.admitted_bytes,
          'inputs', coalesce((
            SELECT jsonb_agg(
              jsonb_build_object(
                'input_port', consumer.input_port,
                'stream_id', consumer.stream_id,
                'next_chunk_seq', consumer.next_chunk_seq,
                'consumed_frontier_lsn', consumer.consumed_frontier_lsn
              )
              ORDER BY consumer.input_port
            )
            FROM shiba_internal.effect_stream_consumers AS consumer
            WHERE consumer.result_oid = dataflow.result_oid
              AND consumer.consumer_stage_id
                    = stage.ordinality - 1
          ), '[]'::jsonb),
          'output', (
            SELECT jsonb_build_object(
              'stream_id', output.stream_id,
              'next_chunk_seq', output.next_chunk_seq,
              'backpressured', output.backpressured
            )
            FROM shiba_internal.effect_streams AS output
            WHERE output.producer_kind = 'operator'
              AND output.producer_result_oid = dataflow.result_oid
              AND output.producer_stage_id
                    = stage.ordinality - 1
          )
        )
        ORDER BY stage.ordinality
      )
      FROM jsonb_array_elements(dataflow.plan -> 'stages')
        WITH ORDINALITY AS stage(value, ordinality)
      LEFT JOIN shiba_internal.operator_checkpoints AS checkpoint
        ON checkpoint.result_oid = dataflow.result_oid
       AND checkpoint.stage_id = stage.ordinality - 1
    ), '[]'::jsonb)
  )
  INTO STRICT explanation
  FROM shiba_internal.dataflows AS dataflow
  WHERE dataflow.result_oid = result_table::oid;

  RETURN explanation;
END;
$$;

-- Runtime health and durable backlog.  This is deliberately derived from the
-- catalog instead of Runtime-local memory: the snapshot remains useful while
-- the worker is restarting, or when a second session is inspecting a stalled
-- worker.  The function exposes metadata and counters only; source payloads
-- are never returned here.
CREATE FUNCTION shiba.runtime_status()
RETURNS TABLE (
    worker_state text,
    runtime_active boolean,
    runtime_pid integer,
    runtime_started_at timestamptz,
    runtime_last_heartbeat timestamptz,
    runtime_heartbeat_age interval,
    runtime_healthy boolean,
    launch_generation bigint,
    slot_name name,
    slot_active boolean,
    slot_pid integer,
    slot_restart_lsn pg_lsn,
    slot_confirmed_flush_lsn pg_lsn,
    slot_retained_wal_bytes bigint,
    pending_wal_bytes bigint,
    ingress_generation bigint,
    ingress_state text,
    persisted_lsn pg_lsn,
    published_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn,
    open_payload_bytes bigint,
    open_transactions bigint,
    pending_publications bigint,
    staged_events numeric,
    staged_bytes numeric,
    source_streams bigint,
    buffered_chunks numeric,
    buffered_rows numeric,
    buffered_bytes numeric,
    backpressured_streams bigint,
    observed_at timestamptz
)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT CASE
             WHEN NOT state_row.active THEN 'inactive'
             WHEN state_row.owner_pid IS NULL THEN 'starting'
             WHEN activity.pid IS NULL THEN 'missing'
             WHEN state_row.last_heartbeat IS NULL
                  OR state_row.last_heartbeat < clock_timestamp() - interval '10 seconds'
               THEN 'stale'
             ELSE 'running'
           END,
           state_row.active,
           state_row.owner_pid,
           state_row.started_at,
           state_row.last_heartbeat,
           CASE
             WHEN state_row.last_heartbeat IS NULL THEN NULL::interval
             ELSE clock_timestamp() - state_row.last_heartbeat
           END,
           (
             state_row.active
             AND state_row.owner_pid IS NOT NULL
             AND activity.pid IS NOT NULL
             AND state_row.last_heartbeat IS NOT NULL
             AND state_row.last_heartbeat >= clock_timestamp() - interval '10 seconds'
           ),
           state_row.launch_generation,
           slot.slot_name::name,
           slot.active_pid IS NOT NULL,
           slot.active_pid,
           slot.restart_lsn,
           slot.confirmed_flush_lsn,
           COALESCE(
             GREATEST(
               pg_wal_lsn_diff(pg_current_wal_lsn(), slot.restart_lsn),
               0
             )::bigint,
             0
           ),
           COALESCE(
             GREATEST(
               pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn),
               0
             )::bigint,
             0
           ),
           replay.slot_generation,
           replay.state,
           replay.persisted_lsn,
           replay.published_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn,
           COALESCE(replay.open_payload_bytes, 0),
           COALESCE(ingress.open_transactions, 0),
           COALESCE(ingress.pending_publications, 0),
           COALESCE(ingress.staged_events, 0),
           COALESCE(ingress.staged_bytes, 0),
           COALESCE(streams.source_streams, 0),
           COALESCE(streams.buffered_chunks, 0),
           COALESCE(streams.buffered_rows, 0),
           COALESCE(streams.buffered_bytes, 0),
           COALESCE(streams.backpressured_streams, 0),
           clock_timestamp()
    FROM shiba_internal.runtime_state AS state_row
    LEFT JOIN pg_stat_activity AS activity
      ON activity.pid = state_row.owner_pid
    LEFT JOIN pg_replication_slots AS slot
      ON slot.slot_name = shiba_internal.slot_name()::text
    LEFT JOIN LATERAL (
        SELECT replay_state.*
        FROM shiba_internal.ingress_replay_state AS replay_state
        WHERE replay_state.state = 'active'
        ORDER BY replay_state.slot_generation DESC
        LIMIT 1
    ) AS replay ON true
    LEFT JOIN LATERAL (
        SELECT count(*) FILTER (WHERE txn.status = 'open')::bigint
                   AS open_transactions,
               COALESCE(sum(txn.pending_publications), 0)::bigint
                   AS pending_publications,
               COALESCE(sum(txn.event_count), 0)::numeric AS staged_events,
               COALESCE(sum(txn.payload_bytes), 0)::numeric AS staged_bytes
        FROM shiba_internal.ingress_transactions AS txn
        WHERE txn.slot_generation = replay.slot_generation
    ) AS ingress ON true
    LEFT JOIN LATERAL (
        SELECT count(*) FILTER (WHERE stream.producer_kind = 'source')::bigint
                   AS source_streams,
               COALESCE(sum(stream.buffered_chunks), 0)::numeric
                   AS buffered_chunks,
               COALESCE(sum(stream.buffered_rows), 0)::numeric
                   AS buffered_rows,
               COALESCE(sum(stream.buffered_bytes), 0)::numeric
                   AS buffered_bytes,
               count(*) FILTER (WHERE stream.backpressured)::bigint
                   AS backpressured_streams
        FROM shiba_internal.effect_streams AS stream
    ) AS streams ON true
    WHERE state_row.singleton;
$$;

-- Per-dataflow queue health.  Result-table privileges are checked against the
-- outer caller, because this function is SECURITY DEFINER.  The ready-stage
-- predicate intentionally mirrors the scheduler's durable readiness rule so
-- that an operator backlog can be explained without inspecting private Rust
-- state.
CREATE FUNCTION shiba.dataflow_status()
RETURNS TABLE (
    result_table regclass,
    active boolean,
    created_at timestamptz,
    stage_count bigint,
    ready_stage_count bigint,
    checkpoint_revision numeric,
    admitted_rows numeric,
    admitted_bytes numeric,
    input_stream_count bigint,
    pending_input_chunks numeric,
    buffered_output_chunks numeric,
    buffered_output_rows numeric,
    buffered_output_bytes numeric,
    backpressured_output_streams bigint,
    applied_lsn pg_lsn,
    last_stage_update timestamptz,
    observed_at timestamptz
)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT dataflow.result_oid::regclass,
           dataflow.active,
           dataflow.created_at,
           COALESCE(checkpoints.stage_count, 0),
           COALESCE(checkpoints.ready_stage_count, 0),
           COALESCE(checkpoints.checkpoint_revision, 0),
           COALESCE(checkpoints.admitted_rows, 0),
           COALESCE(checkpoints.admitted_bytes, 0),
           COALESCE(inputs.input_stream_count, 0),
           COALESCE(inputs.pending_input_chunks, 0),
           COALESCE(outputs.buffered_output_chunks, 0),
           COALESCE(outputs.buffered_output_rows, 0),
           COALESCE(outputs.buffered_output_bytes, 0),
           COALESCE(outputs.backpressured_output_streams, 0),
           sink.applied_lsn,
           GREATEST(checkpoints.last_stage_update, sink.last_stage_update),
           clock_timestamp()
    FROM shiba_internal.dataflows AS dataflow
    LEFT JOIN LATERAL (
        SELECT count(*)::bigint AS stage_count,
               count(*) FILTER (WHERE
                   (
                     checkpoint.has_continuation
                     OR EXISTS (
                       SELECT 1
                       FROM shiba_internal.effect_stream_consumers AS consumer
                       JOIN shiba_internal.effect_streams AS input_stream
                         ON input_stream.stream_id = consumer.stream_id
                       WHERE consumer.result_oid = checkpoint.result_oid
                         AND consumer.consumer_stage_id = checkpoint.stage_id
                         AND (
                           consumer.next_chunk_seq < input_stream.next_chunk_seq
                           OR (
                             input_stream.producer_kind = 'source'
                             AND EXISTS (
                               SELECT 1
                               FROM shiba_internal.ingress_replay_state AS publication
                               WHERE publication.slot_generation = input_stream.slot_generation
                                 AND publication.published_lsn IS NOT NULL
                                 AND consumer.consumed_frontier_lsn
                                       < publication.published_lsn
                             )
                           )
                         )
                     )
                   )
                   AND NOT EXISTS (
                     SELECT 1
                     FROM shiba_internal.effect_streams AS output_stream
                     WHERE output_stream.producer_kind = 'operator'
                       AND output_stream.producer_result_oid = checkpoint.result_oid
                       AND output_stream.producer_stage_id = checkpoint.stage_id
                       AND output_stream.backpressured
                   )
               )::bigint AS ready_stage_count,
               COALESCE(sum(checkpoint.revision), 0)::numeric
                   AS checkpoint_revision,
               COALESCE(sum(checkpoint.admitted_rows), 0)::numeric
                   AS admitted_rows,
               COALESCE(sum(checkpoint.admitted_bytes), 0)::numeric
                   AS admitted_bytes,
               max(checkpoint.updated_at) AS last_stage_update
        FROM shiba_internal.operator_checkpoints AS checkpoint
        WHERE checkpoint.result_oid = dataflow.result_oid
    ) AS checkpoints ON true
    LEFT JOIN LATERAL (
        SELECT count(*)::bigint AS input_stream_count,
               COALESCE(
                 sum(
                   GREATEST(
                     input_stream.next_chunk_seq - consumer.next_chunk_seq,
                     0
                   )::numeric
                 ),
                 0
               ) AS pending_input_chunks
        FROM shiba_internal.effect_stream_consumers AS consumer
        JOIN shiba_internal.effect_streams AS input_stream
          ON input_stream.stream_id = consumer.stream_id
        WHERE consumer.result_oid = dataflow.result_oid
    ) AS inputs ON true
    LEFT JOIN LATERAL (
        SELECT COALESCE(sum(stream.buffered_chunks), 0)::numeric
                   AS buffered_output_chunks,
               COALESCE(sum(stream.buffered_rows), 0)::numeric
                   AS buffered_output_rows,
               COALESCE(sum(stream.buffered_bytes), 0)::numeric
                   AS buffered_output_bytes,
               count(*) FILTER (WHERE stream.backpressured)::bigint
                   AS backpressured_output_streams
        FROM shiba_internal.effect_streams AS stream
        WHERE stream.producer_kind = 'operator'
          AND stream.producer_result_oid = dataflow.result_oid
    ) AS outputs ON true
    LEFT JOIN LATERAL (
        SELECT consumer.consumed_frontier_lsn AS applied_lsn,
               consumer.updated_at AS last_stage_update
        FROM shiba_internal.effect_stream_consumers AS consumer
        JOIN jsonb_array_elements(dataflow.plan -> 'stages')
          WITH ORDINALITY AS stage(value, ordinality)
          ON stage.value -> 'spec' ->> 'operator' = 'sink'
         AND consumer.consumer_stage_id = stage.ordinality - 1
        WHERE consumer.result_oid = dataflow.result_oid
        ORDER BY consumer.consumed_frontier_lsn, consumer.updated_at
        LIMIT 1
    ) AS sink ON true
    WHERE has_table_privilege(
              shiba.invoker_oid(), dataflow.result_oid, 'SELECT'
          );
$$;

-- Scalar metrics for lightweight SQL/Prometheus-style polling.  Labels such
-- as result_table are intentionally kept in dataflow_status(), while this
-- function stays a small database-wide metric set.
CREATE FUNCTION shiba.runtime_metrics()
RETURNS TABLE (
    metric_name text,
    metric_value double precision
)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT metric.metric_name, metric.metric_value
    FROM shiba.runtime_status() AS status
    CROSS JOIN LATERAL (
        VALUES
          ('shiba_runtime_up',
             CASE WHEN status.worker_state = 'running' THEN 1
                  ELSE 0 END::double precision),
          ('shiba_runtime_healthy',
             CASE WHEN status.runtime_healthy THEN 1
                  ELSE 0 END::double precision),
          ('shiba_runtime_heartbeat_age_seconds',
             COALESCE(extract(epoch FROM status.runtime_heartbeat_age), -1)
               ::double precision),
          ('shiba_slot_active',
             CASE WHEN status.slot_active THEN 1
                  ELSE 0 END::double precision),
          ('shiba_slot_retained_wal_bytes',
             status.slot_retained_wal_bytes::double precision),
          ('shiba_pending_wal_bytes',
             status.pending_wal_bytes::double precision),
          ('shiba_ingress_open_transactions',
             status.open_transactions::double precision),
          ('shiba_ingress_pending_publications',
             status.pending_publications::double precision),
          ('shiba_ingress_staged_events',
             status.staged_events::double precision),
          ('shiba_ingress_staged_bytes',
             status.staged_bytes::double precision),
          ('shiba_effect_stream_buffered_chunks',
             status.buffered_chunks::double precision),
          ('shiba_effect_stream_buffered_rows',
             status.buffered_rows::double precision),
          ('shiba_effect_stream_buffered_bytes',
             status.buffered_bytes::double precision),
          ('shiba_effect_stream_backpressured_streams',
             status.backpressured_streams::double precision)
    ) AS metric(metric_name, metric_value);
$$;
