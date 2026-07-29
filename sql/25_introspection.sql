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
