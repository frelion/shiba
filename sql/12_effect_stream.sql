-- Transactional APIs for the durable stream catalog declared in
-- 00_catalog.sql. Payload rows live in the per-stream LOGGED relation recorded
-- by effect_stream_payloads; chunk metadata and payload rows commit together.

-- Logical effect bytes are the complete PostgreSQL binary record plus the
-- stream's weight field. record_send detoasts every attribute, so an in-memory
-- producer row and the same stored payload have identical accounting. Unlike
-- a JSONB roundtrip, the binary record also retains array dimensions.
CREATE FUNCTION shiba_internal.effect_row_bytes(row_value anyelement)
RETURNS bigint
LANGUAGE sql
STABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $$
  SELECT (
    pg_catalog.octet_length(pg_catalog.record_send(row_value)) + 8
  )::bigint
$$;

-- Registration holds every source table against writes while each Scan
-- spools its activation snapshot. A source consumer joins at the current
-- stream tail and ignores later-published backlog at or before activation_lsn.
-- Operator consumers are fixed before their producer starts and must consume
-- the producer's activation SnapshotFrontier, so their frontier baseline is
-- zero rather than the dataflow activation LSN.
CREATE FUNCTION shiba_internal.attach_effect_stream_consumer(
    target_stream_id bigint,
    target_result_oid oid,
    target_consumer_stage_id integer,
    target_input_port integer,
    target_activation_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stream_state shiba_internal.effect_streams%ROWTYPE;
    initial_next_chunk_seq bigint;
    initial_activation_lsn pg_lsn;
BEGIN
    IF target_result_oid IS NULL
       OR target_consumer_stage_id IS NULL
       OR target_consumer_stage_id < 0
       OR target_input_port IS NULL
       OR target_input_port < 0
       OR target_activation_lsn IS NULL THEN
        RAISE EXCEPTION 'invalid effect stream consumer identity'
          USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO STRICT stream_state
    FROM shiba_internal.effect_streams
    WHERE stream_id = target_stream_id
    FOR UPDATE;

    IF stream_state.producer_kind = 'operator' THEN
        IF stream_state.producer_result_oid <> target_result_oid THEN
            RAISE EXCEPTION 'operator edge crosses result graphs'
              USING ERRCODE = 'foreign_key_violation';
        END IF;
        IF stream_state.next_chunk_seq <> 1 THEN
            RAISE EXCEPTION
              'cannot attach an operator consumer after production starts'
              USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
        initial_next_chunk_seq := 1;
        initial_activation_lsn := '0/0';
    ELSE
        initial_next_chunk_seq := stream_state.next_chunk_seq;
        initial_activation_lsn := target_activation_lsn;
    END IF;

    PERFORM 1
    FROM shiba_internal.operator_checkpoints
    WHERE result_oid = target_result_oid
      AND stage_id = target_consumer_stage_id
    FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'effect consumer stage %/% is not registered',
          target_result_oid, target_consumer_stage_id
          USING ERRCODE = 'foreign_key_violation';
    END IF;

    INSERT INTO shiba_internal.effect_stream_consumers(
      stream_id,
      result_oid,
      consumer_stage_id,
      input_port,
      next_chunk_seq,
      activation_lsn,
      consumed_frontier_lsn
    )
    VALUES (
      target_stream_id,
      target_result_oid,
      target_consumer_stage_id,
      target_input_port,
      initial_next_chunk_seq,
      initial_activation_lsn,
      initial_activation_lsn
    );
END;
$$;

CREATE FUNCTION shiba_internal.append_effect_stream_chunk(
    target_stream_id bigint,
    expected_chunk_seq bigint,
    target_chunk_kind text,
    target_row_count bigint,
    target_payload_bytes bigint,
    target_chunk_lsn pg_lsn
)
RETURNS TABLE (outcome text, appended_chunk_seq bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stream_state shiba_internal.effect_streams%ROWTYPE;
    generation_published_lsn pg_lsn;
    projected_chunks numeric;
    projected_rows numeric;
    projected_bytes numeric;
BEGIN
    IF expected_chunk_seq IS NULL OR expected_chunk_seq < 1 THEN
        RAISE EXCEPTION 'expected chunk sequence must be positive'
          USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO STRICT stream_state
    FROM shiba_internal.effect_streams
    WHERE stream_id = target_stream_id
    FOR UPDATE;

    IF expected_chunk_seq <> stream_state.next_chunk_seq THEN
        RAISE EXCEPTION
          'effect stream expected chunk %, got %',
          stream_state.next_chunk_seq,
          expected_chunk_seq
          USING ERRCODE = 'serialization_failure';
    END IF;

    -- A source with no consumer has no effect to retain.  Keep only stream
    -- identity and replay CAS checks ahead of this branch so an unwatched
    -- source can never fail ingress because of payload shape or size.
    IF stream_state.producer_kind = 'source'
       AND NOT EXISTS (
         SELECT 1
         FROM shiba_internal.effect_stream_consumers
         WHERE stream_id = target_stream_id
       ) THEN
        RETURN QUERY SELECT 'discarded'::text, NULL::bigint;
        RETURN;
    END IF;

    IF stream_state.producer_kind = 'operator'
       AND NOT EXISTS (
         SELECT 1
         FROM shiba_internal.effect_stream_consumers
         WHERE stream_id = target_stream_id
       ) THEN
        RAISE EXCEPTION 'operator effect stream has no registered consumer'
          USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF expected_chunk_seq = 9223372036854775807 THEN
        RAISE EXCEPTION 'chunk sequence space exhausted'
          USING ERRCODE = 'program_limit_exceeded';
    END IF;
    IF target_chunk_kind NOT IN ('data', 'frontier')
       OR target_row_count IS NULL
       OR target_row_count < 0
       OR target_payload_bytes IS NULL
       OR target_payload_bytes < 0
       OR target_chunk_lsn IS NULL THEN
        RAISE EXCEPTION 'invalid effect stream chunk'
          USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF (
         target_chunk_kind = 'data'
         AND (
           target_row_count < 1
           OR target_payload_bytes < 1
           OR target_row_count > stream_state.target_chunk_rows
           OR (
             target_payload_bytes > stream_state.target_chunk_bytes
             AND target_row_count <> 1
           )
         )
       )
       OR (
         target_chunk_kind = 'frontier'
         AND (target_row_count <> 0 OR target_payload_bytes <> 0)
       ) THEN
        RAISE EXCEPTION
          'invalid chunk resources; only one oversized row may stand alone'
          USING ERRCODE = 'program_limit_exceeded';
    END IF;

    IF stream_state.producer_kind = 'source' THEN
        IF target_chunk_kind <> 'data' THEN
            RAISE EXCEPTION 'source streams contain data chunks only'
              USING ERRCODE = 'data_exception';
        END IF;

        SELECT replay.published_lsn
        INTO generation_published_lsn
        FROM shiba_internal.ingress_replay_state AS replay
        WHERE replay.slot_generation = stream_state.slot_generation;

        IF generation_published_lsn IS NOT NULL
           AND target_chunk_lsn <= generation_published_lsn THEN
            RAISE EXCEPTION
              'source chunk causal LSN must follow published ingress frontier'
              USING ERRCODE = 'data_exception';
        END IF;
        IF stream_state.latest_data_lsn IS NOT NULL
           AND target_chunk_lsn < stream_state.latest_data_lsn THEN
            RAISE EXCEPTION 'source chunk causal LSN is not monotonic'
              USING ERRCODE = 'data_exception';
        END IF;
    ELSIF target_chunk_kind = 'data' THEN
        IF stream_state.published_frontier_lsn IS NOT NULL
           AND target_chunk_lsn <= stream_state.published_frontier_lsn THEN
            RAISE EXCEPTION 'operator data cannot follow its causal frontier'
              USING ERRCODE = 'data_exception';
        END IF;
    ELSE
        IF (
             stream_state.published_frontier_lsn IS NOT NULL
             AND target_chunk_lsn <= stream_state.published_frontier_lsn
           )
           OR (
             stream_state.latest_data_lsn IS NOT NULL
             AND target_chunk_lsn < stream_state.latest_data_lsn
           ) THEN
            RAISE EXCEPTION 'operator frontier is not advancing past its data'
              USING ERRCODE = 'data_exception';
        END IF;
    END IF;

    IF stream_state.backpressured THEN
        RETURN QUERY SELECT 'blocked'::text, NULL::bigint;
        RETURN;
    END IF;

    projected_chunks := stream_state.buffered_chunks::numeric + 1;
    projected_rows := stream_state.buffered_rows + target_row_count;
    projected_bytes := stream_state.buffered_bytes + target_payload_bytes;

    INSERT INTO shiba_internal.effect_stream_chunks(
      stream_id,
      chunk_seq,
      chunk_kind,
      row_count,
      payload_bytes,
      chunk_lsn
    )
    VALUES (
      target_stream_id,
      expected_chunk_seq,
      target_chunk_kind,
      target_row_count,
      target_payload_bytes,
      target_chunk_lsn
    );

    UPDATE shiba_internal.effect_streams
    SET next_chunk_seq = next_chunk_seq + 1,
        latest_data_lsn = CASE
          WHEN target_chunk_kind = 'data'
          THEN greatest(latest_data_lsn, target_chunk_lsn)
          ELSE latest_data_lsn
        END,
        published_frontier_lsn = CASE
          WHEN target_chunk_kind = 'frontier' THEN target_chunk_lsn
          ELSE published_frontier_lsn
        END,
        buffered_chunks = projected_chunks::bigint,
        buffered_rows = projected_rows,
        buffered_bytes = projected_bytes,
        backpressured = (
          projected_chunks >= high_chunks
          OR projected_rows >= high_rows
          OR projected_bytes >= high_bytes
        )
    WHERE stream_id = target_stream_id;

    RETURN QUERY SELECT 'appended'::text, expected_chunk_seq;
END;
$$;

-- Advance one input cursor and its durable frontier together.  Source inputs
-- derive frontier progress from the generation-wide publication-safe LSN.
-- Operator inputs derive it only from frontier chunks consumed in this call.
CREATE FUNCTION shiba_internal.advance_effect_stream_consumer(
    target_stream_id bigint,
    target_result_oid oid,
    target_consumer_stage_id integer,
    target_input_port integer,
    expected_next_chunk_seq bigint,
    new_next_chunk_seq bigint,
    expected_consumed_frontier_lsn pg_lsn,
    new_consumed_frontier_lsn pg_lsn,
    chunk_limit integer,
    row_limit bigint,
    byte_limit bigint
)
RETURNS TABLE (
    next_chunk_seq bigint,
    consumed_frontier_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stream_state shiba_internal.effect_streams%ROWTYPE;
    current_next bigint;
    current_frontier pg_lsn;
    actual_chunks bigint;
    actual_rows numeric;
    actual_bytes numeric;
    consumed_operator_frontier pg_lsn;
    derived_operator_frontier pg_lsn;
    generation_published_lsn pg_lsn;
    next_unconsumed_lsn pg_lsn;
    updated_rows bigint;
BEGIN
    IF target_result_oid IS NULL
       OR target_consumer_stage_id IS NULL
       OR target_consumer_stage_id < 0
       OR expected_next_chunk_seq IS NULL
       OR expected_next_chunk_seq < 1
       OR new_next_chunk_seq IS NULL
       OR new_next_chunk_seq < expected_next_chunk_seq
       OR expected_consumed_frontier_lsn IS NULL
       OR new_consumed_frontier_lsn IS NULL
       OR new_consumed_frontier_lsn < expected_consumed_frontier_lsn
       OR (
         new_next_chunk_seq = expected_next_chunk_seq
         AND new_consumed_frontier_lsn = expected_consumed_frontier_lsn
       )
       OR chunk_limit IS NULL
       OR chunk_limit < 1
       OR row_limit IS NULL
       OR row_limit < 0
       OR byte_limit IS NULL
       OR byte_limit < 0
       OR (
         new_next_chunk_seq::numeric - expected_next_chunk_seq::numeric
       ) > chunk_limit THEN
        RAISE EXCEPTION 'invalid consumer advance or limits'
          USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Every operator step prelocks all input and output streams in stream_id
    -- order.  Re-locking this one row is harmless and keeps this API safe when
    -- used by source publication and GC outside an operator step.
    SELECT * INTO STRICT stream_state
    FROM shiba_internal.effect_streams
    WHERE stream_id = target_stream_id
    FOR UPDATE;

    SELECT consumer.next_chunk_seq,
           consumer.consumed_frontier_lsn
    INTO STRICT current_next, current_frontier
    FROM shiba_internal.effect_stream_consumers AS consumer
    WHERE consumer.stream_id = target_stream_id
      AND consumer.result_oid = target_result_oid
      AND consumer.consumer_stage_id = target_consumer_stage_id
      AND consumer.input_port = target_input_port
    FOR UPDATE;

    IF current_next <> expected_next_chunk_seq
       OR current_frontier <> expected_consumed_frontier_lsn THEN
        RAISE EXCEPTION
          'consumer cursor or frontier did not match expected state'
          USING ERRCODE = 'serialization_failure';
    END IF;

    IF new_next_chunk_seq > stream_state.next_chunk_seq THEN
        RAISE EXCEPTION 'consumer cannot advance beyond produced chunks'
          USING ERRCODE = 'data_exception';
    END IF;

    SELECT count(*),
           coalesce(sum(row_count), 0),
           coalesce(sum(payload_bytes), 0),
           max(chunk_lsn) FILTER (WHERE chunk_kind = 'frontier')
    INTO actual_chunks,
         actual_rows,
         actual_bytes,
         consumed_operator_frontier
    FROM shiba_internal.effect_stream_chunks
    WHERE stream_id = target_stream_id
      AND chunk_seq >= expected_next_chunk_seq
      AND chunk_seq < new_next_chunk_seq;

    IF actual_chunks <> new_next_chunk_seq - expected_next_chunk_seq THEN
        RAISE EXCEPTION 'consumer range is not fully retained'
          USING ERRCODE = 'data_exception';
    END IF;
    IF actual_chunks > 1
       AND (actual_rows > row_limit OR actual_bytes > byte_limit) THEN
        RAISE EXCEPTION 'consumer advance exceeds its row or byte limit'
          USING ERRCODE = 'program_limit_exceeded';
    END IF;

    IF stream_state.producer_kind = 'source' THEN
        SELECT replay.published_lsn
        INTO generation_published_lsn
        FROM shiba_internal.ingress_replay_state AS replay
        WHERE replay.slot_generation = stream_state.slot_generation;

        IF new_consumed_frontier_lsn > current_frontier
           AND (
             generation_published_lsn IS NULL
             OR new_consumed_frontier_lsn > generation_published_lsn
           ) THEN
            RAISE EXCEPTION
              'source consumer cannot pass published ingress frontier'
              USING ERRCODE = 'data_exception';
        END IF;

        SELECT chunk.chunk_lsn
        INTO next_unconsumed_lsn
        FROM shiba_internal.effect_stream_chunks AS chunk
        WHERE chunk.stream_id = target_stream_id
          AND chunk.chunk_seq >= new_next_chunk_seq
        ORDER BY chunk.chunk_seq
        LIMIT 1;

        IF next_unconsumed_lsn IS NOT NULL
           AND next_unconsumed_lsn <= new_consumed_frontier_lsn THEN
            RAISE EXCEPTION
              'source consumer frontier would skip published data'
              USING ERRCODE = 'data_exception';
        END IF;
    ELSE
        derived_operator_frontier := greatest(
          current_frontier,
          coalesce(consumed_operator_frontier, current_frontier)
        );
        IF new_consumed_frontier_lsn <> derived_operator_frontier THEN
            RAISE EXCEPTION
              'operator consumer frontier must match consumed frontier chunks'
              USING ERRCODE = 'data_exception';
        END IF;
    END IF;

    UPDATE shiba_internal.effect_stream_consumers AS consumer
    SET next_chunk_seq = new_next_chunk_seq,
        consumed_frontier_lsn = new_consumed_frontier_lsn,
        updated_at = clock_timestamp()
    WHERE consumer.stream_id = target_stream_id
      AND consumer.result_oid = target_result_oid
      AND consumer.consumer_stage_id = target_consumer_stage_id
      AND consumer.input_port = target_input_port
      AND consumer.next_chunk_seq = expected_next_chunk_seq
      AND consumer.consumed_frontier_lsn = expected_consumed_frontier_lsn;

    GET DIAGNOSTICS updated_rows = ROW_COUNT;
    IF updated_rows <> 1 THEN
        RAISE EXCEPTION 'consumer CAS update did not affect one row'
          USING ERRCODE = 'serialization_failure';
    END IF;

    RETURN QUERY
    SELECT new_next_chunk_seq, new_consumed_frontier_lsn;
END;
$$;

CREATE FUNCTION shiba_internal.gc_effect_stream(
    target_stream_id bigint,
    chunk_limit integer,
    row_limit bigint,
    byte_limit bigint
)
RETURNS TABLE (
    deleted_chunks bigint,
    deleted_rows numeric,
    deleted_bytes numeric,
    first_retained_chunk_seq bigint,
    backpressured boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stream_state shiba_internal.effect_streams%ROWTYPE;
    slowest_next bigint;
    removed_chunks bigint := 0;
    removed_rows numeric := 0;
    removed_bytes numeric := 0;
    last_removed_seq bigint;
    remaining_chunks bigint;
    remaining_rows numeric;
    remaining_bytes numeric;
    retained_seq bigint;
    remains_backpressured boolean;
BEGIN
    IF chunk_limit IS NULL OR chunk_limit < 1
       OR row_limit IS NULL OR row_limit < 0
       OR byte_limit IS NULL OR byte_limit < 0 THEN
        RAISE EXCEPTION 'invalid GC limits'
          USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO STRICT stream_state
    FROM shiba_internal.effect_streams
    WHERE stream_id = target_stream_id
    FOR UPDATE;

    SELECT min(next_chunk_seq) INTO slowest_next
    FROM shiba_internal.effect_stream_consumers
    WHERE stream_id = target_stream_id;

    IF slowest_next IS NULL AND stream_state.producer_kind = 'source' THEN
        slowest_next := stream_state.next_chunk_seq;
    END IF;
    IF slowest_next IS NULL THEN
        RETURN QUERY
        SELECT 0::bigint,
               0::numeric,
               0::numeric,
               stream_state.first_retained_chunk_seq,
               stream_state.backpressured;
        RETURN;
    END IF;

    WITH limited AS MATERIALIZED (
      SELECT chunk.chunk_seq, chunk.row_count, chunk.payload_bytes
      FROM shiba_internal.effect_stream_chunks AS chunk
      WHERE chunk.stream_id = target_stream_id
        AND chunk.chunk_seq < slowest_next
      ORDER BY chunk.chunk_seq
      LIMIT chunk_limit
    ),
    measured AS (
      SELECT limited.*,
             row_number() OVER (ORDER BY limited.chunk_seq) AS ordinal,
             sum(limited.row_count) OVER (
               ORDER BY limited.chunk_seq ROWS UNBOUNDED PRECEDING
             ) AS running_rows,
             sum(limited.payload_bytes) OVER (
               ORDER BY limited.chunk_seq ROWS UNBOUNDED PRECEDING
             ) AS running_bytes
      FROM limited
    ),
    garbage AS (
      SELECT measured.chunk_seq
      FROM measured
      WHERE measured.ordinal = 1
         OR (
           measured.running_rows <= row_limit
           AND measured.running_bytes <= byte_limit
         )
    ),
    deleted AS (
      DELETE FROM shiba_internal.effect_stream_chunks AS chunk
      USING garbage
      WHERE chunk.stream_id = target_stream_id
        AND chunk.chunk_seq = garbage.chunk_seq
      RETURNING chunk.chunk_seq, chunk.row_count, chunk.payload_bytes
    )
    SELECT count(*),
           coalesce(sum(row_count), 0),
           coalesce(sum(payload_bytes), 0),
           max(chunk_seq)
    INTO removed_chunks, removed_rows, removed_bytes, last_removed_seq
    FROM deleted;

    remaining_chunks := stream_state.buffered_chunks - removed_chunks;
    remaining_rows := stream_state.buffered_rows - removed_rows;
    remaining_bytes := stream_state.buffered_bytes - removed_bytes;
    retained_seq := coalesce(
      last_removed_seq + 1,
      stream_state.first_retained_chunk_seq
    );
    remains_backpressured := stream_state.backpressured
      AND NOT (
        remaining_chunks <= stream_state.low_chunks
        AND remaining_rows <= stream_state.low_rows
        AND remaining_bytes <= stream_state.low_bytes
      );

    UPDATE shiba_internal.effect_streams
    SET first_retained_chunk_seq = retained_seq,
        buffered_chunks = remaining_chunks,
        buffered_rows = remaining_rows,
        buffered_bytes = remaining_bytes,
        backpressured = remains_backpressured
    WHERE stream_id = target_stream_id;

    RETURN QUERY
    SELECT removed_chunks,
           removed_rows,
           removed_bytes,
           retained_seq,
           remains_backpressured;
END;
$$;

CREATE FUNCTION shiba_internal.reject_effect_stream_chunk_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'effect stream chunks are immutable'
      USING ERRCODE = 'object_not_in_prerequisite_state';
END;
$$;

CREATE TRIGGER effect_stream_chunks_are_immutable
BEFORE UPDATE ON shiba_internal.effect_stream_chunks
FOR EACH ROW
EXECUTE FUNCTION shiba_internal.reject_effect_stream_chunk_update();

REVOKE ALL ON FUNCTION
  shiba_internal.effect_row_bytes(anyelement)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.attach_effect_stream_consumer(
    bigint, oid, integer, integer, pg_lsn
  )
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.append_effect_stream_chunk(
    bigint, bigint, text, bigint, bigint, pg_lsn
  )
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.advance_effect_stream_consumer(
    bigint, oid, integer, integer, bigint, bigint,
    pg_lsn, pg_lsn, integer, bigint, bigint
  )
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.gc_effect_stream(bigint, integer, bigint, bigint)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.reject_effect_stream_chunk_update()
  FROM PUBLIC;
