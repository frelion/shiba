-- V2 durable ingress fixed-shape SQL API.
--
-- CALLING TRANSACTION BOUNDARIES
-- --------------------------------
-- These functions never commit and never perform replication I/O.  The
-- Runtime must call claim/create, event insertion(s), and record_batch inside
-- one bounded SPI transaction, then commit before reading or waiting on the
-- replication socket.  Commit/abort finalization is a separate short SPI
-- transaction.  Read feedback_upper_bound in a short SPI transaction, end
-- that transaction, and only then send Standby Status Update.  After a
-- successful send, record the confirmation intent in another short SPI
-- transaction with v2_record_ingress_feedback.
--
-- GLOBAL ROW-LOCK ORDER
-- --------------------------------
-- 0. v2_ingress_replay_state table lock (generation creation only)
-- 1. v2_ingress_replay_state rows (ascending slot_generation)
-- 2. v2_ingress_transactions rows (ascending ingress_txn_id)
-- 3. v2_change_log stable identity / v2_ingress_decode_batches
-- 4. v2_ingress_sources
-- 5. v2_routing_tasks
--
-- Every mutating function below follows the applicable prefix of this order.
-- PostgreSQL retains the row locks until the caller commits or rolls back the
-- surrounding transaction.  A retry after deadlock or serialization failure
-- must replay the complete bounded SPI transaction.
--
-- All names in SECURITY DEFINER bodies are schema-qualified and search_path is
-- restricted.  PUBLIC receives no EXECUTE privilege; the extension owner
-- invokes these functions from the Runtime backend.

CREATE FUNCTION shiba_internal.v2_ensure_ingress_generation(
    p_slot_name name
)
RETURNS TABLE (
    slot_generation bigint,
    created boolean,
    persisted_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_database_oid oid;
    v_slot_generation bigint;
    v_max_generation bigint;
BEGIN
    IF p_slot_name IS NULL OR length(p_slot_name::text) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'v2 ingress slot name must not be NULL or empty';
    END IF;

    SELECT database.oid
      INTO STRICT v_database_oid
      FROM pg_catalog.pg_database AS database
     WHERE database.datname = pg_catalog.current_database();

    -- Lock-order level 0.  SHARE ROW EXCLUSIVE serializes generation
    -- allocation with concurrent lifecycle calls and ordinary table writers.
    -- Keep this lifecycle transaction short; the table lock is released by
    -- caller COMMIT.
    LOCK TABLE shiba_internal.v2_ingress_replay_state
        IN SHARE ROW EXCLUSIVE MODE;

    SELECT replay.slot_generation
      INTO v_slot_generation
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.database_oid = v_database_oid
       AND replay.slot_name = p_slot_name
       AND replay.state = 'active'
     FOR UPDATE;

    IF FOUND THEN
        created := false;
    ELSE
        SELECT max(replay.slot_generation)
          INTO v_max_generation
          FROM shiba_internal.v2_ingress_replay_state AS replay;

        IF v_max_generation = 9223372036854775807 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'v2 ingress slot generation exhausted bigint range';
        END IF;

        v_slot_generation := coalesce(v_max_generation, 0) + 1;

        INSERT INTO shiba_internal.v2_ingress_replay_state (
            slot_generation,
            slot_name,
            database_oid
        )
        VALUES (
            v_slot_generation,
            p_slot_name,
            v_database_oid
        );
        created := true;
    END IF;

    RETURN QUERY
    SELECT replay.slot_generation,
           created,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation;
END;
$$;

CREATE FUNCTION shiba_internal.v2_claim_ingress_txn(
    p_slot_generation bigint,
    p_source_xid bigint,
    p_first_stream_lsn pg_lsn
)
RETURNS TABLE (
    ingress_txn_id bigint,
    txn_status text,
    next_input_seq bigint,
    event_count bigint,
    payload_bytes bigint,
    created boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_ingress_txn_id bigint;
    v_created boolean;
BEGIN
    IF p_slot_generation IS NULL
       OR p_source_xid IS NULL
       OR p_first_stream_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress transaction identity must not contain NULL';
    END IF;

    -- Lock-order level 1: generation/replay state.
    PERFORM 1
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'v2 ingress slot generation %s is absent, inactive, or belongs to another database',
                p_slot_generation
            );
    END IF;

    INSERT INTO shiba_internal.v2_ingress_transactions (
        slot_generation,
        source_xid,
        first_stream_lsn
    )
    VALUES (
        p_slot_generation,
        p_source_xid,
        p_first_stream_lsn
    )
    ON CONFLICT (slot_generation, source_xid, first_stream_lsn)
        DO NOTHING
    RETURNING v2_ingress_transactions.ingress_txn_id
         INTO v_ingress_txn_id;

    v_created := FOUND;

    IF NOT v_created THEN
        -- Lock-order level 2: transaction header.
        SELECT txn.ingress_txn_id
          INTO STRICT v_ingress_txn_id
          FROM shiba_internal.v2_ingress_transactions AS txn
         WHERE txn.slot_generation = p_slot_generation
           AND txn.source_xid = p_source_xid
           AND txn.first_stream_lsn = p_first_stream_lsn
         FOR UPDATE;
    END IF;

    RETURN QUERY
    SELECT txn.ingress_txn_id,
           txn.status,
           txn.next_input_seq,
           txn.event_count,
           txn.payload_bytes,
           v_created
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = v_ingress_txn_id;
END;
$$;

CREATE FUNCTION shiba_internal.v2_insert_ingress_event(
    p_ingress_txn_id bigint,
    p_change_lsn pg_lsn,
    p_change_ordinal bigint,
    p_image_ordinal integer,
    p_source_oid oid,
    p_weight bigint,
    p_typed_payload jsonb
)
RETURNS TABLE (
    input_seq bigint,
    inserted boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_first_stream_lsn pg_lsn;
    v_next_input_seq bigint;
    v_existing_source_oid oid;
    v_existing_weight bigint;
    v_existing_payload jsonb;
    v_payload_bytes bigint;
BEGIN
    IF p_ingress_txn_id IS NULL
       OR p_change_lsn IS NULL
       OR p_change_ordinal IS NULL
       OR p_image_ordinal IS NULL
       OR p_source_oid IS NULL
       OR p_weight IS NULL
       OR p_typed_payload IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress event fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order level 1.
    PERFORM 1
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'v2 ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    -- Lock-order level 2.  This serializes input_seq allocation for one source
    -- transaction and makes replay retain the first allocation.
    SELECT txn.status,
           txn.first_stream_lsn,
           txn.next_input_seq
      INTO STRICT v_status,
                  v_first_stream_lsn,
                  v_next_input_seq
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF v_status <> 'open' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'cannot append to v2 ingress transaction %s in state %s',
                p_ingress_txn_id,
                v_status
            );
    END IF;

    IF p_change_lsn < v_first_stream_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'event LSN %s precedes transaction first-stream LSN %s',
                p_change_lsn,
                v_first_stream_lsn
            );
    END IF;

    SELECT event.input_seq,
           event.source_oid,
           event.weight,
           event.typed_payload
      INTO input_seq,
           v_existing_source_oid,
           v_existing_weight,
           v_existing_payload
      FROM shiba_internal.v2_change_log AS event
     WHERE event.ingress_txn_id = p_ingress_txn_id
       AND event.change_lsn = p_change_lsn
       AND event.change_ordinal = p_change_ordinal
       AND event.image_ordinal = p_image_ordinal;

    IF FOUND THEN
        IF v_existing_source_oid IS DISTINCT FROM p_source_oid
           OR v_existing_weight IS DISTINCT FROM p_weight
           OR v_existing_payload IS DISTINCT FROM p_typed_payload THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'v2 ingress event identity conflict for transaction %s at (%s,%s,%s)',
                    p_ingress_txn_id,
                    p_change_lsn,
                    p_change_ordinal,
                    p_image_ordinal
                );
        END IF;

        inserted := false;
        RETURN NEXT;
        RETURN;
    END IF;

    v_payload_bytes :=
        pg_catalog.octet_length(pg_catalog.jsonb_send(p_typed_payload))::bigint;
    input_seq := v_next_input_seq;

    -- Lock-order level 3: stable event identity.
    INSERT INTO shiba_internal.v2_change_log (
        ingress_txn_id,
        change_lsn,
        change_ordinal,
        image_ordinal,
        input_seq,
        source_oid,
        weight,
        typed_payload,
        payload_bytes
    )
    VALUES (
        p_ingress_txn_id,
        p_change_lsn,
        p_change_ordinal,
        p_image_ordinal,
        input_seq,
        p_source_oid,
        p_weight,
        p_typed_payload,
        v_payload_bytes
    );

    -- Lock-order level 4: per-transaction source set.
    INSERT INTO shiba_internal.v2_ingress_sources (
        ingress_txn_id,
        source_oid
    )
    VALUES (
        p_ingress_txn_id,
        p_source_oid
    )
    ON CONFLICT (ingress_txn_id, source_oid) DO NOTHING;

    UPDATE shiba_internal.v2_ingress_transactions AS txn
       SET next_input_seq = txn.next_input_seq + 1,
           event_count = txn.event_count + 1,
           payload_bytes = txn.payload_bytes + v_payload_bytes
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    inserted := true;
    RETURN NEXT;
END;
$$;

-- One Runtime/SPI call inserts a bounded array in wire order.  Each element
-- has this fixed JSONB shape:
-- {
--   "change_lsn": "0/16B6A20",
--   "change_ordinal": 0,
--   "image_ordinal": 0,
--   "source_oid": 16384,
--   "weight": 1,
--   "typed_payload": {...}
-- }
--
-- The first implementation deliberately delegates each element to the single
-- event function so identity-conflict semantics have one implementation.
-- This removes per-row SPI round trips; a later set-oriented SQL body may
-- replace the loop without changing the API.
CREATE FUNCTION shiba_internal.v2_insert_ingress_events(
    p_ingress_txn_id bigint,
    p_events jsonb
)
RETURNS TABLE (
    inserted_count bigint,
    replayed_count bigint,
    first_input_seq bigint,
    last_input_seq bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_event jsonb;
    v_array_ordinal bigint;
    v_input_seq bigint;
    v_inserted boolean;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_events IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress event batch fields must not contain NULL';
    END IF;

    IF pg_catalog.jsonb_typeof(p_events) <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'v2 ingress event batch must be a JSONB array';
    END IF;

    inserted_count := 0;
    replayed_count := 0;

    FOR v_event, v_array_ordinal IN
        SELECT item.value, item.ordinality
          FROM pg_catalog.jsonb_array_elements(p_events)
               WITH ORDINALITY AS item(value, ordinality)
         ORDER BY item.ordinality
    LOOP
        IF pg_catalog.jsonb_typeof(v_event) <> 'object'
           OR NOT (
               v_event ? 'change_lsn'
               AND v_event ? 'change_ordinal'
               AND v_event ? 'image_ordinal'
               AND v_event ? 'source_oid'
               AND v_event ? 'weight'
               AND v_event ? 'typed_payload'
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format(
                    'v2 ingress event batch element %s has invalid shape',
                    v_array_ordinal
                );
        END IF;

        SELECT event_result.input_seq,
               event_result.inserted
          INTO STRICT v_input_seq,
                      v_inserted
          FROM shiba_internal.v2_insert_ingress_event(
                   p_ingress_txn_id,
                   (v_event ->> 'change_lsn')::pg_lsn,
                   (v_event ->> 'change_ordinal')::bigint,
                   (v_event ->> 'image_ordinal')::integer,
                   (v_event ->> 'source_oid')::oid,
                   (v_event ->> 'weight')::bigint,
                   v_event -> 'typed_payload'
               ) AS event_result;

        IF first_input_seq IS NULL THEN
            first_input_seq := v_input_seq;
        END IF;
        last_input_seq := v_input_seq;

        IF v_inserted THEN
            inserted_count := inserted_count + 1;
        ELSE
            replayed_count := replayed_count + 1;
        END IF;
    END LOOP;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.v2_record_ingress_batch(
    p_slot_generation bigint,
    p_decode_end_lsn pg_lsn,
    p_message_digest bytea,
    p_event_count bigint,
    p_payload_bytes bigint
)
RETURNS TABLE (
    inserted boolean,
    persisted_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_existing_digest bytea;
    v_existing_event_count bigint;
    v_existing_payload_bytes bigint;
BEGIN
    IF p_slot_generation IS NULL
       OR p_decode_end_lsn IS NULL
       OR p_message_digest IS NULL
       OR p_event_count IS NULL
       OR p_payload_bytes IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress batch fields must not contain NULL';
    END IF;

    -- Lock-order level 1.  This serializes persisted_lsn advancement.
    PERFORM 1
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'v2 ingress slot generation %s is not active',
                p_slot_generation
            );
    END IF;

    -- Lock-order level 3: batch identity.  Event rows inserted earlier in this
    -- same caller transaction become durable atomically with this descriptor.
    INSERT INTO shiba_internal.v2_ingress_decode_batches (
        slot_generation,
        decode_end_lsn,
        message_digest,
        event_count,
        payload_bytes
    )
    VALUES (
        p_slot_generation,
        p_decode_end_lsn,
        p_message_digest,
        p_event_count,
        p_payload_bytes
    )
    ON CONFLICT (slot_generation, decode_end_lsn, message_digest) DO NOTHING;

    inserted := FOUND;

    SELECT batch.message_digest,
           batch.event_count,
           batch.payload_bytes
      INTO STRICT v_existing_digest,
                  v_existing_event_count,
                  v_existing_payload_bytes
     FROM shiba_internal.v2_ingress_decode_batches AS batch
     WHERE batch.slot_generation = p_slot_generation
       AND batch.decode_end_lsn = p_decode_end_lsn
       AND batch.message_digest = p_message_digest
     FOR KEY SHARE;

    IF v_existing_digest IS DISTINCT FROM p_message_digest
       OR v_existing_event_count IS DISTINCT FROM p_event_count
       OR v_existing_payload_bytes IS DISTINCT FROM p_payload_bytes THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'v2 ingress batch identity conflict for generation %s at %s',
                p_slot_generation,
                p_decode_end_lsn
            );
    END IF;

    UPDATE shiba_internal.v2_ingress_replay_state AS replay
       SET persisted_lsn = CASE
               WHEN replay.persisted_lsn IS NULL
                 OR replay.persisted_lsn < p_decode_end_lsn
               THEN p_decode_end_lsn
               ELSE replay.persisted_lsn
           END,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = p_slot_generation
     RETURNING replay.persisted_lsn
          INTO persisted_lsn;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.v2_commit_ingress_txn(
    p_ingress_txn_id bigint,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn
)
RETURNS TABLE (
    finalized boolean,
    routing_task_created boolean,
    event_count bigint,
    payload_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_first_stream_lsn pg_lsn;
    v_existing_commit_lsn pg_lsn;
    v_existing_end_lsn pg_lsn;
BEGIN
    IF p_ingress_txn_id IS NULL
       OR p_commit_lsn IS NULL
       OR p_end_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress commit fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order levels 1 then 2.
    PERFORM 1
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
     FOR UPDATE;

    SELECT txn.status,
           txn.first_stream_lsn,
           txn.commit_lsn,
           txn.end_lsn,
           txn.event_count,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_first_stream_lsn,
                  v_existing_commit_lsn,
                  v_existing_end_lsn,
                  event_count,
                  payload_bytes
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF p_commit_lsn < v_first_stream_lsn OR p_end_lsn < p_commit_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'invalid commit/end LSNs %s/%s for transaction beginning at %s',
                p_commit_lsn,
                p_end_lsn,
                v_first_stream_lsn
            );
    END IF;

    IF v_status = 'aborted' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'cannot commit aborted v2 ingress transaction %s',
                p_ingress_txn_id
            );
    ELSIF v_status = 'committed' THEN
        IF v_existing_commit_lsn IS DISTINCT FROM p_commit_lsn
           OR v_existing_end_lsn IS DISTINCT FROM p_end_lsn THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'v2 ingress commit identity conflict for transaction %s',
                    p_ingress_txn_id
                );
        END IF;
        finalized := false;
    ELSE
        -- Header-only finalization: no v2_change_log row is scanned or updated.
        UPDATE shiba_internal.v2_ingress_transactions AS txn
           SET status = 'committed',
               commit_lsn = p_commit_lsn,
               end_lsn = p_end_lsn,
               finalized_at = clock_timestamp()
         WHERE txn.ingress_txn_id = p_ingress_txn_id;
        finalized := true;
    END IF;

    -- Lock-order level 5: one routing task, never subscriber fan-out here.
    INSERT INTO shiba_internal.v2_routing_tasks (
        ingress_txn_id,
        commit_lsn
    )
    VALUES (
        p_ingress_txn_id,
        p_commit_lsn
    )
    ON CONFLICT (ingress_txn_id) DO NOTHING;

    routing_task_created := FOUND;

    IF NOT routing_task_created
       AND EXISTS (
           SELECT 1
             FROM shiba_internal.v2_routing_tasks AS task
            WHERE task.ingress_txn_id = p_ingress_txn_id
              AND task.commit_lsn IS DISTINCT FROM p_commit_lsn
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'v2 routing task identity conflict for transaction %s',
                p_ingress_txn_id
            );
    END IF;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.v2_abort_ingress_txn(
    p_ingress_txn_id bigint,
    p_end_lsn pg_lsn
)
RETURNS TABLE (
    finalized boolean,
    event_count bigint,
    payload_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_first_stream_lsn pg_lsn;
    v_existing_end_lsn pg_lsn;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_end_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress abort fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order levels 1 then 2.
    PERFORM 1
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
     FOR UPDATE;

    SELECT txn.status,
           txn.first_stream_lsn,
           txn.end_lsn,
           txn.event_count,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_first_stream_lsn,
                  v_existing_end_lsn,
                  event_count,
                  payload_bytes
      FROM shiba_internal.v2_ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF p_end_lsn < v_first_stream_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'abort LSN %s precedes transaction first-stream LSN %s',
                p_end_lsn,
                v_first_stream_lsn
            );
    END IF;

    IF v_status = 'committed' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'cannot abort committed v2 ingress transaction %s',
                p_ingress_txn_id
            );
    ELSIF v_status = 'aborted' THEN
        IF v_existing_end_lsn IS DISTINCT FROM p_end_lsn THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'v2 ingress abort identity conflict for transaction %s',
                    p_ingress_txn_id
                );
        END IF;
        finalized := false;
    ELSE
        UPDATE shiba_internal.v2_ingress_transactions AS txn
           SET status = 'aborted',
               end_lsn = p_end_lsn,
               finalized_at = clock_timestamp()
         WHERE txn.ingress_txn_id = p_ingress_txn_id;
        finalized := true;
    END IF;

    IF EXISTS (
        SELECT 1
          FROM shiba_internal.v2_routing_tasks AS task
         WHERE task.ingress_txn_id = p_ingress_txn_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'aborted v2 ingress transaction %s has a routing task',
                p_ingress_txn_id
            );
    END IF;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.v2_feedback_upper_bound(
    p_slot_generation bigint
)
RETURNS TABLE (
    slot_generation bigint,
    persisted_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT replay.slot_generation,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
$$;

-- Record feedback only after the replication feedback write succeeds.  This
-- is monotonic confirmation intent; it MUST NOT advance replay_safe_lsn.
CREATE FUNCTION shiba_internal.v2_record_ingress_feedback(
    p_slot_generation bigint,
    p_confirmed_lsn pg_lsn
)
RETURNS TABLE (
    confirmed_lsn pg_lsn,
    persisted_lsn pg_lsn,
    replay_safe_lsn pg_lsn,
    advanced boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_persisted_lsn pg_lsn;
    v_confirmed_lsn pg_lsn;
    v_replay_safe_lsn pg_lsn;
BEGIN
    IF p_slot_generation IS NULL OR p_confirmed_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'v2 ingress feedback fields must not contain NULL';
    END IF;

    -- Lock-order level 1.  The row lock and update are transaction-scoped.
    SELECT replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      INTO STRICT v_persisted_lsn,
                  v_confirmed_lsn,
                  v_replay_safe_lsn
      FROM shiba_internal.v2_ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
     FOR UPDATE;

    IF v_persisted_lsn IS NULL OR p_confirmed_lsn > v_persisted_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'feedback LSN %s exceeds persisted ingress LSN %s for generation %s',
                p_confirmed_lsn,
                coalesce(v_persisted_lsn::text, 'NULL'),
                p_slot_generation
            );
    END IF;

    advanced := v_confirmed_lsn IS NULL
                OR p_confirmed_lsn > v_confirmed_lsn;

    IF advanced THEN
        UPDATE shiba_internal.v2_ingress_replay_state AS replay
           SET confirmed_lsn = p_confirmed_lsn,
               updated_at = clock_timestamp()
         WHERE replay.slot_generation = p_slot_generation;
        v_confirmed_lsn := p_confirmed_lsn;
    END IF;

    confirmed_lsn := v_confirmed_lsn;
    persisted_lsn := v_persisted_lsn;
    replay_safe_lsn := v_replay_safe_lsn;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION
    shiba_internal.v2_ensure_ingress_generation(name)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_claim_ingress_txn(bigint, bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_insert_ingress_event(
        bigint, pg_lsn, bigint, integer, oid, bigint, jsonb
    )
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_insert_ingress_events(bigint, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_record_ingress_batch(
        bigint, pg_lsn, bytea, bigint, bigint
    )
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_commit_ingress_txn(bigint, pg_lsn, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_abort_ingress_txn(bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_feedback_upper_bound(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.v2_record_ingress_feedback(bigint, pg_lsn)
    FROM PUBLIC;
