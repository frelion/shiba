-- Durable ingress fixed-shape SQL API.
--
-- CALLING TRANSACTION BOUNDARIES
-- --------------------------------
-- These functions never commit and never perform replication I/O.  The
-- Runtime must call claim/create, event insertion(s), record_batch, and any
-- Commit finalization for that batch inside one bounded SPI transaction, then
-- commit before reading or waiting on the replication socket. Prefix batches
-- do not advance durable replication progress; commit_ingress_transaction
-- atomically finalizes the header, creates routing work, and advances it.
-- Read feedback_upper_bound in a short SPI transaction, end that transaction,
-- and only then send Standby Status Update.  After a
-- successful send, record the confirmation intent in another short SPI
-- transaction with record_ingress_feedback.
--
-- GLOBAL ROW-LOCK ORDER
-- --------------------------------
-- 0. ingress_replay_state table lock (generation creation only)
-- 1. ingress_replay_state rows (ascending slot_generation)
-- 2. ingress_transactions rows (ascending ingress_txn_id)
-- 3. change_log stable identity / ingress_decode_batches
-- 4. ingress_sources
-- 5. routing_tasks
--
-- Every mutating function below follows the applicable prefix of this order.
-- PostgreSQL retains the row locks until the caller commits or rolls back the
-- surrounding transaction.  A retry after deadlock or serialization failure
-- must replay the complete bounded SPI transaction.
--
-- All names in SECURITY DEFINER bodies are schema-qualified and search_path is
-- restricted.  PUBLIC receives no EXECUTE privilege; the extension owner
-- invokes these functions from the Runtime backend.

CREATE FUNCTION shiba_internal.ensure_ingress_generation(
    p_slot_name name,
    p_force_new boolean DEFAULT false
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
    v_system_identifier text;
    v_slot_database name;
    v_slot_plugin name;
    v_slot_confirmed_lsn pg_lsn;
    v_persisted_lsn pg_lsn;
    v_baseline_lsn pg_lsn;
BEGIN
    IF p_slot_name IS NULL
       OR length(p_slot_name::text) = 0
       OR p_force_new IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'ingress slot name must not be NULL or empty';
    END IF;

    SELECT database.oid
      INTO STRICT v_database_oid
      FROM pg_catalog.pg_database AS database
     WHERE database.datname = pg_catalog.current_database();

    SELECT control.system_identifier::text
      INTO STRICT v_system_identifier
      FROM pg_catalog.pg_control_system() AS control;

    SELECT slot.database,
           slot.plugin,
           slot.confirmed_flush_lsn
      INTO v_slot_database,
           v_slot_plugin,
           v_slot_confirmed_lsn
      FROM pg_catalog.pg_replication_slots AS slot
     WHERE slot.slot_name = p_slot_name::text;

    IF NOT FOUND
       OR v_slot_database IS DISTINCT FROM pg_catalog.current_database()::name
       OR v_slot_plugin IS DISTINCT FROM 'pgoutput'::name
       OR v_slot_confirmed_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical slot %s is absent or is not this database''s pgoutput slot',
                p_slot_name
            );
    END IF;

    -- Lock-order level 0.  SHARE ROW EXCLUSIVE serializes generation
    -- allocation with concurrent lifecycle calls and ordinary table writers.
    -- Keep this lifecycle transaction short; the table lock is released by
    -- caller COMMIT.
    LOCK TABLE shiba_internal.ingress_replay_state
        IN SHARE ROW EXCLUSIVE MODE;

    SELECT replay.slot_generation
      INTO v_slot_generation
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.database_oid = v_database_oid
       AND replay.slot_name = p_slot_name
       AND replay.state = 'active'
     FOR UPDATE;

    IF FOUND AND p_force_new THEN
        UPDATE shiba_internal.ingress_replay_state AS replay
           SET state = 'retired',
               retired_at = clock_timestamp(),
               updated_at = clock_timestamp()
         WHERE replay.slot_generation = v_slot_generation;
        v_slot_generation := NULL;
    ELSIF FOUND THEN
        SELECT replay.persisted_lsn,
               replay.slot_baseline_lsn
          INTO v_persisted_lsn,
               v_baseline_lsn
          FROM shiba_internal.ingress_replay_state AS replay
         WHERE replay.slot_generation = v_slot_generation
           AND replay.system_identifier = v_system_identifier;

        IF NOT FOUND
           OR v_slot_confirmed_lsn < v_baseline_lsn
           OR (v_persisted_lsn IS NULL
               AND v_slot_confirmed_lsn <> v_baseline_lsn)
           OR (v_persisted_lsn IS NOT NULL
               AND v_slot_confirmed_lsn > v_persisted_lsn) THEN
            -- Fail closed without pretending to persist an `invalid` state:
            -- the exception rolls back this lifecycle transaction. Recovery
            -- requires repairing/replacing the mismatched slot explicitly.
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'logical slot %s no longer matches durable ingress generation %s',
                    p_slot_name,
                    v_slot_generation
                );
        END IF;
        created := false;
    END IF;

    IF v_slot_generation IS NULL THEN
        SELECT max(replay.slot_generation)
          INTO v_max_generation
          FROM shiba_internal.ingress_replay_state AS replay;

        IF v_max_generation = 9223372036854775807 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress slot generation exhausted bigint range';
        END IF;

        v_slot_generation := coalesce(v_max_generation, 0) + 1;

        INSERT INTO shiba_internal.ingress_replay_state (
            slot_generation,
            slot_name,
            database_oid,
            system_identifier,
            slot_baseline_lsn
        )
        VALUES (
            v_slot_generation,
            p_slot_name,
            v_database_oid,
            v_system_identifier,
            v_slot_confirmed_lsn
        );
        created := true;
    END IF;

    RETURN QUERY
    SELECT replay.slot_generation,
           created,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation;
END;
$$;

CREATE FUNCTION shiba_internal.retire_ingress_generation(
    p_slot_name name
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    UPDATE shiba_internal.ingress_replay_state AS replay
       SET state = 'retired',
           retired_at = clock_timestamp(),
           updated_at = clock_timestamp()
     WHERE replay.database_oid = (
               SELECT database.oid
                 FROM pg_catalog.pg_database AS database
                WHERE database.datname = pg_catalog.current_database()
           )
       AND replay.slot_name = p_slot_name
       AND replay.state = 'active';
    RETURN FOUND;
END;
$$;

CREATE FUNCTION shiba_internal.claim_ingress_transaction(
    p_slot_generation bigint,
    p_source_xid bigint,
    p_final_lsn pg_lsn
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
       OR p_final_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress transaction identity must not contain NULL';
    END IF;

    -- Lock-order level 1: generation/replay state.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
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
                'ingress slot generation %s is absent, inactive, or belongs to another database',
                p_slot_generation
            );
    END IF;

    INSERT INTO shiba_internal.ingress_transactions (
        slot_generation,
        source_xid,
        final_lsn
    )
    VALUES (
        p_slot_generation,
        p_source_xid,
        p_final_lsn
    )
    ON CONFLICT (slot_generation, source_xid, final_lsn)
        DO NOTHING
    RETURNING ingress_transactions.ingress_txn_id
         INTO v_ingress_txn_id;

    v_created := FOUND;

    IF NOT v_created THEN
        -- Lock-order level 2: transaction header.
        SELECT txn.ingress_txn_id
          INTO STRICT v_ingress_txn_id
         FROM shiba_internal.ingress_transactions AS txn
         WHERE txn.slot_generation = p_slot_generation
           AND txn.source_xid = p_source_xid
           AND txn.final_lsn = p_final_lsn
         FOR UPDATE;
    END IF;

    RETURN QUERY
    SELECT txn.ingress_txn_id,
           txn.status,
           txn.next_input_seq,
           txn.event_count,
           txn.payload_bytes,
           v_created
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = v_ingress_txn_id;
END;
$$;

CREATE FUNCTION shiba_internal.insert_ingress_event(
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
            MESSAGE = 'ingress event fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order level 1.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    -- Lock-order level 2.  This serializes input_seq allocation for one source
    -- transaction and makes replay retain the first allocation.
    SELECT txn.status,
           txn.next_input_seq
      INTO STRICT v_status,
                  v_next_input_seq
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF v_status <> 'open' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'cannot append to ingress transaction %s in state %s',
                p_ingress_txn_id,
                v_status
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
      FROM shiba_internal.change_log AS event
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
                    'ingress event identity conflict for transaction %s at (%s,%s,%s)',
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
    INSERT INTO shiba_internal.change_log (
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
    INSERT INTO shiba_internal.ingress_sources (
        ingress_txn_id,
        source_oid
    )
    VALUES (
        p_ingress_txn_id,
        p_source_oid
    )
    ON CONFLICT (ingress_txn_id, source_oid) DO NOTHING;

    UPDATE shiba_internal.ingress_transactions AS txn
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
CREATE FUNCTION shiba_internal.insert_ingress_events(
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
    v_first_inserted_input_seq bigint;
    v_last_inserted_input_seq bigint;
    v_batch_ordinal bigint;
    v_previous_last_input_seq bigint;
    v_source record;
    v_source_name text;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_events IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress event batch fields must not contain NULL';
    END IF;

    IF pg_catalog.jsonb_typeof(p_events) <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'ingress event batch must be a JSONB array';
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
                    'ingress event batch element %s has invalid shape',
                    v_array_ordinal
                );
        END IF;

        SELECT event_result.input_seq,
               event_result.inserted
          INTO STRICT v_input_seq,
                      v_inserted
          FROM shiba_internal.insert_ingress_event(
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
            IF v_first_inserted_input_seq IS NULL THEN
                v_first_inserted_input_seq := v_input_seq;
            END IF;
            v_last_inserted_input_seq := v_input_seq;
        ELSE
            replayed_count := replayed_count + 1;
        END IF;
    END LOOP;

    -- Normalize only the bounded input_seq interval touched by this call.
    -- typed_payload stays immutable so replay compares the original wire
    -- image; canonical_payload is what DAG operators read.
    IF first_input_seq IS NOT NULL THEN
        FOR v_source IN
            SELECT DISTINCT event.source_oid
              FROM shiba_internal.change_log AS event
             WHERE event.ingress_txn_id = p_ingress_txn_id
               AND event.input_seq BETWEEN first_input_seq AND last_input_seq
        LOOP
            SELECT pg_catalog.format(
                       '%I.%I',
                       namespace_catalog.nspname,
                       relation_catalog.relname
                   )
              INTO STRICT v_source_name
              FROM pg_catalog.pg_class AS relation_catalog
              JOIN pg_catalog.pg_namespace AS namespace_catalog
                ON namespace_catalog.oid = relation_catalog.relnamespace
             WHERE relation_catalog.oid = v_source.source_oid
               AND relation_catalog.relkind IN ('r', 'p');

            EXECUTE pg_catalog.format(
                'UPDATE shiba_internal.change_log AS event
                    SET canonical_payload = pg_catalog.to_jsonb(
                        pg_catalog.jsonb_populate_record(
                            NULL::%s,
                            event.typed_payload
                        )
                    )
                  WHERE event.ingress_txn_id = $1
                    AND event.input_seq BETWEEN $2 AND $3
                    AND event.source_oid = $4
                    AND event.canonical_payload IS NULL',
                v_source_name
            )
            USING p_ingress_txn_id,
                  first_input_seq,
                  last_input_seq,
                  v_source.source_oid;
        END LOOP;
    END IF;

    -- The ingress transaction header remains locked by insert_ingress_event
    -- until the caller commits, so batch ordinal allocation is serialized
    -- with input_seq allocation.  Replayed events create no second apply
    -- batch even when the replication transport groups frames differently.
    IF inserted_count > 0 THEN
        IF v_first_inserted_input_seq IS NULL
           OR v_last_inserted_input_seq IS NULL
           OR v_last_inserted_input_seq - v_first_inserted_input_seq + 1
                <> inserted_count THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'new ingress events for transaction %s are not contiguous',
                    p_ingress_txn_id
                );
        END IF;

        SELECT coalesce(max(batch.batch_ordinal), 0) + 1,
               max(batch.last_input_seq)
          INTO v_batch_ordinal,
               v_previous_last_input_seq
          FROM shiba_internal.ingress_apply_batches AS batch
         WHERE batch.ingress_txn_id = p_ingress_txn_id;

        IF v_first_inserted_input_seq
             <> coalesce(v_previous_last_input_seq + 1, 1) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress apply batches for transaction %s are not adjacent',
                    p_ingress_txn_id
                );
        END IF;

        INSERT INTO shiba_internal.ingress_apply_batches (
            ingress_txn_id,
            batch_ordinal,
            first_input_seq,
            last_input_seq,
            event_count
        )
        VALUES (
            p_ingress_txn_id,
            v_batch_ordinal,
            v_first_inserted_input_seq,
            v_last_inserted_input_seq,
            inserted_count
        );
    END IF;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.record_ingress_batch(
    p_slot_generation bigint,
    p_decode_end_lsn pg_lsn,
    p_message_digest bytea,
    p_event_count bigint,
    p_payload_bytes bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_inserted boolean;
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
            MESSAGE = 'ingress batch fields must not contain NULL';
    END IF;

    -- Lock-order level 1. This prevents generation retirement while the
    -- bounded batch is being checkpointed.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                p_slot_generation
            );
    END IF;

    -- Lock-order level 3: batch identity.  Event rows inserted earlier in this
    -- same caller transaction become durable atomically with this descriptor.
    INSERT INTO shiba_internal.ingress_decode_batches (
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

    v_inserted := FOUND;

    SELECT batch.message_digest,
           batch.event_count,
           batch.payload_bytes
      INTO STRICT v_existing_digest,
                  v_existing_event_count,
                  v_existing_payload_bytes
     FROM shiba_internal.ingress_decode_batches AS batch
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
                'ingress batch identity conflict for generation %s at %s',
                p_slot_generation,
                p_decode_end_lsn
            );
    END IF;

    -- Prefix batches are durable and idempotent but never advance the
    -- replication acknowledgement boundary. Only transaction finalization
    -- has enough database state to do that safely.
    RETURN v_inserted;
END;
$$;

CREATE FUNCTION shiba_internal.commit_ingress_transaction(
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
    v_final_lsn pg_lsn;
    v_existing_commit_lsn pg_lsn;
    v_existing_end_lsn pg_lsn;
    v_batch_count bigint;
    v_batch_event_count bigint;
    v_first_batch_ordinal bigint;
    v_last_batch_ordinal bigint;
    v_first_batch_input_seq bigint;
    v_last_batch_input_seq bigint;
    v_batches_are_adjacent boolean;
BEGIN
    IF p_ingress_txn_id IS NULL
       OR p_commit_lsn IS NULL
       OR p_end_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress commit fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order levels 1 then 2.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    SELECT txn.status,
           txn.final_lsn,
           txn.commit_lsn,
           txn.end_lsn,
           txn.event_count,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_final_lsn,
                  v_existing_commit_lsn,
                  v_existing_end_lsn,
                  event_count,
                  payload_bytes
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF p_commit_lsn <> v_final_lsn
       OR p_end_lsn < p_commit_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'invalid commit/end LSNs %s/%s for Begin.final_lsn %s',
                p_commit_lsn,
                p_end_lsn,
                v_final_lsn
            );
    END IF;

    IF v_status = 'committed' THEN
        IF v_existing_commit_lsn IS DISTINCT FROM p_commit_lsn
           OR v_existing_end_lsn IS DISTINCT FROM p_end_lsn THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress commit identity conflict for transaction %s',
                    p_ingress_txn_id
                );
        END IF;
        finalized := false;
    ELSE
        IF EXISTS (
            SELECT 1
              FROM shiba_internal.change_log AS event
             WHERE event.ingress_txn_id = p_ingress_txn_id
               AND event.canonical_payload IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress transaction %s has unnormalized payload',
                    p_ingress_txn_id
                );
        END IF;

        -- The manifest is the downstream execution contract. Validate it at
        -- the admission boundary so a damaged or incomplete range cannot be
        -- mistaken for the final batch and published early.
        WITH ordered_batches AS (
            SELECT batch.batch_ordinal,
                   batch.first_input_seq,
                   batch.last_input_seq,
                   batch.event_count,
                   lag(batch.last_input_seq) OVER (
                       ORDER BY batch.batch_ordinal
                   ) AS previous_last_input_seq
              FROM shiba_internal.ingress_apply_batches AS batch
             WHERE batch.ingress_txn_id = p_ingress_txn_id
        )
        SELECT count(*),
               coalesce(sum(batch.event_count), 0),
               min(batch.batch_ordinal),
               max(batch.batch_ordinal),
               min(batch.first_input_seq)
                   FILTER (WHERE batch.batch_ordinal = 1),
               max(batch.last_input_seq),
               coalesce(bool_and(
                   batch.first_input_seq
                     = coalesce(batch.previous_last_input_seq + 1, 1)
               ), true)
          INTO v_batch_count,
               v_batch_event_count,
               v_first_batch_ordinal,
               v_last_batch_ordinal,
               v_first_batch_input_seq,
               v_last_batch_input_seq,
               v_batches_are_adjacent
          FROM ordered_batches AS batch;

        IF (event_count = 0 AND v_batch_count <> 0)
           OR (
             event_count > 0
             AND (
               v_batch_count = 0
               OR v_first_batch_ordinal <> 1
               OR v_last_batch_ordinal <> v_batch_count
               OR v_first_batch_input_seq <> 1
               OR v_last_batch_input_seq <> event_count
               OR v_batch_event_count <> event_count
               OR NOT v_batches_are_adjacent
             )
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress transaction %s has an invalid apply-batch manifest',
                    p_ingress_txn_id
                );
        END IF;

        -- Header-only finalization: no change_log row is updated.
        UPDATE shiba_internal.ingress_transactions AS txn
           SET status = 'committed',
               commit_lsn = p_commit_lsn,
               end_lsn = p_end_lsn,
               finalized_at = clock_timestamp()
         WHERE txn.ingress_txn_id = p_ingress_txn_id;
        finalized := true;
    END IF;

    -- Lock-order level 5: one routing task, never subscriber fan-out here.
    INSERT INTO shiba_internal.routing_tasks (
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
             FROM shiba_internal.routing_tasks AS task
            WHERE task.ingress_txn_id = p_ingress_txn_id
              AND task.commit_lsn IS DISTINCT FROM p_commit_lsn
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'routing task identity conflict for transaction %s',
                p_ingress_txn_id
            );
    END IF;

    -- This is the only API that advances durable replication progress.
    -- Header finalization, routing-task creation, and this watermark update
    -- commit or roll back together.
    UPDATE shiba_internal.ingress_replay_state AS replay
       SET persisted_lsn = CASE
               WHEN replay.persisted_lsn IS NULL
                 OR replay.persisted_lsn < p_end_lsn
               THEN p_end_lsn
               ELSE replay.persisted_lsn
           END,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = v_slot_generation;

    RETURN NEXT;
END;
$$;

-- Fan one committed transaction out to a bounded page of DAG subscribers.
-- The shared payload remains in change_log and every inbox row points directly
-- to the durable ingress transaction.
CREATE FUNCTION shiba_internal.route_ingress_page(
    p_max_subscribers integer
)
RETURNS TABLE (
    worked boolean,
    completed boolean,
    subscribers_routed integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_task shiba_internal.routing_tasks%ROWTYPE;
    v_candidates oid[];
    v_candidate_count integer;
    v_page oid[];
BEGIN
    IF p_max_subscribers IS NULL OR p_max_subscribers < 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'routing page size must be at least one';
    END IF;

    SELECT task.*
      INTO v_task
      FROM shiba_internal.routing_tasks AS task
     WHERE task.status IN ('pending', 'routing')
     ORDER BY task.commit_lsn, task.ingress_txn_id
     LIMIT 1
     FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        RETURN QUERY SELECT false, false, 0;
        RETURN;
    END IF;

    PERFORM 1
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = v_task.ingress_txn_id
       AND txn.status = 'committed'
     FOR KEY SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'routing task %s does not reference a committed ingress transaction',
                v_task.ingress_txn_id
            );
    END IF;

    SELECT pg_catalog.array_agg(candidate.result_oid ORDER BY candidate.result_oid)
      INTO v_candidates
      FROM (
          SELECT matched.result_oid
            FROM (
                SELECT stream_view.result_oid
                  FROM shiba_internal.ingress_sources AS source
                  JOIN shiba_internal.stream_views AS stream_view
                    ON stream_view.source_oid = source.source_oid
                  JOIN shiba_internal.view_progress AS progress
                    ON progress.result_oid = stream_view.result_oid
                 WHERE source.ingress_txn_id = v_task.ingress_txn_id
                   AND stream_view.activation_lsn < v_task.commit_lsn
                   AND (
                       progress.applied_lsn IS NULL
                       OR progress.applied_lsn < v_task.commit_lsn
                   )
                UNION
                SELECT stream_view.result_oid
                  FROM shiba_internal.ingress_sources AS source
                  JOIN shiba_internal.inner_join_views AS join_view
                    ON join_view.right_source_oid = source.source_oid
                  JOIN shiba_internal.stream_views AS stream_view
                    ON stream_view.result_oid = join_view.result_oid
                  JOIN shiba_internal.view_progress AS progress
                    ON progress.result_oid = stream_view.result_oid
                 WHERE source.ingress_txn_id = v_task.ingress_txn_id
                   AND stream_view.activation_lsn < v_task.commit_lsn
                   AND (
                       progress.applied_lsn IS NULL
                       OR progress.applied_lsn < v_task.commit_lsn
                   )
            ) AS matched
           WHERE matched.result_oid > v_task.subscriber_cursor
           ORDER BY matched.result_oid
           LIMIT p_max_subscribers + 1
      ) AS candidate;

    v_candidate_count := coalesce(pg_catalog.cardinality(v_candidates), 0);
    v_page := v_candidates[1:least(v_candidate_count, p_max_subscribers)];

    IF coalesce(pg_catalog.cardinality(v_page), 0) > 0 THEN
        INSERT INTO shiba_internal.dag_inbox (
            result_oid,
            ingress_txn_id,
            commit_lsn
        )
        SELECT subscriber.result_oid,
               v_task.ingress_txn_id,
               v_task.commit_lsn
          FROM pg_catalog.unnest(v_page) AS subscriber(result_oid)
        ON CONFLICT DO NOTHING;
        subscribers_routed := pg_catalog.cardinality(v_page);
    ELSE
        subscribers_routed := 0;
    END IF;

    completed := v_candidate_count <= p_max_subscribers;
    UPDATE shiba_internal.routing_tasks AS task
       SET subscriber_cursor = CASE
               WHEN subscribers_routed > 0
               THEN v_page[subscribers_routed]
               ELSE task.subscriber_cursor
           END,
           status = CASE WHEN completed THEN 'complete' ELSE 'routing' END,
           attempts = task.attempts + 1,
           updated_at = clock_timestamp(),
           completed_at = CASE WHEN completed THEN clock_timestamp() ELSE NULL END,
           last_error = NULL
     WHERE task.ingress_txn_id = v_task.ingress_txn_id;

    worked := true;
    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.ingress_feedback_upper_bound(
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
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
$$;

-- Record feedback only after the replication feedback write succeeds.  This
-- is monotonic confirmation intent; it MUST NOT advance replay_safe_lsn.
CREATE FUNCTION shiba_internal.record_ingress_feedback(
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
            MESSAGE = 'ingress feedback fields must not contain NULL';
    END IF;

    -- Lock-order level 1.  The row lock and update are transaction-scoped.
    SELECT replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      INTO STRICT v_persisted_lsn,
                  v_confirmed_lsn,
                  v_replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
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
        UPDATE shiba_internal.ingress_replay_state AS replay
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

-- Advance the GC-safe watermark only from PostgreSQL's observed slot state.
-- A successful feedback write is intent; confirmed_flush_lsn is proof.
CREATE FUNCTION shiba_internal.reconcile_ingress_replay_safe(
    p_slot_generation bigint
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_name name;
    v_database_oid oid;
    v_baseline_lsn pg_lsn;
    v_persisted_lsn pg_lsn;
    v_confirmed_lsn pg_lsn;
    v_replay_safe_lsn pg_lsn;
    v_actual_lsn pg_lsn;
BEGIN
    SELECT replay.slot_name,
           replay.database_oid,
           replay.slot_baseline_lsn,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      INTO STRICT v_slot_name,
                  v_database_oid,
                  v_baseline_lsn,
                  v_persisted_lsn,
                  v_confirmed_lsn,
                  v_replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    SELECT slot.confirmed_flush_lsn
      INTO v_actual_lsn
      FROM pg_catalog.pg_replication_slots AS slot
     WHERE slot.slot_name = v_slot_name::text
       AND slot.database = pg_catalog.current_database()
       AND slot.plugin = 'pgoutput';

    IF NOT FOUND
       OR v_database_oid IS DISTINCT FROM (
           SELECT database.oid
             FROM pg_catalog.pg_database AS database
            WHERE database.datname = pg_catalog.current_database()
       )
       OR v_actual_lsn IS NULL
       OR v_actual_lsn < v_baseline_lsn
       OR (v_persisted_lsn IS NOT NULL
           AND v_actual_lsn > v_persisted_lsn)
       OR (v_persisted_lsn IS NULL
           AND v_actual_lsn <> v_baseline_lsn) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical slot %s cannot prove replay safety for generation %s',
                v_slot_name,
                p_slot_generation
            );
    END IF;

    IF v_persisted_lsn IS NULL THEN
        RETURN NULL;
    END IF;

    v_confirmed_lsn := greatest(
        coalesce(v_confirmed_lsn, v_actual_lsn),
        v_actual_lsn
    );
    v_replay_safe_lsn := greatest(
        coalesce(v_replay_safe_lsn, v_actual_lsn),
        v_actual_lsn
    );

    UPDATE shiba_internal.ingress_replay_state AS replay
       SET confirmed_lsn = v_confirmed_lsn,
           replay_safe_lsn = v_replay_safe_lsn,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = p_slot_generation;

    RETURN v_replay_safe_lsn;
END;
$$;

REVOKE ALL ON FUNCTION
    shiba_internal.ensure_ingress_generation(name, boolean)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.retire_ingress_generation(name)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.claim_ingress_transaction(bigint, bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.insert_ingress_event(
        bigint, pg_lsn, bigint, integer, oid, bigint, jsonb
    )
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.insert_ingress_events(bigint, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.record_ingress_batch(
        bigint, pg_lsn, bytea, bigint, bigint
    )
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.commit_ingress_transaction(bigint, pg_lsn, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.route_ingress_page(integer)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.ingress_feedback_upper_bound(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.record_ingress_feedback(bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.reconcile_ingress_replay_safe(bigint)
    FROM PUBLIC;
